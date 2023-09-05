// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// We recover the contents of the descriptor from the other files we find.
// (1) Any log files are first converted to tables
// (2) We scan every table to compute
//     (a) smallest/largest for the table
//     (b) largest sequence number in the table
// (3) We generate descriptor contents:
//      - log number is set to zero
//      - next-file-number is set to 1 + largest file number we found
//      - last-sequence-number is set to largest sequence# found across
//        all tables (see 2c)
//      - compaction pointers are cleared
//      - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.
// 从找到的其他文件中恢复描述符的内容。
// (1)所有日志文件首先被转换为表
// (2)我们扫描每个表来计算
//    (a)表的最小/最大
//    (b)表中最大序号
// (3)生成描述符内容:
//    -日志数设置为零
//    - next-file-number设置为1 +我们找到的最大文件号
//    - last-sequence-number设置为找到的最大序列#
// 所有表(见2c)
//    -清除压缩指针
//    -每个表文件在0级添加
//
// 可能的优化1:
//    (a)计算总大小并选择合适的最大级别M
//    (b)按表中最大的序列#进行排序
//    (c)对于每个表:如果它与前一个表重叠，放在0级;
//    else存放在m级。
//    可能的优化2:
//    存储每个表的元数据(最小，最大，最大-seq#，…)
//    在表的元段中加速ScanTable。

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"

namespace leveldb {

// 匿名空间
namespace {

class Repairer {
 public:
  Repairer(const std::string& dbname, const Options& options)
      : dbname_(dbname),
        env_(options.env),
        icmp_(options.comparator),
        ipolicy_(options.filter_policy),
        options_(SanitizeOptions(dbname, &icmp_, &ipolicy_, options)),
        owns_info_log_(options_.info_log != options.info_log),
        owns_cache_(options_.block_cache != options.block_cache),
        next_file_number_(1) {
    // TableCache can be small since we expect each table to be opened once.
    table_cache_ = new TableCache(dbname_, options_, 10);
  }

  ~Repairer() {
    delete table_cache_;
    if (owns_info_log_) {
      delete options_.info_log;
    }
    if (owns_cache_) {
      delete options_.block_cache;
    }
  }

  // Run()运行db恢复的接口
  Status Run() {
    Status status = FindFiles();
    if (status.ok()) {
      ConvertLogFilesToTables();
      ExtractMetaData();
      status = WriteDescriptor();
    }
    if (status.ok()) {
      unsigned long long bytes = 0;
      for (size_t i = 0; i < tables_.size(); i++) {
        bytes += tables_[i].meta.file_size;
      }
      Log(options_.info_log,
          "**** Repaired leveldb %s; "
          "recovered %d files; %llu bytes. "
          "Some data may have been lost. "
          "****",
          dbname_.c_str(), static_cast<int>(tables_.size()), bytes);
    }
    return status;
  }

 private:
  struct TableInfo { // SSTable的信息
    FileMetaData meta; // SSTable的元信息
    SequenceNumber max_sequence; // 最大的序列号
  };

  // FindFiles()遍历数据目录下的文件解析文件名至manifest，日志，SSTable。
  Status FindFiles() {
    std::vector<std::string> filenames;
    Status status = env_->GetChildren(dbname_, &filenames);
    if (!status.ok()) {
      return status;
    }
    if (filenames.empty()) {
      return Status::IOError(dbname_, "repair found no files");
    }

    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        if (type == kDescriptorFile) {
          manifests_.push_back(filenames[i]);
        } else {
          if (number + 1 > next_file_number_) {
            next_file_number_ = number + 1;
          }
          if (type == kLogFile) {
            logs_.push_back(number);
          } else if (type == kTableFile) {
            table_numbers_.push_back(number);
          } else {
            // Ignore other files
          }
        }
      }
    }
    return status;
  }

  /*
  ConvertLogFilesToTables()解析日志文件生成为SSTble，主要用到了ConvertLogToTable。
  ConvertLogFilesToTables主要用了一个和RecoverLogFile函数类似的ConvertLogToTable函数，
  他们的主要区别在RecoverLogFile在转化日志操作的时候使用的是leveldb的全局环境memtable和immtable，
  所以recover的数据有可能会持久化为SSTable，而有一部分则会留在内存中的memtable中；
  而ConvertLogToTable则是自己新建了一个局部的memtable来进行操作，然后将数据持久化为SSTable，
  这个过程从log恢复的数据一定会持久化为SSTable。
  */
  void ConvertLogFilesToTables() {
    for (size_t i = 0; i < logs_.size(); i++) {
      std::string logname = LogFileName(dbname_, logs_[i]);
      Status status = ConvertLogToTable(logs_[i]);
      if (!status.ok()) {
        Log(options_.info_log, "Log #%llu: ignoring conversion error: %s",
            (unsigned long long)logs_[i], status.ToString().c_str());
      }
      ArchiveFile(logname);
    }
  }

  // ConvertLogToTable()是具体由某个Log来来生成SSTable文件的。
  Status ConvertLogToTable(uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env* env;
      Logger* info_log;
      uint64_t lognum;
      void Corruption(size_t bytes, const Status& s) override {
        // We print error messages for corruption, but continue repairing.
        Log(info_log, "Log #%llu: dropping %d bytes; %s",
            (unsigned long long)lognum, static_cast<int>(bytes),
            s.ToString().c_str());
      }
    };

    // Open the log file
    std::string logname = LogFileName(dbname_, log);
    SequentialFile* lfile;
    Status status = env_->NewSequentialFile(logname, &lfile);
    if (!status.ok()) {
      return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.lognum = log;
    // We intentionally make log::Reader do checksumming so that
    // corruptions cause entire commits to be skipped instead of
    // propagating bad information (like overly large sequence
    // numbers).
    log::Reader reader(lfile, &reporter, false /*do not checksum*/,
                       0 /*initial_offset*/);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    MemTable* mem = new MemTable(icmp_);
    mem->Ref();
    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < 12) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      status = WriteBatchInternal::InsertInto(&batch, mem);
      if (status.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        Log(options_.info_log, "Log #%llu: ignoring %s",
            (unsigned long long)log, status.ToString().c_str());
        status = Status::OK();  // Keep going with rest of file
      }
    }
    delete lfile;

    // Do not record a version edit for this conversion to a Table
    // since ExtractMetaData() will also generate edits.
    FileMetaData meta;
    meta.number = next_file_number_++;
    Iterator* iter = mem->NewIterator();
    status = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    delete iter;
    mem->Unref();
    mem = nullptr;
    if (status.ok()) {
      if (meta.file_size > 0) {
        table_numbers_.push_back(meta.number);
      }
    }
    Log(options_.info_log, "Log #%llu: %d ops saved to Table #%llu %s",
        (unsigned long long)log, counter, (unsigned long long)meta.number,
        status.ToString().c_str());
    return status;
  }

  /* 
  ExtractMetaData()逐个遍历解析扫描到的SSTable并重新manifest的filemeta信息以供后面使用，
  如果解析过程中有不能解析的数据，丢弃不能解析的数据并生成新的SSTable。
  */
  void ExtractMetaData() {
    for (size_t i = 0; i < table_numbers_.size(); i++) {
      ScanTable(table_numbers_[i]);
    }
  }

  // NewTableIterator()创建SSTable的两层迭代器
  Iterator* NewTableIterator(const FileMetaData& meta) {
    // Same as compaction iterators: if paranoid_checks are on, turn
    // on checksum verification.
    ReadOptions r;
    r.verify_checksums = options_.paranoid_checks;
    return table_cache_->NewIterator(r, meta.number, meta.file_size);
  }

  // ScanTable()对指定的SSTable进行扫描。
  void ScanTable(uint64_t number) {
    TableInfo t;
    t.meta.number = number;
    std::string fname = TableFileName(dbname_, number);
    Status status = env_->GetFileSize(fname, &t.meta.file_size);
    if (!status.ok()) {
      // Try alternate file name.
      fname = SSTTableFileName(dbname_, number);
      Status s2 = env_->GetFileSize(fname, &t.meta.file_size);
      if (s2.ok()) {
        status = Status::OK();
      }
    }
    if (!status.ok()) {
      ArchiveFile(TableFileName(dbname_, number));
      ArchiveFile(SSTTableFileName(dbname_, number));
      Log(options_.info_log, "Table #%llu: dropped: %s",
          (unsigned long long)t.meta.number, status.ToString().c_str());
      return;
    }

    // Extract metadata by scanning through table.
    int counter = 0;
    Iterator* iter = NewTableIterator(t.meta);
    bool empty = true;
    ParsedInternalKey parsed;
    t.max_sequence = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      if (!ParseInternalKey(key, &parsed)) {
        Log(options_.info_log, "Table #%llu: unparsable key %s",
            (unsigned long long)t.meta.number, EscapeString(key).c_str());
        continue;
      }

      counter++;
      if (empty) {
        empty = false;
        t.meta.smallest.DecodeFrom(key);
      }
      t.meta.largest.DecodeFrom(key);
      if (parsed.sequence > t.max_sequence) {
        t.max_sequence = parsed.sequence;
      }
    }
    if (!iter->status().ok()) {
      status = iter->status();
    }
    delete iter;
    Log(options_.info_log, "Table #%llu: %d entries %s",
        (unsigned long long)t.meta.number, counter, status.ToString().c_str());

    if (status.ok()) {
      tables_.push_back(t);
    } else {
      RepairTable(fname, t);  // RepairTable archives input file.
    }
  }

  // RepairTable()对有损坏的数据进行重建SSTable。
  void RepairTable(const std::string& src, TableInfo t) {
    // We will copy src contents to a new table and then rename the
    // new table over the source.

    // Create builder.
    std::string copy = TableFileName(dbname_, next_file_number_++);
    WritableFile* file;
    Status s = env_->NewWritableFile(copy, &file);
    if (!s.ok()) {
      return;
    }
    TableBuilder* builder = new TableBuilder(options_, file);

    // Copy data.
    Iterator* iter = NewTableIterator(t.meta);
    int counter = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      builder->Add(iter->key(), iter->value());
      counter++;
    }
    delete iter;

    ArchiveFile(src);
    if (counter == 0) {
      builder->Abandon();  // Nothing to save
    } else {
      s = builder->Finish();
      if (s.ok()) {
        t.meta.file_size = builder->FileSize();
      }
    }
    delete builder;
    builder = nullptr;

    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (counter > 0 && s.ok()) {
      std::string orig = TableFileName(dbname_, t.meta.number);
      s = env_->RenameFile(copy, orig);
      if (s.ok()) {
        Log(options_.info_log, "Table #%llu: %d entries repaired",
            (unsigned long long)t.meta.number, counter);
        tables_.push_back(t);
      }
    }
    if (!s.ok()) {
      env_->RemoveFile(copy);
    }
  }

  // WriteDescriptor()重置manifest文件号为1并生成最新的manifest，将其记录到current文件
  Status WriteDescriptor() {
    std::string tmp = TempFileName(dbname_, 1);
    WritableFile* file;
    Status status = env_->NewWritableFile(tmp, &file);
    if (!status.ok()) {
      return status;
    }

    SequenceNumber max_sequence = 0;
    for (size_t i = 0; i < tables_.size(); i++) {
      if (max_sequence < tables_[i].max_sequence) {
        max_sequence = tables_[i].max_sequence;
      }
    }

    edit_.SetComparatorName(icmp_.user_comparator()->Name());
    edit_.SetLogNumber(0);
    edit_.SetNextFile(next_file_number_);
    edit_.SetLastSequence(max_sequence);

    for (size_t i = 0; i < tables_.size(); i++) {
      // TODO(opt): separate out into multiple levels
      const TableInfo& t = tables_[i];
      edit_.AddFile(0, t.meta.number, t.meta.file_size, t.meta.smallest,
                    t.meta.largest);
    }

    // std::fprintf(stderr,
    //              "NewDescriptor:\n%s\n", edit_.DebugString().c_str());
    {
      log::Writer log(file);
      std::string record;
      edit_.EncodeTo(&record);
      status = log.AddRecord(record);
    }
    if (status.ok()) {
      status = file->Close();
    }
    delete file;
    file = nullptr;

    if (!status.ok()) {
      env_->RemoveFile(tmp);
    } else {
      // Discard older manifests
      for (size_t i = 0; i < manifests_.size(); i++) {
        ArchiveFile(dbname_ + "/" + manifests_[i]);
      }

      // Install new manifest
      status = env_->RenameFile(tmp, DescriptorFileName(dbname_, 1));
      if (status.ok()) {
        status = SetCurrentFile(env_, dbname_, 1);
      } else {
        env_->RemoveFile(tmp);
      }
    }
    return status;
  }

  // ArchiveFile()该函数的目的是将给定的文件移动到另一个目录，
  // 并记录操作的状态信息。同时，在移动文件之前，会先创建目标目录。
  void ArchiveFile(const std::string& fname) {
    // Move into another directory.  E.g., for
    //    dir/foo
    // rename to
    //    dir/lost/foo
    const char* slash = strrchr(fname.c_str(), '/'); // 从文件名中找到最后一个"/"的位置，返回指向该位置的指针，没有找到则返回空指针
    std::string new_dir;
    if (slash != nullptr) { // 如果没有找到，则创建一个空字符串new_dir用于存放新目录的路径
      new_dir.assign(fname.data(), slash - fname.data()); 
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir);  // Ignore error 创建新的目录
    std::string new_file = new_dir; 
    new_file.append("/");
    new_file.append((slash == nullptr) ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file); // 将原始文件移动到新文件路径new_file中
    Log(options_.info_log, "Archiving %s: %s\n", fname.c_str(),
        s.ToString().c_str());
  }

  const std::string dbname_; // DB的名称
  Env* const env_; // 文件操作的统一接口
  InternalKeyComparator const icmp_; // internalkey的比较方法
  InternalFilterPolicy const ipolicy_; // 过滤器
  const Options options_; // 相关设置
  bool owns_info_log_; // 是否拥有日志文件
  bool owns_cache_; // 是否拥有Cache缓存
  TableCache* table_cache_; // TableCache缓存SSTable的基本信息，但不包括DataBlock，有专门的Block_Cache
  VersionEdit edit_; // 存放版本变更的信息

  std::vector<std::string> manifests_; // manifests文件记录的信息
  std::vector<uint64_t> table_numbers_; // 所有SSTable的集合
  std::vector<uint64_t> logs_; // 所有日志文件的集合
  std::vector<TableInfo> tables_; // 所有SSTable信息的集合
  uint64_t next_file_number_; // 下一个文件的编号
};
}  // namespace

// 唯一暴露给外部调用的接口
Status RepairDB(const std::string& dbname, const Options& options) {
  Repairer repairer(dbname, options);
  return repairer.Run();
}

}  // namespace leveldb
