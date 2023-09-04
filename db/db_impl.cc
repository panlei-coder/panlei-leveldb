// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

/*
DB API:主要用于封装一些供客户端应用进行调用的接口,即头文件中的相关API函数接口,
客户端应用可以通过这些接口实现数据引擎的各种操作
*/

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
/*
Writer用来封装一个WriteBatch，用来标识状态。
*/
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status; // 写入的状态
  WriteBatch* batch; // 对应的WriteBatch
  bool sync; // 写日志时，是否需要同步刷盘
  bool done; // 写入是否要完成
  port::CondVar cv; // 封装的条件变量，用来等待其他线程的通知
};

// 压缩的状态
struct DBImpl::CompactionState {
  // Files produced by compaction
  // 压缩之后产生的文件信息
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction; 

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  // Sequence numbers < smallest_snapshot是不重要的，因为我们将永远不必服务小于最小快照的快照。
  // 因此，如果我们看到序列号S <= smallest_snapshot，我们可以删除序列号<S的相同键的所有条目。
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile; // 压缩之后生成的文件
  TableBuilder* builder; // 用于生成SSTable文件的构造器

  uint64_t total_bytes; // 总的字节数
};

// Fix user-supplied options to be reasonable
// 修复用户提供的选项，使其合理
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) { // 限制在最大、最小值范围内
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

// SanitizeOptions()函数用于初始化创建一个Options
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp; // 比较器
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr; // 过滤器
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000); // 最大打开文件数
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30); // 写缓冲区大小
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30); // 最大文件大小
  ClipToRange(&result.block_size, 1 << 10, 4 << 20); // Block的大小
  if (result.info_log == nullptr) { // 在与数据库相同的目录下打开一个日志文件
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist 如果不存在则创建目录
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname)); // 将旧的日志文件重命名
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log); // 创建一个新的日志文件
    if (!s.ok()) { // 创建失败
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    // 如果block_cache为空，则创建一个大小为8MB的LRU缓存
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

/* 
TableCacheSize()函数用于获取TableCache的大小
*/
static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  // 为其他用途保留10个左右的文件，并将其余的文件交给TableCache。
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

// 构造
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {}

// 析构
DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

// 创建DB
Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

/*
RemoveObsoleteFiles()函数用于删除过时的文件
*/
void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    // 在后台发生错误后，我们无法确定是否已经提交了新版本，
    // 所以不能安全地进行垃圾回收操作，直接返回
    return;
  }

  // Make a set of all of the live files
  // 获取所有存活文件的集合live
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  // 忽略错误，意在获取数据库目录下的所有文件名
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  // 依次遍历所有文件，判断文件是否需要被添加到删除集合files_to_delete中
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          // 对于日志文件，保留当前日志编号及上一个日志编号的文件
          keep = ((number >= versions_->LogNumber()) || // mem_的日志文件
                  (number == versions_->PrevLogNumber())); // 记录imm_的日志文件
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          // 对于描述符文件（manifest文件），保留当前描述符文件及更新版本的文件
          // （以防存在允许其他版本的竞争）
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          // 对于表文件，只保留在live集合中的文件
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          // 对于临时文件，当前正在写入的临时文件必须记录在pending_outputs_中，
          // 该集合已经合并到"live"中
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) { // 将不需要保留的文件添加到集合中
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  // 在删除所有文件时解锁其他线程。
  // 所有待删除的文件具有唯一的名称，不会与新创建的文件发生冲突，
  // 因此可以在允许其他线程继续执行的同时删除这些文件。
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock(); // 便于后续继续操作
}

/*
Recover()主要是进行日志故障恢复，将mem_和imm_的数据恢复到故障之前的状态。
（1）创建数据库目录；
（2）对这个数据库里面的LOCK文件加文件锁，LevelDB是单进程多线程的，
    需要保证每次只有一个进程能够打开数据库，方式就是使用了文件锁，
    如果有其它进程打开了数据库，那么加锁就会失败；
（3）如果数据库不存在，那么调用DBImpl::NewDB创建新的数据库；
（4）调用VersionSet::Recover来读取MANIFEST，恢复版本信息；
（5）根据版本信息，搜索数据库目录，找到关闭时没有写入到SSTable的日志，
    按日志写入顺序逐个恢复日志数据。DBImpl::RecoverLogFile会创建一个MemTable，
    开始读取日志信息，将日志的数据插入到MemTable，并根据需要调用DBImpl::WriteLevel0Table
    将MemTable写入到SSTable中。
*/
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_); // 创建数据库目录
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_); // 加文件锁，防止其他线程进入
  if (!s.ok()) {
    return s;
  }

  // 如果CURRENT文件不存在，说明需要新创建数据库
  if (!env_->FileExists(CurrentFileName(dbname_))) {    
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  // 读取MANIFEST文件进行版本信息的恢复
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  /*
   从所有比描述符中命名的日志文件更新的日志文件中恢复(新日志文件可能是由前一个版本添加的，
   但没有在描述符中注册)。请注意，PrevLogNumber()不再使用，
   但我们要注意它，以防我们正在恢复由旧版本的leveldb生成的数据库。
  */

  // 之前的MANIFEST恢复，会得到版本信息，里面包含了之前的log number
  // 搜索文件系统里的log，如果这些日志的编号 >= 这个log number，那么这些
  // 日志都是关闭时丢失的数据，需要恢复，这里将日志按顺序存储在logs里面

  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  // 获取所有的日志文件
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  // 按照日志生成的顺序进行恢复
  std::sort(logs.begin(), logs.end()); // 对日志文件进行排序，日志文件是有编号的
  // 逐个恢复日志的内容
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

/*
RecoverLogFile()根据WAL日志恢复写入到内存中的数据（mem_和imm_）。
该方法会创建一个MemTable，开始读取日志信息，将日志的数据插入到MemTable，
并根据需要调用DBImpl::WriteLevel0Table将MemTable写入到SSTable中。
相当于是重新执行一边memtable->immtable->sstable的过程。
注意：一个日志文件可能会对应多个SSTable文件。
*/
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  // 遍历读取日志记录
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    // 如果mem_大小达到阈值时，转换为SSTable，写入到level 0中
    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) { 
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  // 看看我们是否应该继续重用最后一个日志文件。
  // 检查状态是否成功、选项中是否启用日志重用、是否存在上一个日志文件并且没有正在进行的压缩操作
  // 即所有要恢复的日志中，最后一个日志可能还没有写满（达到阈值），可以继续进行追加写。
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    // 获取旧日志文件的大小
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      // 如果能够成功获取旧日志文件的大小并创建一个可追加的新文件
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      // 创建一个新的日志写入器，并传入旧日志文件和其大小
      log_ = new log::Writer(logfile_, lfile_size);
      // 将日志文件编号设置为log_number
      logfile_number_ = log_number;
      if (mem != nullptr) {
        // 如果mem不为空，将其赋值给mem_，并将mem置为空
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        // 如果mem为空，创建一个新的内存表（MemTable）并将其引用计数加一
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  // 生成该日志文件的最后一个SSTable
  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

// WriteLevel0Table()函数将Memtable落地为SSTable
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld(); // 判断是否上锁，没有则自动上锁
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  // 获取新SSTable的文件名即FileNum
  meta.number = versions_->NewFileNumber();
  // 保存此File，防止被删除
  pending_outputs_.insert(meta.number);
  // 对此Memable创建访问的迭代器
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    // 按SSTable格式生存SSTable文件到磁盘，
    // 并将SSTable文件加入到table_cache_中
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  // 文件已生存，可删除此记录了
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      // 新生存的SSTable文件，不一定都放在level-0层，有可能是level-1或者level-2层，
      // 但最多是level-2层。此方法就是根据最小和最大key，找到需要放此SSTable的level。
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    // 通过VersionEdit记录新增的SSTable，用于后续产生新的Version。
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats; 
  // 记录状态信息
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

/*
CompactMemTable()函数将memtable转换为SSTable文件写入到磁盘level 0层中。
（1）将Immutable落地生成SSTable文件、同时将文件信息放入Table_Cache中；
（2）生成新的Version文件；
（3）删除无用文件。
*/
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  // 1、落地生成新的SSTable文件。
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    // 2、记录VersionEdit信息，
    // 通过LogAndApply()生存新的Version。
    // LogAndApply()还做了以下事情：
    // 1）记录了compaction_score_最高的那一层的level及score。
    // 2）检测更新manifest和Current文件。
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    // 3、因为进行了Compact，此处主要涉及到logFile、Manifest、Current，
    // 所以调用此方法，对已经无用的文件进行删除。
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

/*
CompactRange()函数进行手动压缩指定范围内的文件（即将指定范围内的文件移动到下一层中    ）。
*/
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1; // 记录最大到哪个层与指定的范围有重叠
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) { // 从level 1寻找与指定范围有重叠的文件
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) { 
    TEST_CompactRange(level, begin, end);
  }
}

/*
TEST_CompactRange()函数将第level层的文件进行压缩。
这里使用一个循环不仅仅是为了等待Compaction完成，begin和end之间的文件可能非常多，
为了保证Compaction的效率，不会一次Compaction完所有的文件，会先选择一个middle，
然后Compaction begin和middle之间的文件，完成后将begin设置成middle，
将DBImpl::manual_compaction_设置为null，这样下次循环的时候会继续Compaction middle->end。
当最后一次Compaction完成后，done设置为true，循环就退出了。
*/
void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  // 构造ManualCompaction实例manual，保存Manual Compaction的信息
  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek); // 相同的key按照sequenceNum降序排列
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0)); // 相同的key按照sequenceNum降序排列
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      // manual_compaction_是DBImpl的一个字段，后台线程会去检查这个字段，如果不为空，会触发Compaction
      // 所以将manual_compaction_赋值manual，后台线程就会检查触发Compaction
      manual_compaction_ = &manual;
      // 可能调度一次Compaction
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      // manual_compaction_已经被设置，表示已经有Manual Compaction进行中了
      // 等待后台线程完成，完成后，会重新执行循环
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    // 取消我的手动压缩，因为我们因为某种原因提前终止了。
    manual_compaction_ = nullptr;
  }
}

/*
TEST_CompactMemTable()函数用于判断是否有imm_正在转换为SSTable文件写入磁盘，如果有就一直阻塞，直到它写入磁盘。
*/
Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) { // imm_不为空，正有imm_转换为SSTable文件，一直阻塞，直到imm_写完
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

// RecordBackgroundError()用于记录后台运行产生的错误
void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

// MaybeScheduleCompaction()函数根据目前的DB状态有可能产生压缩，如果产生压缩则调用后台处理函数（线程池中运行）
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) { // 压缩已经完成了
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) { // DB已经被删除，不能进行后台压缩了
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) { // 出现错误
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) { // 不需要做任何操作
    // No work to be done
  } else { // 进行后台压缩
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

// BGWork()函数调用BackgroundCall()进行后台压缩处理
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

// BackgroundCall()函数进行后台压缩处理
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction(); // 压缩处理
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  // 以前的压缩可能在一个级别中产生了太多的文件，因此如果需要，请重新安排另一次压缩。
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

/*
BackgroundCompaction()函数将后台进行压缩处理，根据手动compact还是自动compact来产生Compaction。
*/
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  // 1.如果imm_不空，则先压缩imm_
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  // 2.如果是手动压缩
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else { // 不是手动压缩则按照规则选择要压缩的文件
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) { // 表示没有需要Compaction的内容
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // 处理一种特殊情况，也就是参与Compaction的文件，level_有一个文件，而level_ + 1 没有
    // 这时候只需要直接更改元数据，然后文件移动到level_ + 1即可，不需要多路归并

    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    // 多路归并操作实现compaction
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact); // 删除compact过程中产生的中间临时文件
    c->ReleaseInputs(); 
    RemoveObsoleteFiles(); // 由于产生了新的version导致某些文件过期需要删除
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) { // 如果是手动压缩名，需要重置manual_compaction_，并设置相关信息
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // 我们只压缩了要求范围的一部分。将*m更新为剩下的待压缩的范围。
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

// CleanupCompaction()函数用于清理Compaction过程中产生的一些文件。
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    // 如果我们在压缩过程中接到停工电话会发生什么
    compact->builder->Abandon(); // 停止压缩
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile; // 删除压缩之后的文件
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

// OpenCompactionOutputFile()创建用于存放compact生成的key-value的新SSTable文件
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

// FinishCompactionOutputFile()函数将compact操作生成的SSTable大小达到指定阈值，
// 或者已经compact完成之后生成正式的SSTable文件。
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

/*
InstallCompactionResults()用于安装Compaction的结果，生成新的version。
*/
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

/*
DoCompactionWork()实际做压缩工作的函数。将需要compact的SSTable文件通过迭代器访问，
对满足条件的key-value数据就写入到新的SSTable文件中，不满足的就直接丢弃掉。

函数构造了一个迭代器，开始多路归并的操作，会考虑以下几点：
（1）迭代按照Internal Key的顺序进行，多个连续的Internal Key里面可能包含相同的User Key，按照SequenceNumber降序排列；
（2）相同的User Key里只有第一个User Key是有效的，因为它的SequenceNumber是最大的，覆盖了旧的User Key，
    但是无法只保留第一个User Key，因为LevelDB支持多版本，旧的User Key可能依然有线程可以引用，但是不再引用的User Key可以安全的删除；
（3）碰到一个删除时，并且它的SequenceNumber <= 最新的Snapshot，会判断更高Level是否有这个User Key存在。
    如果存在，那么无法丢弃这个删除操作，因为一旦丢弃了，更高Level原被删除的User Key又可见了。如果不存在，那么可以安全的丢弃这个删除操作，这个键就找不到了；
（4）对于生成的SSTable文件，设置两个上限，哪个先达到，都会开始新的SSTable。一个就是2MB，另外一个就是判断上一Level和这个文件的重叠的文件数量，
    不超过10个，这是为了控制这个生成的文件Compaction的时候，不会和太多的上层文件重叠。
*/
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  // 取当前最小的使用中的SequenceNumber
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    // 如果某个快照被外部使用(GetSnapShot),这个快照对应的SnapShotImpl对象会被放
	  // 在SnapshotList中(也就是sequence_number被保存下来了)，Compaction的时候
    // 遇到可以清理的数据，还需要判断要清理数据的seq_number不能大于这些快照中的
	  // sequence_number，否则会影响夸张数据。
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // 参与Compaction的SSTable组成一个迭代器
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  // 这里input迭代器的循环遍历是每次取迭代器中最小的key，
  // key是指InternalKey，key比较器是InternalKeyComparator，
  // InternalKey比较方式是对InternalKey中的User_Key按BytewiseComparator来比较。
  // 在User_Key相同的情况下，按SequenceNumber来比较，SequenceNumber值大的是小于SequenceNumber值小的，
  // 所以针对abc_456_1、abc_123_2，abc_456_1是小于abc_123_2的。针对我们对同一个user_key的操作，
  // 最新对此key的操作是小于之前对此key的操作的。
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key; // 记录当前的User Key
  bool has_current_user_key = false; // 记录是否碰到过一个同样的User Key
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  // 使用MergingIterator来进行多路归并排序
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) { // 如果有imm_优先将其转换为SSTable
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key(); // 从所有需要合并的SSTable中获取最小的一个key
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      // ShouldStopBefore判断生成的SSTable和level_ + 2层的有重叠的文件个数，如果超过10个，
      // 那么这个SSTable生成就完成了,这样保证了新生产的SSTable和上一层不会有过多的重叠;
      // 创建一个新的SSTable，写入文件，将之前已经添加到输出文件的key-value生成SSTable
      status = FinishCompactionOutputFile(compact, input); // 终止了这次SSTable的合并
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) { // 解析internalkey，如果解析失败，则说明internalkey有问题
      // Do not hide error keys
      //解析key失败。
      //针对解析失败的key，这里不丢弃，直接存储。
      //目的就是不隐藏这种错误，存储到SSTable中，
      //便于后续逻辑去处理。
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      // 解析成功，则将当前的user_key保存下来，用于后续的比较
      if (!has_current_user_key || 
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        // 前后检测的两个InternalKey不一样（如果userkey相同，那么肯定会连续被遍历到），
		    // 那就记录这个首次出现的key，并将last_sequence_for_key设置为最大。
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // 如果上一个Key的SequenceNumber <= 最小的存活的Snapshot，那么
      // 这个Key的SequenceNumber一定 < 最小的存活的Snapshot，那么这个Key就不
      // 会被任何线程看到了，可以被丢弃，上面碰到了第一个User Key时，设置了
      // last_sequence_for_key = kMaxSequenceNumber;保证第一个Key一定不会被丢弃
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        // 进入此逻辑，说明last_sequence_for_key不是最大值kMaxSequenceNumber，
		    // 也就是当前这个key的user_key(是一个比较旧的userkey)和上一个key的user_key是相同的。
		    // 所以这里就直接丢弃。
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        // 如果碰到了一个删除操作，并且SequenceNumber <= 最小的Snapshot，
        // 通过IsBaseLevelForKey判断更高Level不会有这个User Key存在，那么这个Key就被丢弃

        // 如果这个InternalKey满足一下三个条件，则可以直接丢弃。
        // 1.是个Deletionkey。
        // 2.sequence <= small_snaphshot。
        // 3.当前compact的level是level-n和level-n+1，
        //  如果在level-n+1以上的层已经没有此InternalKey对应的user_key了。
        // 基于以上三种情况可删除。
        // 为什么要此条件(IsBaseLevelForKey)判断呢？
        // 举个例子：
        // 如果在更高层，还有此InternalKey对应的User_key，
        // 此时你把当前这个InternalKey删除了，那就会出现两个问题：
        // 问题1：再次读取删除的key时，就会读取到老的过期的key(这个key的type是非deletion)，这是有问题的。
        // 问题2：再次合并时，但这个key(这个key的type是非deletion)首次被读取时last_sequence_for_key会设置为kMaxSequenceNumber，
        //      这样就也不会丢弃。
        // 以上两个问题好像在更高层的也就是旧的此key的所有userkey的type都是是delete的时候好像是没问题的，
        // 但这毕竟是少数，原则上为了系统正常运行，我们每次丢弃一个标记为kTypeDeletion的key时，
        // 必须保证数据库中不存在它的过期key，否则就得将它保留，直到后面它和这个过期的key合并为止，合并之后再丢弃
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
    // 没有被丢弃就添加Key
    if (!drop) {
      // Open output file if necessary
      // 打开输出文件
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }

      // 如果输出文件的SSTable构造器的entry是空的，即刚开始添加第一个key的时候，将被添加的key作为将要构造的SSTable的smallest
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }

      // 没有被丢弃就添加Key
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      // 待生成的SSTable已超过特定阈值，那么就将此SSTable文件落地。
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input); // 生成SSTable文件
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next(); // 找到下一个最小的key
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  // 生成最后一个SSTable文件
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

/*
Get()函数是先读取MemTable，存在就读到数据了；
不存在的话，看看Immutable MemTable是否存在，存在的话，读取数据；
否则，从SSTable的Level 0开始读取，如果Level 0还没有找到话，读Level 1，以此类推，直到读到最高层。
*/
Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  // 加锁
  MutexLock l(&mutex_);

  // 设定snapshot，其实就是一个SequenceNumber，这是实现snapshot的关键，设置成选项中的snapshot
  // 当前最近的SequenceNumber
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  // mem是MemTable，imm是Immutable MemTable，
  // SSTable则由当前的version代表，一个version包含了当前版本的SSTable的集合
  // 三者都增加计数
  MemTable* mem = mem_;
  MemTable* imm = imm_;
  // 获取当前指向的version，因为它和snapshot没有关系，snapshot主要是通过key-value的sequenceNum来区别的
  Version* current = versions_->current(); 

  // 这里都调用了Ref，LevelDB里大量使用了这种引用计数的方式管理对象，这里表示读取需要引用这些对象，假设在读取过程
  // 中其他线程发生了MemTable写满了，或者Immutable MemTable写入完成了需要删除了，或者做了一次Compaction，生
  // 成了新的Version，所引用的SSTable不再有效了，这些都需要对这些对象做一些改变，比如删除等，但是当前线程还引用着
  // 这些对象，所以这些对象还不能被删除。采用引用计数，其它线程删除对象时只是简单的Unref，因为当前线程还引用着这些
  // 对象，所以计数>=1，这些对象不会被删除，而当读取结束，调用Unref时，如果对象的计数是0，那么对象会被删除。
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  { 
    // 读取时解锁
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    // 构造一个Lookup Key搜索MemTable
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) { // 搜索MemTable
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) { // 搜索Immutable
      // Done
    } else { // 搜索SSTable
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  // 根据访问SSTable的seek次数来判断是否需要进行compact
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }

  // Unref释放
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

// NewIterator()创建DB的迭代器。
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
// Put()用于添加key-value到DB中。
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

// Delete用于将指定的key从DB中删除。
Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

/*
Write()函数为了实现线程安全，每个线程在写入时，都需要获取锁，但是这样会阻塞其它的线程，降低并发度。
针对这个问题LevelDB做了一个优化，写入时当获取锁后，会将WriteBatch放入到一个std::deque<Writer*> DBImpl::writers_里，
然后会检查writers_里的第一个元素是不是自己，如果不是的话，就会释放锁。当一个线程检查到writers_头元素是自己时，会再次获取锁，
然后将writers_里的数据尽可能多的写入。一次写入会涉及到写日志，占时间比较长，一个线程的数据可能被其它线程批量写入进去了，减少了等待。
总结来说，一个线程的写入有两种情况：一种是恰好自己是头结点，自己写入，另外一种是别的线程帮助自己写入了，自己会检查到写入，然后就可以返回了。
*/
Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  // 1.构造一个writer，插入到wrtiers_里，注意插入前需要先获取锁
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w); // 添加到写队列中

  // 2.检查done判断是否完成，或者自己是否是writers_队列里的第一个成员。有可能其它线程写入时把自己的内容也写入了，
  // 这样自己就是done，或者当自己是头元素了，表示轮到自己写入了
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }

  // 3.判断写入是否完成了，完成了就可以返回了
  if (w.done) {
    return w.status;
  }
  
  // 4.如果写入太快，进行限流，如果MemTable满了，生成新的MemTable
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);

  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    // 5.从writers_头部扫描足够多的WriteBatch构造一个WriteBatch updates，last_writer保存了最后一个写入的writer
    // 设定新的WriteBatch的SequenceNumber
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch); // 因为一个writebatch包含count个record

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    // 添加到日志并应用到memtable。我们可以在此阶段释放锁，
    // 因为&w目前负责日志记录，并防止并发日志记录器和并发写入mem。
    {
      // 注意这一步解锁，很关键，因为接下来的写入可能是一个费时的过程，解锁后，其它线程可以Get，其它线程也可以继续将writer
      // 插入到writers_里面，但是插入后，因为不是头元素，会等待，所以不会冲突;
      // 但记住之前已经被阻塞的线程是不会被唤醒的，依然处于阻塞状态。
      mutex_.Unlock();

      // 6.写入log，根据选项sync决定是否同步
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync(); // wal日志，因此在写完之后，会立马有个Sync同步操作
        if (!status.ok()) {
          sync_error = true;
        }
      }

      // 7.写入memtable
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_); // 优先写入到mem_
      }

      // 加锁，后续需要修改全局的SequenceNumber以及writers_
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();
    // 修改全局的SequenceNumber
    versions_->SetLastSequence(last_sequence);
  }

  // 8.从writers_队列从头开始，将写入完成的writer标识成done，并且弹出，通知这些writer
  // 这样这些writer的线程会被唤醒，发现自己的写入已经完成了，就会返回
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  // 如果writers_里还有元素，就通知头元素，让它可以进来开始写入
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
/*
BuildBatchGroup()函数是批量写入的关键，它会从writers_头部开始扫描，
将尽可能多的writer生成一个新的WriterBatch，将这些writer的内容批量写入。
(1)首先计算出一个max_size，表示构造的WriterBatch的最大尺寸；
(2)然后开始扫描writers_数组，直到满足WriterBatch超过max_size，或者；
(3)第一个writer的sync决定了这整个write是不是sync的，如果第一个writer不是sync的，
碰到一个sync的writer，表明这个writer无法加入到这个批量写入中，所以扫描就结束了。
*/
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front(); // 获取队头的writer
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  // 计算WriteBatch的最大size，如果第一个的size比较小的话，限制最大的size，以防止小的写入太慢
  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  // 允许组增长到最大大小，但如果原始写入很小，限制增长，这样我们就不会过多地减慢小写入。
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    // 如果这是一个sync的写入，但是第一个元素不是sync的话，那么就结束了，因为整体的写入都不是sync的
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      // 如果太大，就结束
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) { // 超过了最大大小
        // Do not make batch too big
        break;
      }

      // Append to *result
      // 追加到result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        // 切换到临时批处理，而不是干扰调用者批处理
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    // 更新最后一个writer
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
// 要求:这个线程当前在写队列的前面（队头）
/*
Write()函数中会使用到的MakeRoomForWrite()，该函数主要是限流和触发后台线程等工作。
首先判断Level 0的文件是否>=8，是的话就sleep 1ms，这里是限流的作用，
Level 0的文件太多，说明写入太快，Compaction跟不上写入的速度，
而在读取的时候Level 0的文件之间可能有重叠，所以太多的话，影响读取的效率，这算是比较轻微的限流，
最多sleep一次；接下来判断MemTable里是否有空间，有空间的话就可以返回了，写入就可以继续；
如果MemTable没有空间，判断Immutable MemTable是否存在，存在的话，
说明上一次写满的MemTable还没有完成写入到SSTable中，说明写入太快了，
需要等待Immutable MemTable写入完成；再判断Level 0的文件数是否>=12，如果太大，说明写入太快了，
需要等待Compaction的完成；到这一步说明可以写入，但是MemTable已经写满了，需要将MemTable变成Immutable MemTable，
生成一个新的MemTable，触发后台线程写入到SSTable中。
*/
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force; // 是否允许延迟，如果updates不为空，则不允许延迟
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) { // 减缓写入的速度
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      // 我们正在接近达到L0文件数量的硬限制。当我们达到硬限制时，不要将单个写入延迟几秒钟，
      // 而是开始将每个写入延迟1ms，以减少延迟差异。此外，如果压缩线程与写入线程共享同一个内核，
      // 则此延迟将一些CPU交给压缩线程。
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) { // mem_没有超过阈值不做处理
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) { // 前一个imm_正在写入，阻塞等待
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      // 如果mem_已经满了，但是前一个imm_依旧正在写入中，则需要进行阻塞等待
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) { // level 0文件的个数达到了上限，需要阻塞等待
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else { // 达到阈值，尝试将mem_转换为imem_，正常转化
      // Attempt to switch to a new memtable and trigger compaction of old
      // 尝试切换到新的memtable并触发旧memtable的压缩，说明日志文件达到了预定大小时(默认是4MB)
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        // 避免在紧凑的循环中咀嚼文件编号空间。
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete log_;

      s = logfile_->Close();
      if (!s.ok()) {
        // We may have lost some data written to the previous log file.
        // Switch to the new log file anyway, but record as a background
        // error so we do not attempt any more writes.
        //
        // We could perhaps attempt to save the memtable corresponding
        // to log file and suppress the error if that works, but that
        // would add more complexity in a critical code path.
        // 我们可能丢失了一些写入前一个日志文件的数据。无论如何，切换到新的日志文件，
        // 但将其记录为后台错误，以便我们不再尝试任何写操作。我们也许可以尝试保存对应于日志文件的memtable，
        // 如果可以的话，可以抑制错误，但是这会在关键代码路径中增加更多的复杂性。
        RecordBackgroundError(s);
      }
      delete logfile_;
      
      // mem_则转变为imm_，并新建一个新mem_
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release); // 内存屏蔽
      mem_ = new MemTable(internal_comparator_); // 将mem_分配新的memtable 
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction(); // 生成压缩计划，将imm_持久化到磁盘
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

// GetApproximateSizes()获取指定范围值其文件的大小
void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
/* 以下是暴露给用户使用的最外层API接口 */
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
