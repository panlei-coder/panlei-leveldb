// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

/*
DB API:主要用于封装一些供客户端应用进行调用的接口,即头文件中的相关API函数接口,
客户端应用可以通过这些接口实现数据引擎的各种操作
*/

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  // 存放手动compact的信息
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range 
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_; // interal_key的比较器
  const InternalFilterPolicy internal_filter_policy_; // 布隆过滤器
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_; // 是否拥有日志文件
  const bool owns_cache_; // 是否拥有cache缓存
  const std::string dbname_; // DB名称

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_; // 用来存放SSTable的缓存（Block_Cache才是具体缓存DataBlock的，TableCache只是存放基本的元信息等）

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_; // 锁定持久DB状态。成功获取非空iff。

  // State below is protected by mutex_
  port::Mutex mutex_; // 互斥锁
  std::atomic<bool> shutting_down_; // DB是否关闭
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  // @todo  mem_是多线程访问，但是对于imm_是只有单线程进行压缩的（源代码中似乎是这样的）？？？
  MemTable* mem_;  // 内存中的可变Table（skiplist），有mutex_进行保护的
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted // 内存中的不可变Table ，多线程将imm_转换为SSTable，写入磁盘
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_ 后台线程来检测是否有imm_
  WritableFile* logfile_;  // 写到缓存中，后续要通过log_刷到磁盘
  uint64_t logfile_number_ GUARDED_BY(mutex_); // 日志文件序列号
  log::Writer* log_; // 日志文件句柄
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling. 随机种子

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_); // 一个写队列
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_); // 用来暂时存放进行批处理操作的变量（例如将多个WriteBatch合并成一个WriteBatch，都是使用的这个变量）

  SnapshotList snapshots_ GUARDED_BY(mutex_); // 快照链表

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  // 要防止删除的一组表文件，因为它们是正在进行的压缩的一部分。
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  // 是否计划或者正在进行后台压缩
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  // 手动压缩
  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  // Version集合
  VersionSet* const versions_ GUARDED_BY(mutex_);

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_); // 记录后台运行的错误信息

  // 压缩的状态信息
  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
// 清理db选项。如果result.info日志不等于src.info日志，调用者应该删除它。
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
