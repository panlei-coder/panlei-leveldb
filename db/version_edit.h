// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;
/*
SSTable产生变化的触发条件：
（1）将Immutable转为SSTable时。
（2）当后台开始进行Compact时。（一个是level0层超过了指定的文件个数时，另一个是当某个文件被seek的次数达到阈值时,......）
*/
struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs; // 当前文件被引用了多少次，同一个SSTable可以在不同version
  int allowed_seeks;  // Seeks allowed until compaction 文件允许被seek的次数，超过这个次数，就需要把整个文件Compact
  uint64_t number; // 文件编号
  uint64_t file_size;    // File size in bytes 文件大小
  InternalKey smallest;  // Smallest internal key served by table 文件的最小key（包含了序列号）
  InternalKey largest;   // Largest internal key served by table 文件的最大key（包含了学列号）
};

// VersionEdit类保存的是一个版本变动的信息，在某个基准版本上面，应用一个或者多个
// VersionEdit就可以得到新的版本。VersionEdit中存放了基于上一个版本增加的文件信息，
// 删除的文件信息。
/* 
Manifest文件可以看作是VersionEdit的日志，为了快速恢复需要将这些变更持久化到磁盘上。
*/
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  // AddFile()方法用于往VersionEdit类实例中添加版本变动需要增加的一个sstable文件，
  // 其中level是sstable文件所在的层数，file是sstable文件的file number，file_size是
  // sstable文件的大小，smallest和largest分别是sstable文件中存放的最小和最大的key信息
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  // DeleteFile()方法用于往VersionEdit类实例中添加一个版本变动需要被删除的sstable文件，
  // level是sstable文件所在的层级数，而file则是sstable文件的file number。
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_; // key的比较器名称，db一旦创建，排序的逻辑就必须保持兼容，不可变更
  uint64_t log_number_;  // 当前使用的日志编号
  uint64_t prev_log_number_; // 前一个日志编号
  uint64_t next_file_number_; // 下一个文件编号
  SequenceNumber last_sequence_; // 最新的键值对序列号
  bool has_comparator_; // 是否有比较器
  bool has_log_number_; // 是否有日志编号
  bool has_prev_log_number_; // 是否有前一个日志编号
  bool has_next_file_number_; // 是否有下一个文件编号
  bool has_last_sequence_; // 是否有最新的键值对序列号

  std::vector<std::pair<int, InternalKey>> compact_pointers_; // 每一层level层的compact pointer
  DeletedFileSet deleted_files_; // 要删除的SSTable文件
  std::vector<std::pair<int, FileMetaData>> new_files_; // 要添加的SSTable文件
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
