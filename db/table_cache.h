// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);

  TableCache(const TableCache&) = delete;
  TableCache& operator=(const TableCache&) = delete;

  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  // 返回指定文件号的迭代器(对应的文件长度必须正好是文件大小字节)。
  // 如果tableptr非空，也设置*tableptr指向返回的迭代器底层的Table对象，
  // 如果没有返回的迭代器底层的Table对象，则设置*tableptr指向nullptr。
  // 返回的*tableptr对象归缓存所有，不应该被删除，只要返回的迭代器是活的，它就有效。
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number,
                        uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  // 如果查找指定文件中的内部键k找到了一个条目，调用(*handle result)(arg, found key, found value)。
  Status Get(const ReadOptions& options, uint64_t file_number,
             uint64_t file_size, const Slice& k, void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  // 驱逐指定文件号的任何条目
  void Evict(uint64_t file_number);

 private:
  // 查找指定的SSTable，优先去缓存找，找不到则去磁盘读取，读完之后在插入到缓存中
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

  Env* const env_; // 用于读取SST文件，统一的操作文件的接口
  const std::string dbname_; // SST文件的名字
  const Options& options_; // Cache参数配置
  Cache* cache_; // 缓存基类句柄，存放的是TableAndFile
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
