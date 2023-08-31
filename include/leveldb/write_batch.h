// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT WriteBatch {
 public:
  class LEVELDB_EXPORT Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };

  WriteBatch();

  // Intentionally copyable.
  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  // 将"key->value"存储在数据库中。
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  // 如果数据库包含"key"的映射，则删除它。否则什么也不做。
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  // 清除此批处理中缓冲的所有更新。
  void Clear();

  // The size of the database changes caused by this batch.
  //
  // This number is tied to implementation details, and may change across
  // releases. It is intended for LevelDB usage metrics.
  // 此批处理导致数据库大小发生变化。这个数字与实现细节有关，
  // 并且可能在不同版本之间发生变化。它用于LevelDB使用度量。
  size_t ApproximateSize() const; // 内存状态信息

  // Copies the operations in "source" to this batch.
  //
  // This runs in O(source size) time. However, the constant factor is better
  // than calling Iterate() over the source batch with a Handler that replicates
  // the operations into this batch.
  // 将源代码中的操作复制到此批处理。这在O(源大小)时间内运行。
  // 但是，常量因素比使用Handler在源批处理上调用Iterate()要好，该Handler将操作复制到此批处理中。
  // 注意:多个WriteBatch还可以继续合并
  void Append(const WriteBatch& source);

  // Support for iterating over the contents of a batch.
  // 批处理的内容支持迭代。
  // 为了方便扩展,参数使用的是Handler基类,对应的是抽象工厂模式
  Status Iterate(Handler* handler) const;

 private:
  friend class WriteBatchInternal; // 内部工具性质的辅助类

  // 存放具体的数据
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
