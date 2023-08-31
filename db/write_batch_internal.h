// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
// WriteBatchInternal提供了静态方法来操作WriteBatch，我们不希望在公共WriteBatch接口中使用这些方法。
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  // 返回批处理的条目数。
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  // 设置批处理中条目数的计数。
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  // 返回此批处理开始的序列号
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  // 将指定的数字存储为此批处理开始时的序列号。
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // 返回WriteBatch具体保存的内容
  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  // 返回保存的内容的size
  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  // 设置WriteBatch具体内容
  static void SetContents(WriteBatch* batch, const Slice& contents);

  // 将MemTable内容插入到WriteBatch
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  // 将src追加到dst
  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
