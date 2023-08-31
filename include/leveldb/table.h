// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
// 表是从字符串到字符串的排序映射。表是不可变的和持久的。
// 一个Table可以在没有外部同步的情况下被多个线程安全地访问。
class LEVELDB_EXPORT Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  // 试图打开以字节[0..file_size)，并读取允许从表中检索数据所需的元数据项。
  // 如果成功，返回ok并将*table设置为新打开的表。当不再需要时，客户端应该删除*表。
  // 如果在初始化表时出现错误，则将*table设置为nullptr并返回非ok状态。
  // 不获取*source的所有权，但客户端必须确保source在返回的表生命周期内保持活动状态。
  // *文件必须保持活动状态
  /*
  （1）静态函数，而且是唯一的公有接口
  （2）主要负责解析出基本数据，如Footer，然后解析出meta index block/index block/filter block/data block
   (3)BlockContents对象到Block的转换，其中主要是计算出restart_offset_;而且Block是可以被遍历的
  */
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Table** table);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // 返回表内容上的新迭代器。NewIterator()的结果最初是无效的(调用者在使用迭代器之前必须调用其中一个Seek方法)。
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  // 给定一个键，返回该键数据开始的文件中的一个近似字节偏移量(如果该键存在于文件中，则返回该键数据开始的位置)。
  // 返回值以文件字节为单位，因此包含诸如压缩底层数据之类的效果。例如，表中最后一个键的近似偏移量将接近文件长度。
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  friend class TableCache;
  struct Rep;

  // 生成读取Block的迭代器
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  explicit Table(Rep* rep) : rep_(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  // 调用(*handle result)(arg，…)，并在调用Seek(key)后找到条目。如果筛选策略说该键不存在，可能不会进行这样的调用。
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v));

  // 读取元数据
  void ReadMeta(const Footer& footer);
  // 读取Filter
  void ReadFilter(const Slice& filter_handle_value);

  Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
