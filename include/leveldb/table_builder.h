// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;

class LEVELDB_EXPORT TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  // 创建一个构建器，它将在*file中存储它正在构建的表的内容。
  // 不关闭文件。调用完Finish()后，由调用者决定是否关闭文件。
  TableBuilder(const Options& options, WritableFile* file);

  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TableBuilder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  // 更改此构建器使用的选项。注意:只有部分选项字段可以在构建后更改。
  // 如果不允许动态更改字段，并且传递给构造函数的结构中的值与传递给此方法的结构中的值不同，
  // 则此方法将在不更改任何字段的情况下返回错误。
  Status ChangeOptions(const Options& options);

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  // 向正在构造的表添加键、值。
  // 根据比较器，键位于先前添加的键之后。
  // require: Finish()， Abandon()没有被调用
  void Add(const Slice& key, const Slice& value);

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  // 高级操作:将所有缓冲的键/值对刷新到文件中。可以用来确保两个相邻的条目永远不会存在于同一个数据块中。
  // 大多数客户端不需要使用这种方法。require: Finish()， Abandon()没有被调用
  void Flush();

  // Return non-ok iff some error has been detected.
  Status status() const;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  // 完成表的构建。在此函数返回后停止使用传递给构造函数的文件。
  // require: Finish()， Abandon()没有被调用
  Status Finish();

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  // 指示应该放弃此构造器的内容。在此函数返回后停止使用传递给构造函数的文件。
  // 如果调用者不打算调用Finish()，它必须在销毁这个构造器之前调用Abandon()。
  // require: Finish()， Abandon()没有被调用
  void Abandon();

  // Number of calls to Add() so far.
  // 到目前位置调用Add()的次数
  uint64_t NumEntries() const;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  // 到目前为止生成的文件大小。如果在成功调用Finish()之后调用，则返回最终生成文件的大小。
  uint64_t FileSize() const;

 private:
  bool ok() const { return status().ok(); }
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  /*
  这里提前声明了Rep结构体，并且将Rep结构体定义table_builder.cc文件中，
  这样的好处是当修改了Rep结构之后，用户是并不知道Rep的结构被修改了，对用户是透明的
  */
  struct Rep;
  Rep* rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
