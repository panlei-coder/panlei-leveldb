// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

/*
该Block是供DataBlock、Meta Index Block、Index Block三者使用的，而Meta Block有单独的存储格式，
见filter_block.h和filter_block.cc。
*/

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  // 重置内容，就像BlockBuilder刚刚被构造一样。
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // 自从上次调用Reset()之后，就没有再调用过Finish()。
  // key大于之前添加的键
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  // 完成构建块并返回一个引用块内容的片。返回的片将在此构建器的生命周期内保持有效，或者直到调用Reset()。
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  // 返回对正在构建的块的当前(未压缩)大小的估计。
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;          // 一些SSTable的基本信息，比如重启点等
  std::string buffer_;              // Destination buffer 序列化之后的数据
  std::vector<uint32_t> restarts_;  // Restart points 内部重启点具体数据，需要注意的是这里存放的是偏移量
  int counter_;                     // Number of entries emitted since restart 重启计数器，每16个重置一次
  bool finished_;                   // Has Finish() been called?  一个Block是否生成完成
  std::string last_key_;            // 记录上一次的key
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
