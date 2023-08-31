// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

// 构造
BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0 第一个重启点是在偏移量为0处开始的
}

// 重置
void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0 第一个重启点是在偏移量为0处开始的
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

// 计算当前的Block的size（不包含压缩类型和CRC检验的5字节内容）
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

// 完成Block的纯数据内容部分的构建
Slice BlockBuilder::Finish() {
  // Append restart array
  // 将重启点偏移量写入buffer_
  for (size_t i = 0; i < restarts_.size(); i++) {
    // 因为要进行二分查找，所以使用固定大小的空间来存储restart point
    PutFixed32(&buffer_, restarts_[i]);
  }

  // 将重启点的个数追加到buffer_中
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true; // 已经生成了一个Block出来（不包含压缩类型和CRC检验的5字节内容）
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  // last_key_保存上一个加入的键
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);

  // shared用来保存本次加入的键和上一个加入的键的共同前缀长度
  size_t shared = 0;
  // 查看当前已经插入的键值对数量是否已经超出了16
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    // 计算共同前缀的长度
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    // 如果键值对已经超过了16,则开启新的重启点
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }

  // non_shared为键的长度减去共同前缀的长度，即非共享部分的键长度
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  // 将共同前缀长度、非共享部分长度、值长度分别写入buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  // 将键的非共享部分数据追加到buffer_中
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  // 更新last_key_为当前写入的键
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  // 将当前buffer_中写入键值对的数量加1
  counter_++;
}

}  // namespace leveldb
