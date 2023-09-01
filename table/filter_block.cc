// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg; // 2KB

// 构建
FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// 每2KB创建一个filter（注意：是一个DataBlock生成一个filter，但是fiter_offset_是每2KB生成一个，
// 注意while (filter_index > filter_offsets_.size())十个循环）
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 判断是否需要创建一个filter
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

// 添加一个key到过滤器中
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  // keys_当前的长度为下一个键开始的索引，将其放入到start_中
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 会生成一个元数据块，并分别写入布隆过滤器的内容、偏移量、内容总大小和基数
Slice FilterBlockBuilder::Finish() {
  // 如果start_变量不为空，说明由AddKey方法加入的键需要生成布隆过滤器
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // 将过滤器偏移量依次追加到result_变量中，每个偏移量占据固定的4字节空间
  const uint32_t array_offset = result_.size(); // array_offset相当于过滤器filter的大小，不包含偏移量
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 将过滤器内容总大小追加到result_中
  PutFixed32(&result_, array_offset);
  // 将过滤器基数追加到result_中
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

// 生成布隆过滤器
void FilterBlockBuilder::GenerateFilter() {
  // 所有key的总个数
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) { // 分离出每一个key，并添加到tmp_keys_
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 为当前键集生成筛选器并追加到结果。
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

// 构造
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size(); // 即元数据块的内容
  // 因为元数据块至少包括1字节的过滤器基数以及4字节的过滤器内容总大小，，因此字节数不会小于5字节
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  // 读取最后1字节的过滤器基数放到成员变量base_lg_中
  base_lg_ = contents[n - 1]; 
  // 将过滤器内容总大小的值放到last_word中
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data(); // data_成员变量指向元数据块的开始位置
  offset_ = data_ + last_word; // 为过滤器偏移量开始位置
  num_ = (n - 5 - last_word) / 4; // 为过滤器偏移量的个数
}

// 利用过滤器判断某个key是否存在
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_; // block_offset / kFilterBase
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter); // 判断是否存在
    } else if (start == limit) { // 说明没有过滤器
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
