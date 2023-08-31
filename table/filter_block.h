// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
// FilterBlockBuilder用于构造特定表的所有过滤器。它生成一个字符串，作为一个特殊的块存储在Table中。
// 调用FilterBlockBuilder的顺序必须匹配regexp:(StartBlock AddKey*)
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  // 目前levelDB中支持的布隆过滤器
  const FilterPolicy* policy_;   
  // 生成布隆过滤器的key，注意keys_类型是string，此处会将所有的key依次追加到keys_中，
  // 例如有3个key：level/level1/level2，则keys_为levellevel1level2
  std::string keys_;             // Flattened key contents 
  // 数组类型，保存keys_参数中每一个key的开始索引，例如上述例子中keys_为levellevel1level2，start_数组中保存的值为0,5,11（偏移量）
  std::vector<size_t> start_;    // Starting index in keys_ of each key 每一个key在keys_中的偏移量
  // 保存生成的布隆过滤器内容，
  std::string result_;           // Filter data computed so far // 构建的filter二进制数据，它是以追加的方式
  // 生成布隆过滤器时，会通过keys_和start_拆分出每一个键，将拆分出来的每一个键保存到tmp_keys数组中
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument 
  // 过滤器偏移量，即每一个过滤器在元数据块中的偏移量
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_; // 布隆过滤器
  const char* data_;    // Pointer to filter data (at block-start) 指向元数据块的开始位置
  const char* offset_;  // Pointer to beginning of offset array (at block-end) 指向元数据块中过滤器偏移量的开始位置
  size_t num_;          // Number of entries in offset array 过滤器偏移量的个数
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file) 过滤器基数
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
