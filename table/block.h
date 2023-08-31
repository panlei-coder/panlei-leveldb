// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

/*
用于读取一个块并且在该块中查找一个键需要通过在Block类中生成一个迭代器来实现
*/

class Block {
 public:
  // Initialize the block with the specified contents.
  // 传入一个构建好的Block内容初始化Block对象
  // 禁止Block block = "contents";这种调用方法
  // 只能Block block = Block::Block("contents");
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  // 返回block的大小
  size_t size() const { return size_; }

  // 迭代器，用于遍历Block
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter; // 声明内部迭代器类

  uint32_t NumRestarts() const;

  const char* data_;         // 存放实际的数据（不包含压缩类型和CRC检验5字节内容）
  size_t size_;              // size_要参与和sizeof()的计算，同时它并不会为了持久化被编码，所以声明为size_t，其它的变量都是uint32_t（data_的大小）
  uint32_t restart_offset_;  // Offset in data_ of restart array restart array在data_中最开始位置的偏移量
  bool owned_;               // Block owns data_[] 表示data_是否是堆上创建的，还是mmap的
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
