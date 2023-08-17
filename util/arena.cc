// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096; // 一个块的大小

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  // 释放所有的Block
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

// 按需分配内存，可能会有内存浪费（分配指定大小的内存）
char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    // Object大于块大小的四分之一。单独分配它，以避免在剩余字节中浪费太多空间。
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // 如果需要分配的内存空间大于目前剩余剩余字节数，但是小于1/4的BlockSize，则将剩余字节位置更新
  // 至新分配的块，原先剩余的部分内存，直接丢弃，目的是快，浪费当前块中的剩余空间。
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

// 分配指定大小的内存
char* Arena::AllocateAligned(size_t bytes) {
  // 它根据指针的大小来判断应该选择多大的对齐字节数，并且最小为 8。
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // Ensure that the pointer is properly aligned
  // align为2的幂次时，(align & (align - 1))为0
  // 因为对齐字节数应该是 2 的幂次，以满足内存对齐要求。
  static_assert((align & (align - 1)) == 0, 
                "Pointer size should be a power of 2");
  // 计算当前分配指针alloc_ptr_的偏移量，以确定当前分配的内存是否满足对齐要求
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1); // 计算余数
  size_t slop = (current_mod == 0? 0 : align - current_mod); // 计算余数基于align的补数
  // The total number of bytes needed
  size_t needed = bytes + slop; // 计算需要满足对齐时需要分配的字节数

  char* result;
  if (needed <= alloc_bytes_remaining_) { // 判断剩余空间是否满足要求
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else { // 如果剩余字节数不够，就通过AllocateFallback分配
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0); // 判断分配完成之后的内存是对齐的
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  // 已经使用的内存包括被分配的block_bytes，还有指向该block的指针
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
