// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {
/*
如果要分配的字节数小于等于剩余的内存字节数，优先考虑在指定大小的Block块上进行内存分配。
如果要分配的字节数大于剩余的内存字节数，且要分配的内存空间大于指定BlockSize/4,
则直接另外开辟一段内存直接分配即可;但如果要分配的内存空间小于等于BlockSize/4，将剩余的内存空间直接丢弃掉，
然后重新分配一个固定BlockSize大小的内存块，在这个块上进行分配。
*/
class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  // 返回一个指向新分配的“bytes”字节的内存块的指针。
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  // 使用malloc提供的正常对齐保证分配内存。
  char* AllocateAligned(size_t bytes); // 分配偶数大小的内存，主要是skiplist节点，目的是加快访问

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  // 返回arena分配的数据的总内存使用估计。
  size_t MemoryUsage() const { // 返回已经使用的内存大小
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  char* AllocateFallback(size_t bytes);  // 按需分配内存，可能会有内存浪费
  char* AllocateNewBlock(size_t block_bytes); // 分配新的Block

  // Allocation state
  char* alloc_ptr_; // 指向用于分配的内存空间（指向某个正在使用的Block）
  size_t alloc_bytes_remaining_; // 剩余内存字节数（alloc_ptr_指向的内存空间剩余的未使用的字节数）

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_; // 实际分配的内存池（由一个个的Block组成，各Block的大小可能不相同）

  // Total memory usage of the arena.
  //
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  std::atomic<size_t> memory_usage_; // 记录内存的使用情况（原子操作），这里记录的是一起总共使用了多少字节数的内存
};

// 返回一个指向新分配的“bytes”字节的内存块的指针。
inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  // 如果我们允许0字节分配，返回的语义会有点混乱，所以我们在这里不允许它们(我们不需要它们用于内部使用)。
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) { // 判断剩余内存是否满足要求（优先考虑当前容量是否足够）
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
