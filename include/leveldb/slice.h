// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

#include "leveldb/export.h"

namespace leveldb {
/*
slice是levelDB中自定义的字符串处理类，主要是因为标准库中的string：
（1）默认语义为拷贝，会损失性能（在可预期的条件下，指针传递即可）
（2）标准库不支持remove_prefix和starts_with等函数，不太方便
*/
class LEVELDB_EXPORT Slice {
 public:
  // Create an empty slice.
  // 创建空的slice
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  // 创建一个引用d[0,n-1]的slice
  Slice(const char* d, size_t n) : data_(d), size_(n) {}

  // Create a slice that refers to the contents of "s"
  // 创建一个引用string的slice
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // Create a slice that refers to s[0,strlen(s)-1]
  // 创建一个引用s[0,strlen(s)-1]的slice
  Slice(const char* s) : data_(s), size_(strlen(s)) {}

  // Intentionally copyable.
  Slice(const Slice&) = default;
  Slice& operator=(const Slice&) = default;

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    // 检查n是否超出范围
    assert(n < size());
    // 返回data_数组中索引为n的元素
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() {
    // 将data_设置为空字符串
    data_ = "";
    // 将size_设置为0
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    // 如果n大于size()，则n就是size()
    assert(n <= size());
    // 将data_和size_减去n
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  std::string ToString() const { return std::string(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    // 比较字符串长度和字符串前缀内容是否相等
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  const char* data_;
  size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
  // 计算x和y的长度，并与memcmp进行比较
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

inline int Slice::compare(const Slice& b) const {
  // First, compare the length of the two slices.
  const size_t min_len = (size_ < b.size_)? size_ : b.size_;
  // NOTE: memcmp compares unsigned values, so we don't want to use
  // the built-in memcmp when possible.
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    // If they are the same length, compare the last characters.
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_
