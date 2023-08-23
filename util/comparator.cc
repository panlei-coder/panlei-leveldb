// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
/*
BytewiseComparatorImpl是LevelDB中内置的默认比较器,主要采用字典序对两个字符串进行比较
*/
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  // 返回一个短的字符串,这个字符串在start=<string<limit范围内
  // 这个函数的主要作用是是在压缩字符串的存储空间,在转化为SST时使用
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    // 找出连个字符串*start和*limit之间的共同前缀,如果没有共同前缀,则直接退出
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // 如果一个字符串是另一个字符串的前缀，不要缩短
      // Do not shorten if one string is a prefix of the other
    } else {
      /*
      如果有共同前缀,判断start共同前缀的后一个字符是否小于0xff,且后一个字符要小于limit中共同前缀的后一个字符,
      如果条件不满足,直接退出;如果满足,则将*start中共同前缀的的后一个字符加1,然后去掉后续所有的字符,并返回退出
      */
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1); // 需要有一个结束符的位置'\0'
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  // 返回一个较短的字符串string,该string >= key,该函数的作用同样是减少字符串的存储空间
  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    // 找到第一个可以加1的字符,即在字符串中找到第一个不为0xff的字符,并将该字符加1,然后丢弃
    // 后面所有的字符
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1); // 需要有一个结束符的位置'\0'
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
