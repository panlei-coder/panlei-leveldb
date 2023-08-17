// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  // 计算key的hash值
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilterPolicy : public FilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    // k_ = bits_per_key * 0.69 =~ ln(2)
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    // k_ must be in [1, 30]
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  const char* Name() const override { return "leveldb.BuiltinBloomFilter2"; }

  /*
  ×××××××××××××××××××××××××|k  
  dst的结构，即生成了布隆过滤器的内容，“|”前面的部分是bit位，后面的是k，表示使用了多少个hash函数
  */

  // 对keys数组的每一个元素都进行k_次hash
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    // Compute bloom filter size (in both bits and bytes)
    // 计算布隆过滤器大小(以位和字节为单位)
    size_t bits = n * bits_per_key_;

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    // 对于较小的n，我们可以看到非常高的假阳性率。通过强制执行最小布隆过滤器长度来修复它。
    if (bits < 64) bits = 64;

    // 计算需要的字节数和位数
    size_t bytes = (bits + 7) / 8; // 向上取整（字节数）
    bits = bytes * 8; // 得到真正需要的位数

    // Number of probes to remember in filter
    // 在过滤器中要记住的探测次数（便于后续使用该过滤器进行k_次探测来找到对应的key是否在其中）
    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);
    dst->push_back(static_cast<char>(k_));  // Remember # of probes in filter（将k添加到了记录bit位内容的尾部）
    
    // array用于存储过滤器的哈希值
    char* array = &(*dst)[init_size];  // 注意这里它不同于&dst[init_size]，注意dst使用[]一次性的偏移量(std::string**)与(*dst)使用[]一次性的偏移量(std::string&)是不同的

    // 对keys数组进行循环
    for (int i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      // 使用双重哈希生成一系列哈希值
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits 向右旋转17位（高15位放到低位，低17位放到高位）

      // 进行k_次循环
      for (size_t j = 0; j < k_; j++) {
        // 获取每个哈希值的位置
        const uint32_t bitpos = h % bits;
        // 将哈希值的位置设置为1
        array[bitpos / 8] |= (1 << (bitpos % 8));
        // 计算哈希值
        h += delta;
      }
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
    // 获取过滤器的字节大小
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    // 获取过滤器的数据，即存放bit位的数组
    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8; // 这是存放bit位的数据长度

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    // 使用编码的k，以便我们可以读取使用不同参数创建的bloom过滤器生成的过滤器。
    const size_t k = array[len - 1]; // 存放k，表示使用了多少次hash函数

    /*
    在这段代码中，`if (k > 30)` 的作用是处理可能的新编码情况。
    在布隆过滤器的设计中，最后一个字节用于存储探测次数 `k`，即使用多少个哈希函数进行哈希计算。
    通常情况下，`k` 的值不会超过 30。
    然而，如果将来在布隆过滤器的编码中引入了新的编码方式，可能会出现一些特殊情况，
    其中 `k` 的值超过了 30。为了兼容这种可能的新编码，当 `k` 大于 30 时，将其视为匹配。
    换句话说，如果布隆过滤器的编码方式发生了变化，并且 `k` 的值大于 30，那么为了向后兼容，
    将其视为匹配，即返回 `true`。这样做是为了处理未来可能的布隆过滤器编码变化情况，
    确保旧的查询操作可以正确地处理新编码的布隆过滤器。
    */
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      // 为布隆过滤器保留潜在的新编码。
      // 将其视为匹配。
      return true;
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_; // 每个key所占的bit位数
  size_t k_; // 进行k_次hash
};
}  // namespace

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
