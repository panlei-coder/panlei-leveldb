// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  // seq的最大值为kMaxSequenceNumber
  assert(seq <= kMaxSequenceNumber);
  // t的最大值为kValueTypeForSeek
  assert(t <= kValueTypeForSeek);
  // 将sequence与valueType拼接起来,sequence占据高7字节,valueType占据低1字节
  return (seq << 8) | t;
}

// 将InternalKey添加到result中
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

// ParsedInternalKey转换为string
std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss; // 输出到字符串缓存区
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

// InternalKey转换为string
std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss; // 输出到字符串缓存区
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

// 返回比较器的名称
const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

// 比较两个InternalKey
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  // 首先比较两个user_key_的大小
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) { // 如果相等,则比较序列号
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

// 寻找start和limit两者之间的最短分隔符
// 该方法的目的是找到一个最短的字符串start，使得它在逻辑上小于limit，并且在物理上尽可能地与start相似。
void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  // 尝试缩短键的user部分(提取user_key_)
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);

  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    // user_key_在物理上变短了，但逻辑上变大了。
    // 在缩短的user_key_上添加最早可能的数字。
    // 将最大序列号和用于查找的值类型添加到tmp的末尾
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0); // 确保start在tmp之前
    assert(this->Compare(tmp, limit) < 0); // 确保tmp在limit之前
    start->swap(tmp); // 交换start和tmp的内容,使得start成为缩短后的字符串
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    // user_key_在物理上变短了，但逻辑上变大了。
    // 在缩短的user_key_上添加最早可能的数字。
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

// 返回policy_name_
const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

// 创建Filter
void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]); // 提取user_key_
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

// 判断key是否在过滤器f中
bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

/*
key_length(varint32)|user_key(blob)|sequence(7B)|type(1B)|value_length(varint32)|value(blob)

start_**************kstart_******************************end_*******************************
********************|<----------internal key------------>|**********************************
********************|<--user key-->|*********************|**********************************
<--------------------------memtable key----------------->|**********************************
*/

// 构建LookupKey
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size(); 
  size_t needed = usize + 13;  // A conservative estimate(保守估计)
  char* dst;
  if (needed <= sizeof(space_)) { // 如果所需空间小于等于预分配的空间,则使用预分配空间
    dst = space_;
  } else {
    dst = new char[needed]; // 否则动态分配
  }
  start_ = dst; // 设置起始位置
  dst = EncodeVarint32(dst, usize + 8); // 将user_key_长度+8编码为varint32,并存放到dst中
  kstart_ = dst;  // 设置kstart_
  std::memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb
