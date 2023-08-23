// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

/*
key_length(varint32)|user_key(blob)|sequence(7B)|type(1B)|value_length(varint32)|value(blob)

start_**************kstart_******************************end_*******************************
********************|<----------internal key------------>|**********************************
********************|<--user key-->|*********************|**********************************
<--------------------------memtable key----------------->|**********************************
*/

// Grouping of constants.  We may want to make some of these
// parameters set via options.
// 希望通过选项设置其中的一些参数
namespace config {
// LSM树的层数
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
// 当有kL0_CompactionTrigger个文件时，0级压缩开始
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
// 0级文件数量软限制，此时减慢写入速度
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
// 0级文件的最大数目，此时需要停止写入
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
/*
如果一个新压缩的memtable不产生重叠，它被推入的最大级别。尝试推到级别2,以避免相对昂贵的级别0=>1压缩，
并避免一些昂贵的清单文件操作。我们不会一直推到最大级别，因为如果重复覆盖相同的键空间，可能会产生大量浪费
的磁盘空间。
*/
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
// 迭代期间读取的数据样本之间的近似字节间隔
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
// 作为内部键的最后一个组件编码的值类型。不要改变这些枚举值:它们嵌入在磁盘上的数据结构中。
enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 }; // 操作类型：删除和添加key
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
/*
kValueTypeForSeek定义了在构造ParsedInternalKey对象以查找特定序列号时应该传递的ValueType
(由于我们按降序对序列号进行排序，并且值类型嵌入在内部键中序列号的低8位，
因此我们需要使用编号最高的ValueType，而不是最低的)。
记录当前操作类型，后续压缩会根据该类型进行对应操作
*/
static const ValueType kValueTypeForSeek = kTypeValue;

// 递增的序列号，相同key则按照其降序
typedef uint64_t SequenceNumber;

/*  
注意：SequenceNumber的高7个字节用来存放具体的序列号，而最低字节存放ValueType
*/

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
// 我们在底部留下8位空，以便类型和序列#可以打包成64位。
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

struct ParsedInternalKey {
  Slice user_key;  // 用户输入的数据key（slice格式）
  SequenceNumber sequence; // 序列号
  ValueType type; // 操作类型

  ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {}
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
// 返回"key"的编码长度
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8; // 这里的8是指SequenceNumber和ValueType合并一共占用8字节
}

// Append the serialization of "key" to *result.
// 将"key"的序列化附加到*result
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
// 尝试解析"internal_key"中的内部密钥。如果成功，将解析后的数据存储在"*result"中，
// 并返回true。出错时，返回false，使"*result"处于未定义状态。
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// Returns the user key portion of an internal key.
// 返回internal key的user key部分。
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
// internal key的比较器，使用指定的比较器对user key部分进行比较，并通过减少序列号来打破连接。
// 主要使用在memtable中
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_; // 用户键值的比较变量

 public:
  // 内部键值的比较
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  // 比较器名字，以levelDB开头
  const char* Name() const override;
  // 支持三种操作：大于/等于/小于
  int Compare(const Slice& a, const Slice& b) const override;
  /*
  这些功能用于减少索引块等内部数据结构的空间需求;
  如果*start < limit，则将*start更改为[start,limit)中的短字符串;
  简单的比较器实现可能会以*start不变返回，即此方法的实现不执行任何操作也是正确的;
  然后再调用Compare函数。
  */
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override;
  /*
  将*key更改为string >= *key.Simple比较器实现可能会在*key不变的情况下返回，
  即，此方法的实现是正确的。
  */
  void FindShortSuccessor(std::string* key) const override;
  
  // 返回用户键值的比较器
  const Comparator* user_comparator() const { return user_comparator_; }
  //  比较两个InternalKey
  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
// 过滤从Internal key转换为user key的策略包装器
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;

 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override;
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
// 该目录中的模块应该将internal key封装在下面的类中，而不是普通的字符串，
// 这样我们就不会错误地使用字符串比较而不是InternalKeyComparator。
class InternalKey {
 private:
  std::string rep_; // 

 public:
  // rep_为空，表示无效
  InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  // 将slice赋值给InternalKey::rep_
  bool DecodeFrom(const Slice& s) {
    // 检查字符串是否为空
    rep_.assign(s.data(), s.size());
    return!rep_.empty();
  }

  // 将InternalKey::rep_编码成slice
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  // 返回InternalKey::rep_的user_key_
  Slice user_key() const { return ExtractUserKey(rep_); }

  // 将InternalKey::rep_设置成p
  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  // 清空InternalKey::rep_
  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

// 两个InternalKey的大小比较
inline int InternalKeyComparator::Compare(const InternalKey& a,
                                          const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

// 解析InternalKey
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  // 判断InternalKey的size大小是否小于8
  const size_t n = internal_key.size();
  if (n < 8) return false;

  uint64_t num = DecodeFixed64(internal_key.data() + n - 8); // 获取sequence和type
  uint8_t c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<uint8_t>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  // 初始化*this用于在具有指定序列号的快照中查找user_key。
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;

  ~LookupKey();
  // 各种key的转换
  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_; // 指向key开始的地址
  const char* kstart_; // 指向user_key_开始的地址
  const char* end_; // 指向结束的地址
  /*
  在 SSO 优化中，短字符串的容量通常被设置为一个固定的值，
  以便直接存储字符串内容，而不需要额外的内存分配。
  对于长字符串，容量将会超过 SSO 限制，字符串会被存储在堆上，而不是直接存储在字符串对象中。
  */
  char space_[200];  // Avoid allocation for short keys,类似string的sso优化
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
