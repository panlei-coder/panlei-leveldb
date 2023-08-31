// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  // @todo 这里为什么会是5 ？？？因为varint32最大为5字节，所有这里是先获取Slice的长度
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted(假设p没有损坏)
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

// 利用内存管理arena_估计memtable大致使用的内存大小
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

// 定义比较函数
int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
// 为target编码一个合适的内部键目标并返回它。使用*scratch作为scratch空间，返回的指针将指向这个scratch空间。
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data(); 
}

// memtable的迭代器，因为memtable的本质还是skiplist，
// 所以它的本质还是对skiplist的迭代，基本上都是调用skiplist的接口函数
class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

// 添加一条键值对到memtable中
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]

  // 1.计算各部分占用的字节数，然后计算总的字节数
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  // 计算memtable中一条记录的长度
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  
  // 2.根据计算的总字节数，为一条记录分配内存空间
  char* buf = arena_.Allocate(encoded_len);

  // 3.将key_size添加到缓存中
  char* p = EncodeVarint32(buf, internal_key_size); 
  std::memcpy(p, key.data(), key_size);
  p += key_size;

  // 4.将sequence和valueType整合写入到buf中
  EncodeFixed64(p, (s << 8) | type);
  p += 8;

  // 5.将value_size和value写入到缓存buf中
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);

  // 6.将一条键值对的记录添加到memtable中
  table_.Insert(buf);
}

// 根据lookupkey（key_length(varint32)|user_key(blob)|sequence(7B)|type(1B)）
// 在memtable中寻找对应的键值对
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  // 1.根据key在skiplist上找到对应的node
  Slice memkey = key.memtable_key(); // 其实就是lookupkey
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data()); // 将iter定位到第一个大于等于memkey的节点上
  
  // 2.判断iter是否合法，如果合法则获取器指向的memtableEntry的key是否与LookupKey中的user_key相等
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length); // 因为varint32最大字节数为5Byte
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
