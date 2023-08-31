// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "util/coding.h"

namespace leveldb {

/*

SeqNum(8字节)|key键值对个数(4字节)|kTypeValue(1字节-kTypeValue)|key长度变长32位|key具体数据|value长度变长32位|value具体数据
SeqNum(8字节)|key键值对个数(4字节)|kTypeValue(1字节-kTypeDeletion)|key长度变长32位|key具体数据

需要说明的是WriteBatch的SeqNum是前一个SeqNum + 1得到的，这样的目的是整个Batch是一个整体积，所以公用一个SeqNum。
但是MemTable中会保证一个键值对对应一个SeqNum

*/

// WriteBatch头有一个8字节的序列号，后面跟着一个4字节的计数。
// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

// 返回WriteBatch的大致大小
size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

// Iterate函数主要来自InsertInto函数，主要是解析数据然后将其插入到memtable
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) { // 判断rep_是否小于kHeader
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  // 删除前8个字节的SeqNum和4字节的record count
  input.remove_prefix(kHeader); // TODO ??? 为什么这里只做了一个删除kHeader的操作,那岂不是多个key共享一个SeqNum操作
  Slice key, value;
  int found = 0;
  while (!input.empty()) { 
    found++;
    // 解析当前key/value的状态,取值为[kTypeValue|kTypeDeletion]
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
      // 解析key和value的长度,并获取key和value
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value); // 实际插入到了memtable中
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
      // 解析key的长度,并获取key
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key); // 在memtable添加删除记录
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }

  // 判断遍历之后的found是否与WriteBatch中记录的record count是否相同
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

// 获取WriteBatch存储的记录数量
int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8); 
}

// 设置WriteBatch存储的记录数量
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

// 获取WriteBatch存储的SeqNum
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

// 设置WriteBatch存储的SeqNum
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

// 添加一个键值对到当前的WriteBatch
void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

// 添加一个删除键到当前的WriteBatch
void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

// 将另一个WriteBatch追加到当前的WriteBatch
void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

// 处理MemTable插入的类
namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_; // 序列号
  MemTable* mem_; // 可变的MemTable
  
  // 将键值对添加到MemTable
  void Put(const Slice& key, const Slice& value) override {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++; // 序列号自增
  }

  // 将删除键添加到MemTable
  void Delete(const Slice& key) override {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++; // 序列号自增
  }
};
}  // namespace

// 将WriteBatch中保存的键值对一一添加到MemTable中
Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

// 设置WriteBatch的rep_
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

// 将另一个WriteBatch(src)追加到WriteBatch(dst)
// 注意:依旧保证合并后的WriteBatch共享一个SeqNum
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
