// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

// 设置二级迭代器时传入的回调函数（函数指针类型声明）
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  // 构造。对于SSTable来说：
  // 1、index_iter是指向index block的迭代器；
  // 2、block_function是Table::BlockReader,即读取一个block;
  // 3、arg是指向一个SSTable;
  // 4、options 读选项
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  // 针对一级迭代器，target是一个index block元素，
  // 这里就是seek到index block对应元素位置
  void Seek(const Slice& target) override;

  // 针对一级迭代器操作
  void SeekToFirst() override;
  void SeekToLast() override;

  // 针对二级迭代器操作
  void Next() override;
  void Prev() override;

  // 针对二级迭代器,指向DataBlock的迭代器是否有效
  bool Valid() const override { return data_iter_.Valid(); }
  // 针对二级迭代器,DataBlock中的一个Entry的Key
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }

  // 针对二级迭代器,DataBlock中的一个Entry的Value
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }

  // 针对二级迭代器,DataBlock中的一个Entry的Value
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  // 保存错误状态，如果最近一次状态是非ok状态，则不保存
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }

  // 跳过当前空的DataBlock,转到下一个DataBlock
  void SkipEmptyDataBlocksForward();
  // 跳过当前空的DataBlock,转到前一个DataBlock
  void SkipEmptyDataBlocksBackward();
  // 设置二级迭代器data_iter
  void SetDataIterator(Iterator* data_iter);
  // 初始化DataBlock的二级迭代器
  void InitDataBlock();

  BlockFunction block_function_; // 函数指针
  void* arg_; // 作为函数指针的参数
  const ReadOptions options_; // 作为函数指针的参数
  Status status_; // 两层迭代器的状态码
  IteratorWrapper index_iter_; // 一级迭代器，对于SSTable来说就是指向Index Block
  IteratorWrapper data_iter_;  // May be nullptr // 二级迭代器，对于SSTable来说就是指向Data Block
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_; // Data Block的Handle，记录其在SSTable中偏移量和大小
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

// 1、seek到target对应的一级迭代器位置;
// 2、初始化二级迭代器;
// 3、跳过当前空的DataBlock
void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward(); // 可能找不到target
}

// 同Seek()
void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward(); // 可能Block为空
}

// 同Seek()
void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward(); // 可能Block为空
}

// 二级迭代器的下一个元素，
// 对SSTable来说就是DataBlock中的下一个元素。
// 需要检查跳过空的DataBlck。
void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward(); // 可能一个Block遍历完了
}

// 二级迭代器的前一个元素，
// 对SSTable来说就是DataBlock中的前一个元素。
// 需要检查跳过空的DataBlck。
void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward(); // 可能一个Block遍历完了
}

// 针对二级迭代器。
// 如果当前二级迭代器指向为空或者非法;
// 那就向后跳到下一个非空的DataBlock。
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

// 针对二级迭代器。
// 如果当前二级迭代器指向为空或者非法;
// 那就向前跳到下一个非空的DataBlock。
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

// 设置二级迭代器
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

// 初始化二级迭代器指向。
// 对SSTable来说就是获取DataBlock的迭代器赋值给二级迭代器
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle); // 调用BlockReader
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

// 创建，唯一暴露给外部调用的接口，只有创建了该对象之后才能调用匿名空间中定义的成员函数
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
