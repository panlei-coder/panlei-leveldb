// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"

namespace leveldb {

// 构建
Iterator::Iterator() {
  // 初始化清理链表的头
  cleanup_head_.function = nullptr;
  cleanup_head_.next = nullptr;
}

// 析构
Iterator::~Iterator() {
  if (!cleanup_head_.IsEmpty()) { // 判断是否为空
    cleanup_head_.Run(); // 依次遍历运行保存的所有的清除函数
    for (CleanupNode* node = cleanup_head_.next; node != nullptr;) {
      node->Run();
      CleanupNode* next_node = node->next;
      delete node;
      node = next_node;
    }
  }
}

// 注册清除函数
void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  CleanupNode* node;
  if (cleanup_head_.IsEmpty()) {
    node = &cleanup_head_;
  } else { // 头插
    node = new CleanupNode();
    node->next = cleanup_head_.next;
    cleanup_head_.next = node;
  }
  node->function = func;
  node->arg1 = arg1;
  node->arg2 = arg2;
}

// 匿名空间，其中定义的函数都只能够在当前文件中使用
namespace {

class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) {}
  ~EmptyIterator() override = default;

  bool Valid() const override { return false; }
  void Seek(const Slice& target) override {}
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  Status status() const override { return status_; }

 private:
  Status status_;
};

}  // anonymous namespace

// 暴露给外部的使用接口，不同够直接调用匿名空间中的函数
Iterator* NewEmptyIterator() { return new EmptyIterator(Status::OK()); } // 状态是OK

Iterator* NewErrorIterator(const Status& status) { // 状态是传入的错误状态码
  return new EmptyIterator(status);
}

}  // namespace leveldb
