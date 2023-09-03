// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

// 匿名空间
namespace {
/*
MergingIterator用于合并多个SST文件的迭代器
*/
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  // Valid()函数用于判断当前迭代器指向的是否是合理的键值对
  bool Valid() const override { return (current_ != nullptr); }

  // SeekToFirst()函数用于将所有的子节点都定位到最开头，找到最小的
  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest(); // 找到最小的
    direction_ = kForward; // 正向遍历
  }

// SeekToLast()函数用于将所有的子节点都定位到尾部，找到最大的
  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast(); 
    }
    FindLargest(); // 找到最大的
    direction_ = kReverse; // 反向遍历
  }

// Seek()函数用于找到目前target
  void Seek(const Slice& target) override {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

// Next()函数用于获取下一个键值对（多路归并的思想，按照从小到大的顺序依次获取多个要合并的SSTable的键值对）
  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    /*
     我们要确保所有的迭代器都往前走
     如果这里不是从前往后找，也就是从小到大
     那么此时current是最大值，此时其他的迭代器我们就需要往前更新，至少>=当前的key，否则
     其他的迭代器可能就没有机会，部分key就会漏掉
     */
    if (direction_ != kForward) { // 如果迭代器不是正向遍历的
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) { // 要找到第一个大于current_指向的key
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next(); // 更新current_指向下一个key-value
    FindSmallest();
  }

// prev()函数用于获取前一个的键值对（多路归并的思想，按照从大到小的顺序依次获取多个要合并的SSTable的键值对）
  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
       /*
     我们要确保所有的迭代器都往后走
     如果这里不是从后往前找，也就是从大到小
     那么此时current是最小值，此时其他的迭代器我们就需要往后更新，至少<=当前的key，否则
     其他的迭代器可能就没有机会，部分key就会漏掉
     */
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev(); // 找到第一个小于current_指向的key
          } else { // 定位到最后一个key
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev(); // 更新current_指向前一个key-value
    FindLargest();
  }

// key()返回迭代器current_指向的键
  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

// value()返回迭代器current_指向的值
  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

// status()函数用于返回所有子迭代器的状态码
  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest(); // 依次获取多个SSTable的迭代器指向的最小key
  void FindLargest(); // 依次获取多个SSTable的迭代器指向的最大key

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  // 如果有很多子节点，我们可能需要使用堆。现在我们使用一个简单的数组，因为我们希望leveldb中的子元素数量非常少。
  const Comparator* comparator_; // 比较器
  IteratorWrapper* children_; // 这里是为了防止迭代器太多，所以直接在对上分配（多个SSTable的迭代器）
  int n_; // 子节点的个数，即children_数组的大小
  IteratorWrapper* current_; // 当前有效的迭代器，因为有多个，所以需要实时记录当前所用的（即获取值的那个子迭代器）
  Direction direction_; // 表示迭代器的方向
};

// FindSmallest()函数用于找到所有子迭代器指向的最小的key
void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

// FindLargest()函数用于找到所有子迭代器指向的最大的key
void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

// 唯一暴露给外部调用的接口，用来创建多个SSTable文件合并的迭代器
Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
