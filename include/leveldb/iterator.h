// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

// 支持直接对集合中的首元素和末尾元素进行访问,还可以根据实际的key进行元素定位,此外还支持正向迭代和反向迭代
class LEVELDB_EXPORT Iterator {
 public:
  Iterator();

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  // 迭代器在析构时会遍历cleanup_head_链表中的所有节点,并调用相应的函数,实现迭代器相关资源的释放与清除
  virtual ~Iterator();

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  // 判断迭代器是否指向一个数据或非法
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  // 将迭代器指向集合中的第一个元素
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  // 将迭代器指向集合中的最后一个元素
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  // 将迭代器指向集合中key为target的元素
  virtual void Seek(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  // 将迭代器指向下一个元素
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  // 将迭代器指向前一个元素
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  // 返回迭代器目前指向元素的key
  virtual Slice key() const = 0;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  // 返回迭代器目前指向元素的value
  virtual Slice value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  // 返回处理的状态信息
  virtual Status status() const = 0;

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.
  //
  // Note that unlike all of the preceding methods, this method is
  // not abstract and therefore clients should not override it.
  // 允许客户端注册函数/arg1/arg2三元组，当这个迭代器被销毁时调用。
  // 注意，与前面的所有方法不同，这个方法不是抽象的，因此客户机不应该覆盖它。
  using CleanupFunction = void (*)(void* arg1, void* arg2);
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

 private:
  // Cleanup functions are stored in a single-linked list.
  // The list's head node is inlined in the iterator.
  // 清理函数存储在一个单链表中。列表的头节点内联在迭代器中。
  struct CleanupNode {
    // True if the node is not used. Only head nodes might be unused.
    bool IsEmpty() const { return function == nullptr; }
    // Invokes the cleanup function.
    void Run() {
      assert(function != nullptr);
      (*function)(arg1, arg2);
    }

    // The head node is used if the function pointer is not null.
    // 如果函数指针不为空，则使用头节点。
    CleanupFunction function;
    void* arg1;
    void* arg2;
    CleanupNode* next;
  };
  CleanupNode cleanup_head_; // 清理函数链表的头节点
};

// Return an empty iterator (yields nothing).
LEVELDB_EXPORT Iterator* NewEmptyIterator(); // 返回空的迭代器

// Return an empty iterator with the specified status.
LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status); // 返回带有特定的status的空迭代器

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
