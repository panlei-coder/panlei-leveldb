// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

class SnapshotList;

/*
快照并不是将所有数据进行完整的物理空间备份，而是保存每一个快照备份记录创建时刻的序列号，
在DBImpl类中专门有一个成员变量采用双向循环链表的数据结构，存储所有快照备份的序列号。
*/

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
// 快照保存在DB中的双链表中。每个SnapshotImpl对应一个特定的序列号。

class SnapshotImpl : public Snapshot {
 public:
  SnapshotImpl(SequenceNumber sequence_number)
      : sequence_number_(sequence_number) {}

  SequenceNumber sequence_number() const { return sequence_number_; }

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list. The SnapshotList
  // implementation operates on the next/previous fields directly.
  // SnapshotImpl保存在一个双链循环列表中。SnapshotList的实现直接对next/previous字段进行操作。
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  const SequenceNumber sequence_number_; // 快照创建时刻的序列号
// 调试模式下
#if !defined(NDEBUG)
  SnapshotList* list_ = nullptr;
#endif  // !defined(NDEBUG)
};

// 快照的双向循环链表
class SnapshotList {
 public:
  SnapshotList() : head_(0) {
    head_.prev_ = &head_;
    head_.next_ = &head_;
  }

  // 判断链表是否为空
  bool empty() const { return head_.next_ == &head_; }

  // 获取最老版本的快照（链表的第一个节点保存的）
  SnapshotImpl* oldest() const {
    assert(!empty());
    return head_.next_;
  }

  // 获取最新版本的快照（新一版的快照在链表的尾部）
  SnapshotImpl* newest() const {
    assert(!empty());
    return head_.prev_;
  }

  // Creates a SnapshotImpl and appends it to the end of the list.
  // 创建一个SnapshotImpl并将其附加到列表的末尾。
  SnapshotImpl* New(SequenceNumber sequence_number) {
    assert(empty() || newest()->sequence_number_ <= sequence_number);

    SnapshotImpl* snapshot = new SnapshotImpl(sequence_number);

// 调试模式，只有一个版本的快照
#if !defined(NDEBUG)
    snapshot->list_ = this;
#endif  // !defined(NDEBUG)
    // 添加到链表的尾部
    snapshot->next_ = &head_;
    snapshot->prev_ = head_.prev_;
    snapshot->prev_->next_ = snapshot;
    snapshot->next_->prev_ = snapshot;
    return snapshot;
  }

  // Removes a SnapshotImpl from this list.
  //
  // The snapshot must have been created by calling New() on this list.
  //
  // The snapshot pointer should not be const, because its memory is
  // deallocated. However, that would force us to change DB::ReleaseSnapshot(),
  // which is in the API, and currently takes a const Snapshot.
  // 从列表中删除SnapshotImpl。快照必须是通过调用此列表中的New()创建的。
  // 快照指针不应该是const，因为它的内存已被释放。
  // 然而，这将迫使我们改变DB::ReleaseSnapshot()，它在API中，目前使用const快照。
  void Delete(const SnapshotImpl* snapshot) {
#if !defined(NDEBUG)
    assert(snapshot->list_ == this);
#endif  // !defined(NDEBUG)
    snapshot->prev_->next_ = snapshot->next_;
    snapshot->next_->prev_ = snapshot->prev_;
    delete snapshot;
  }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl head_; // 链表头（不存放实际的数据）
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_
