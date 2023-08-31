// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

/*  
线程安全
写操作需要外部同步，很可能是互斥锁。读取需要保证在读取过程中SkipList不会被破坏。
除此之外，读取过程没有任何内部锁定或同步。

不变量:
(1)分配的节点永远不会被删除，直到SkipList被销毁。
  这是由代码保证的，因为我们从不删除任何跳跃表节点。
(2)节点被链接到SkipList后，除next/prev指针外的其他内容都是不可变的。
  只有Insert()会修改列表，并且会小心地初始化节点并使用release-stores将节点发布到一个或多个列表中。
…上一个vs下一个指针排序…
*/

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node; // 声明了一个Node节点

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  // 创建一个新的SkipList对象，它将使用"cmp"来比较键，并使用"*arena"分配内存。
  // 在arena中分配的对象必须在skiplist对象的生存期内保持分配状态。
  explicit SkipList(Comparator cmp, Arena* arena);

  // 禁用拷贝构造
  SkipList(const SkipList&) = delete;
  // 禁用赋值构造函数
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // 向列表中插入键。
  // REQUIRES:当前列表中没有与key比较的值。
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  // 如果列表中存在与key相等的项，则返回true。
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  // 对skiplist的内容进行迭代
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    // 在指定列表上初始化迭代器。
    // 返回的迭代器无效。
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    // 如果迭代器位于有效节点，则返回true。
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    // 返回当前位置的键。
    // REQUIRES：要求指向的位置是合理的
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    // 迭代到下一个
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    // 迭代到前一个
    void Prev();

    // Advance to the first entry with a key >= target
    // 找到第一个键 >= target的表项
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    // 列表中第一个元素的位置。
    // 如果list不为空，迭代器的最终状态为Valid()。
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    // 列表中最后一项的位置。
    // 如果list不为空，迭代器的最终状态为Valid()。
    void SeekToLast();

   private:
    const SkipList* list_; // 跳表的指针
    Node* node_; // 节点的指针
    // Intentionally copyable
  };

private:
  enum { kMaxHeight = 12 };

  // 获取跳表的最大高度
  inline int GetMaxHeight() const {
    // 从max_height_中获取最大高度
    return max_height_.load(std::memory_order_relaxed);
  }

  // 创建一个节点
  Node* NewNode(const Key& key, int height);
  // 生成随机高度
  int RandomHeight();
  // 判断连个key是否相等
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  // 如果key大于存储在"n"中的数据返回true
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  // 返回key处或之后最早出现的节点。
  // 如果没有，返回nullptr。
  //
  // 如果prev非空，则用指向previous的指针填充prev[level]
  // 在[0.. max_height_1]中的每一层的“level”节点。
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  // 返回键值 < key的最新节点。
  // 如果没有这样的节点，返回head_。
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  // 返回列表中的最后一个节点。
  // 返回head_如果列表为空。
  Node* FindLast() const;

  // Immutable after construction
  // 构造后不可变
  Comparator const compare_;
  // 用于分配node的内存空间
  Arena* const arena_;  // Arena used for allocations of nodes

  // 跳表的头节点
  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  // 只能由Insert()修改。由读者快速读取，但陈旧的值是可以的。
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  // 只能由Insert()进行读写操作。
  Random rnd_;
};

// 实际定义的Node节点
// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;

  /*
  `std::memory_order_relaxed`，`std::memory_order_release`，`std::memory_order_acquire` 
  是 C++ 标准库中提供的三个内存屏蔽（memory ordering）操作选项，用于控制原子操作和多线程间的同步，
  避免数据竞争、不一致的结果和不确定的行为。（像原子操作、锁等）

  这三个选项的区别如下：

  1. `std::memory_order_relaxed`：宽松的内存屏蔽操作。使用该选项时，不会引入额外的同步或顺序要求。
  它允许操作的执行和访问在其他线程中的操作和访问之间以任意的顺序进行重排序。
  这是最轻量级、最灵活的内存屏蔽选项，适用于不需要严格同步或顺序要求的场景。

  2. `std::memory_order_acquire`：获取操作的内存屏蔽操作。
  使用该选项时，所有对共享数据的加载操作都要在该操作之后进行，
  并且确保该操作之后的所有操作（包括非原子操作）在该操作之后可见。
  这个选项用于确保获取操作后的所有操作对其他线程可见，但不会强制保证之前的存储操作的顺序。

  3. `std::memory_order_release`：释放操作的内存屏蔽操作。
  使用该选项时，所有对共享数据的修改操作都要在该操作之前完成，
  并且确保该操作之前的所有操作（包括非原子操作）在该操作之前可见。
  这个选项用于确保释放操作前的所有操作对其他线程可见，但不会强制保证之后的加载操作的顺序。

  需要注意的是，`std::memory_order_release` 和 `std::memory_order_acquire` 是成对使用的，
  用于实现获取-释放（acquire-release）语义，确保对共享数据的同步和顺序性。

  除了上述三个选项外，C++ 还提供了其他一些内存屏蔽选项，
  如 `std::memory_order_seq_cst`（顺序一致性内存屏蔽操作），
  它提供了最强的同步和顺序保证，但通常会带来较高的开销。

  在多线程编程中，选择适当的内存屏蔽选项非常重要，以确保正确的同步和顺序性，
  避免数据竞争和不一致的结果。具体选择哪个选项取决于具体的应用场景和需求。
  */

  /* 相对严格的内存屏障操作 */
  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  // 链接的访问器/变量。封装在方法中，以便我们可以根据需要添加适当的屏障。
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    // 使用'acquire load'，这样我们就可以观察到返回节点的完全初始化版本。
    return next_[n].load(std::memory_order_acquire);
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    // 使用'acquire load'，这样我们就可以观察到返回节点的完全初始化版本。
    next_[n].store(x, std::memory_order_release);
  }

  /* 相对宽松的内存屏蔽操作，性能损耗更小 */
  // No-barrier variants that can be safely used in a few locations.
  // 可以在少数位置安全地使用的无屏障变体。
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed); // 使用内存屏蔽操作读取共享变量
  }

  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed); // 使用内存屏蔽操作存储共享变量
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  // 长度等于节点高度的数组。Next_[0]为最低级别链路。
  std::atomic<Node*> next_[1]; // 结构体设计的技巧，通过使用长度为1的数组来模拟动态分配的指针数组，便于扩展
};

// 创建节点的方法
// 由于 Node 是在类模板内部声明的，因此在类外部定义时，
// 需要使用typename SkipList<Key, Comparator>::Node的方式来指定结构体的完整类型。
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  // 能够确保分配的内存对齐
  // 这里sizeof(Node)包含了一个key和一个std::atomic<Node*>的大小，
  // 而sizeof(std::atomic<Node*>) * (height - 1)是（height - 1）个std::atomic<Node*>的大小，
  // 总的加起来就是跳表中的某一个节点高度为height
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  // 使用定位 new 的语法，它将在 node_memory 指向的内存空间上构造一个 Node 对象。
  // 这样做的好处是可以在指定的内存位置上构造对象，而不会再次分配内存。
  return new (node_memory) Node(key);
}

// 生成跳表的迭代器
template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

// 判断跳表的迭代器是否合理，即指定向的节点是否为nullptr
template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

// 获取迭代器指向的key
template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

// 将迭代器指向下一个节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

// 将迭代器指向当前节点的前驱节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  // 不使用显式的"prev"链接，我们只搜索在key之前的最后一个节点。
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) { // 如果前驱节点是跳表的头节点，则返回nullptr
    node_ = nullptr;
  }
}

// 将迭代器指向指定的key的node，如果不存在指定的target，则将指向第一个大于目标target的node
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

// 获取跳表的第一个节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

// 获取跳表的最后一个节点
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 生成一个跳表的随机高度
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  // 以1/kbranches的概率增加高度
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  // 根据随机数生成的高度，计算出最大高度
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// 判断key是否在节点node之后的节点中（node.key < key）
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 寻找第一个大于等于key的节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1; // 获取跳表的最大高度
  while (true) { 
    Node* next = x->Next(level); // 获取level层的链表的下一个节点
    if (KeyIsAfterNode(key, next)) { // 如果要找的key在节点next之后
      // Keep searching in this list
      x = next; // 更新x
    } else { // 不在节点next之后，说明key在x与next之间
      if (prev != nullptr) prev[level] = x; // 记录指定层级的前驱节点
      if (level == 0) {
        return next;
      } else { // 在下一层中查找
        // Switch to next list
        level--;
      }
    }
  }
}

// 找到第一个小于key的节点node
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 找到跳表中最后一个节点
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 跳表默认构造函数
template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  // 初始化跳表的头节点
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

// 跳表的插入操作
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev); // 找到第一个大于等于key的节点node，prev记录node节点的前驱节点信息

  // Our data structure does not allow duplicate insertion
  // 不允许重复key的插入操作
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight(); // 获取随机高度
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_; // 对于超出目前跳表最大高度的部分，使用head_来填充prev
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.

    // 修改max_height_，不需要与并发读取器同步。如果并发读取器观察到max_height_的新值，
    // 那么它看到的要么是head_ (nullptr)中新级别指针的旧值，要么是在下面的循环中设置的新值。
    // 在前一种情况下，由于nullptr对所有键进行排序，因此读取器将立即下降到下一层。在后一种情况下，阅读器将使用新节点。
    max_height_.store(height, std::memory_order_relaxed);
  }

  // 将新加入的节点添加到prev之后
  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

// 判断跳表中是否包含指定的key
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr); // 找到第一个大于等于key的节点node
  if (x != nullptr && Equal(key, x->key)) { // 如果相等则存在
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
