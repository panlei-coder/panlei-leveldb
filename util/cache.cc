// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

// 匿名空间,匿名空间中定义的内容只能够在当前的文件中使用,
// 无法在外部调用,主要目的是在当前文件中提供内部实现的封装和隐藏。
// 如果需要多个文件中共享功能,使用具名命名空间,并将相关的代码文件
// 包含到需要使用功能的文件中。这样可以更好地组织和管理代码。
// 文件的末尾定义了NewLRUCache,后续可以通过动态绑定调用匿名函数中定义的方法
namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.

/*

LRU缓存实现

缓存项有一个in缓存布尔值，表示缓存中是否有对该项的引用。在不将条目传递给其删除器的情况下，
该值变为false的唯一方法是通过Erase()、插入具有重复键的元素时通过Insert()或销毁缓存。

缓存在缓存中保留两个项的链表。缓存中的所有项都在其中一个列表中，而不是同时在两个列表中。
仍然被客户端引用但从缓存中删除的项不在这两个列表中。列表如下:
(1)- in-use:包含客户端当前引用的项，没有特定的顺序。(此列表用于不变检查。如果我们删除了check，
    那么原本在这个列表上的元素可能会被保留为断开连接的单例列表。)
(2)LRU:包含当前未被客户端引用的项，在LRU顺序中，当它们检测到缓存中的元素获取或丢失其唯一的外部引用时，
    元素通过Ref()和Unref()方法在这些列表之间移动。
(3)表项是可变长度的堆分配结构。条目保存在按访问时间排序的双向循环链表中。

*/
// 链表中的一个单独的节点
struct LRUHandle {
  void* value; // 当前节点存放的具体值
  void (*deleter)(const Slice&, void* value); // 删除的函数指针
  LRUHandle* next_hash; // 用于HashTable(HandleTable)冲突时,下一个节点(产生冲突时的链表,将所有冲突的节点统一放到一个链表中,作为一个哈希桶)
  LRUHandle* next; // 代表LRU中双向链表中下一个节点
  LRUHandle* prev; // 代表LRU中双向链表中上一个节点
  size_t charge;  // TODO(opt): Only allow uint32_t? 记录当前value所占用的内存大小,用于后面超出容量后需要进行LRU
  size_t key_length; // 数据key的长度
  bool in_cache;     // Whether entry is in the cache. 表示是否在缓存中
  uint32_t refs;     // References, including cache reference, if present. 引用计数,因为当前节点可能会被多个组件使用,不能简单的删除
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons 记录当前key的hash值,避免多次求
  char key_data[1];  // Beginning of key key的数据域(柔性数组)

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    // next只有在LRU句柄是空列表的列表头时才等于this。列表头从来没有有意义的键。
    assert(next != this);
    // 将key_data包装成Slice
    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
/*
我们提供了自己的简单哈希表，因为它消除了一大堆移植操作，而且在我们测试过的一些编译器/运行时组合中，
它比一些内置哈希表实现要快。例如，readrandom的速度比g++ 4.4.3的内置哈希表提高了约5%。
*/
// HashTable
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  // 查找某个key
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  // 插入一个节点到list_中(支持采用替换方式写入相同的key数据,新key的value将会替换老key的value)
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    // 如果存在相同节点，替换该节点
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    // 将节点替换，要么是老节点位置，要么是最后一个节点位置
    *ptr = h;
    // TODO ??? 为什么对于old非空时,就不更新elems_的值呢?如果不更新,那么elems_最多也只会出现elems_ <= length_的情况出现吧(见113和115行)
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) { // 元素个数超过hash表的长度时需要进行扩容
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        // 由于每个缓存条目相当大，我们的目标是使平均链表长度较小(<= 1)。
        Resize();
      }
    }
    return old; // 不应该返回*ptr么? 这里就是为了返回被替换的旧节点
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  // 该表由一个桶数组组成，其中每个桶是一个链表，其中包含散列到桶中的缓存条目。
  uint32_t length_; // 哈希表的总长度
  uint32_t elems_; // 当前哈希表的实际元素个数
  LRUHandle** list_; // 实际存储的数据,底层结构(LRUHandle*的数组)

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // 返回一个指向与key/hash匹配的缓存条目的slot指针。如果没有这样的缓存项，则返回一个指向相应链表末尾槽的指针。
  // (用来定位元素所在的节点)
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    // 在冲突链表中查找
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }
  
  // 返回一个指向LRU链表的指针, 该链表包含所有具有相同散列值的元素
  // 重新分配内存
  void Resize() {
    uint32_t new_length = 4; // 当元素超过4时,才会按照2的倍数对齐分配内存
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  // 与构造函数分离，以便调用者可以轻松地创建LRUCache数组
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  size_t capacity_; // 缓存节点的容量,超过了需要将多余的移除

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_); // 用来计算使用的数量,前面的LRUHandle.charge参数累加

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // LRU列表的假头。lru.prev是最新条目，lru.next是最老的条目。条目有refs==1 and in_cache==true。
  LRUHandle lru_ GUARDED_BY(mutex_); // 缓存链表

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // in-use的list的头节点
  LRUHandle in_use_ GUARDED_BY(mutex_); // 正在被引用的链表

  HandleTable table_ GUARDED_BY(mutex_);  // hash表
};

// 初始化双向链表指向自己
LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  // 强校验是否还有元素没有Release掉，还在使用
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    // 强校验并减少引用计数,触发其析构器
    Unref(e);
    e = next;
  }
}

// 增加引用计数
// 引用计数默认1，变为 >1 时，从 lru_ 链表移除，加入到 in_use_ 链表
// 表示正在被使用
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    // 不再使用的;移动到lru列表。
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// 链表的操作
// 双向链表移除
// 修改前一个节点的下一个
// 修改后一个节点的前一个
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

// 添加函数，采用 append 方式，即插入头节点之前
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

// 查找，直接从 hashtable 里面查找即可
// 找到后增加引用计数，代表被外部使用，从而放入 in_use_ 链表
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_); // 离开作用域后会自动析构
  LRUHandle* e = table_.Lookup(key, hash); // 判断当前的key是否存在于缓存区中
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

// 使用完成显示调用 Release 减少引用计数
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

// 插入节点，需要传入key和hash和value和charge和析构器
Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size())); // 因为LRUHandle的key_data是柔性数组
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  // 容量如果 <0 证明不需要缓存
  // 插入时，默认引用计数为2，因为需要将该节点返回给上层使用，放在 in_use_ 链表
  // 插入hashTable时遇见重复的key，将其删除
  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }

  // 大于容量，取lru_的下一个，即最老未使用节点进行删除
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
// 如果e != nullptr，完成从缓存中删除*e;它已经从哈希表中删除了。返回是否e != nullptr。
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    // 引用计数归0，如果外部还在使用它，外部应该会显示将其归0
    Unref(e);
  }
  return e != nullptr;
}

// 删除节点，参数接受为 key 和 hash 值
// 先找到节点，进行删除
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

// 清理节点，遍历 lru_ 链表进行清理即可
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits; // 16

class ShardedLRUCache : public Cache {
 private:
  // 读写互斥，当 HandleTable 发生扩容时会阻塞读操作，为了使锁粒度减小，采用 ShardedLRUCache 封装多个 LRUCache 来分散锁的粒度。
  LRUCache shard_[kNumShards]; // 一共有16个分片(LRUCache)
  port::Mutex id_mutex_; 
  uint64_t last_id_;// 提供递增序号 last_id_ 用于表示唯一 cache 方便共享使用

  // 计算hash函数
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  // 将 hash 函数控制到 4 bit 即 < 16 放入对应 LRUCache 中
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  // 容量平摊到每一个LRUcache
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards; // 向上取整
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }

  ~ShardedLRUCache() override {}

  // 插入，先计算在哪个LRUCache，插入即可
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

  // 查找，先计算在哪个LRUCache，查找即可
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  // 释放引用，同理计算在哪个LRUCache
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }

  // 删除，同上
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  // 返回节点值
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  // 递增序号 last_id_
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }

  // 都是对 LRUCache 的封装
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }

  // 统计缓存中所有键值对的value所占用的内存大小
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

// 匿名空间暴露给外部空间使用创建ShardedLRUCache的接口
Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
