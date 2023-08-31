// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
// 创建一个固定大小容量的新缓存。这种缓存实现使用最近最少使用的回收策略。
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

class LEVELDB_EXPORT Cache {
 public:
  Cache() = default;

  // 禁用拷贝构造和拷贝赋值
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  // 通过调用传递给构造函数的“deleter”函数来销毁所有现有条目。
  // 派生类需要自定义析构函数
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  // 存储在缓存中的项的不透明句柄。
  struct Handle {};

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  // 将一个从key->value的映射插入到缓存中，并将指定的总缓存容量分配给它。返回对应于映射的句柄。
  // 当返回的映射不再需要时，调用者必须调用this->Release(handle) 。
  // 当插入的条目不再需要时，键和值将传递给deleter。
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // 如果缓存没有key的映射，则返回nullptr。否则返回对应于映射的句柄。
  // 当返回的映射不再需要时，调用者必须调用this->Release(handle) 。
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  // 释放先前Lookup()返回的映射。句柄必须还没有被释放。require:句柄必须由*this上的方法返回。
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  // 返回封装在成功Lookup()返回的句柄中的值。句柄必须还没有被释放。require:句柄必须由*this上的方法返回。
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  // 如果缓存包含键项，则擦除它。注意，底层条目将一直保留，直到所有现有的句柄都被释放。
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  // 返回一个新的数字id。可以由共享同一缓存的多个客户端使用，以对键空间进行分区。
  // 通常，客户机将在启动时分配一个新id，并将该id添加到其缓存键。
  virtual uint64_t NewId() = 0;

  // Remove all cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // leveldb may change Prune() to a pure abstract method.
  // 删除所有未活跃使用的缓存项。内存受限的应用程序可能希望调用此方法来减少内存使用。
  // Prune()的默认实现不做任何事情。强烈建议子类覆盖默认实现。
  // leveldb的未来版本可能会将Prune()更改为纯抽象方法。
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  // 返回缓存中存储的所有元素的总估计值。
  virtual size_t TotalCharge() const = 0;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
