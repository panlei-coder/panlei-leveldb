// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }
//  Helper类，它在构造时锁定互斥锁，并在调用MutexLock对象的析构函数时解锁互斥锁，一般离开作用域会自动实现解锁操作。

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;

 private:
  port::Mutex* const mu_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
