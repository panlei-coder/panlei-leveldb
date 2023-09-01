// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {
// 将SSTable文件和SSTable存放到一个结构体中
// TableCache中的Value结构
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

// 删除TableCache一条KV记录，
// 1、删除ldb在内存中的数据；
// 2、关闭ldb文件句柄。
static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

// 这里是解引用TableCache中的一条KV记录
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

// 构造一个TableCache，则构造过程中
// 就根据传入的entries（KV个数）来创建TableCache。
TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

// 析构
TableCache::~TableCache() { delete cache_; }

// 查找ldb文件对应的Cache记录。这里要说明下，当前版本是1.22版本。
// 落地的存储文件后缀都是.ldb，旧的落地文件后缀是.sst。
// file_number：就是ldb文件名
// file_size：ldb文件大小。
// handle：要返回的ldb对应的Cache实体
// 查找流程是：
// 1、file_number就是key，先去TableCache中查找，若找到则直接返回。
// 2、TableCache中未找到，则需要打开此文件，先以后缀.ldb格式打开。
// 3、若打开失败，尝试用文件后缀.sst格式打开。
// 4、打开文件成功之后要创建Table实体，用于管理ldb文件内容。
// 5、将打开的文件插入到TableCache中。

// 查找指定的SSTable，优先去缓存中查找，找不到则去磁盘中读取，读完之后再插入到缓存中
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;

  // 在缓存中根据file_number去缓存中查找对应的SSTable
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key); 

  // 如果缓存中没有指定的SSTable文件，需要根据文件名，从磁盘中读取
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number); // ldb
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file); // 获取随机可读的文件句柄
    if (!s.ok()) { // 如果创建的ldb的文件句柄失败，则创建sst的文件句柄
      std::string old_fname = SSTTableFileName(dbname_, file_number); // 创建一个SSTable文件名称，sst
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {  // 读取SSTable文件
        s = Status::OK();
      }
    }

    // 打开SSTable文件并且生成一个Table实例保存到table变量中
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      // 打开失败的处理
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else { // 实例化一个TableAndFile结构，该结构中的file变量保存一个RandomAccessFile实例，table变量保存一个Table实例
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 以文件序列号作为key，TableAndFile实例作为value，插入到缓存中
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

// 创建访问ldb文件的迭代器。
// 1、先根据文件名找到ldb/sst文件结构；
// 2、根据找到的ldb结构，对table结构创建一个二层指针迭代器；
// 3、注册迭代器销毁时的操作函数。
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  // 根据文件名找到对应的SSTable
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 根据找到的SSTable创建二层迭代器（index block迭代器和data block迭代器）
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle); // 注册销毁操作
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

// 此方法就是查找ldb/sst文件中是否存在key，若存在则执行handle_result函数。
// InternalGet()流程如下：
// 1、先去ldb文件的index_block中查找key对应的block offset；
// 2、根据block offset去Filter Block（若开启的话）中去查找；
// 3、若确定存在，则去实际的DataBlock中去读取，同时执行handle_result方法。
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle); // 找到SSTable文件在缓存中的位置
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table; // 得到SSTable
    s = t->InternalGet(options, k, arg, handle_result); // 去SSTable中查找对应的key，如果找到了则使用handle_result函数处理
    cache_->Release(handle); // 使用完成之后减少引用次数
  }
  return s;
}

// 删除ldb/sst文件在tableCache中缓存
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
