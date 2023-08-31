// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options; // Table的相关参数信息
  Status status; // Table相关的状态信息
  RandomAccessFile* file; // Table所持有的文件（可随机访问的文件）
  uint64_t cache_id; // Table本身对应的缓存id
  FilterBlockReader* filter; // filter block块的读取
  const char* filter_data; // 保存对应的filter数据

  // 解析保存meta_index_handler
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block; // index block数据
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  // 判断SSTable文件的大小是否满足条件
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  // 读取Footer
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  // 对Footer进行解析
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  // 读取index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true; // 做crc检验
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  // index block读取成功
  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // 我们已经成功地读取了Footer和数据索引块:准备好处理请求了。
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer); // 读取元数据
  }

  return s;
}

// 读取元数据meta index block
void Table::ReadMeta(const Footer& footer) {
  // 判断是否建立了过滤器
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  // 跳过这个Footer。Metaindex handle()的大小表明它是一个空块。
  // 读取meta index Block
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true; // 做CRC检验
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);
  
  // 创建meta index block上的迭代器
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  // 读取指定Filter的BlockHandle
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

// 读取一个meta Block
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle; 
  if (!filter_handle.DecodeFrom(&v).ok()) { // 解析出Filter的BlockHandle
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  // 如果我们开始要求在Table::Open中进行校验和验证，我们可能需要统一ReadBlock()。
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true; // 做CRC检验
  }
  // 读取meta block
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) { // 如果读取的block是在堆上分配的，那么
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

// @todo 删除Block（堆上的）
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

// @todo 删除缓存上的Block
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

// @todo 释放缓存上的Block
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 将一个索引迭代器的值(例如，一个编码的BlockHandle)转换为对应块内容的迭代器。
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  // arg参数为一个Table结构，options为读取时的参数结构，index_value为通过第一层
  // 迭代器读取到的值。该值为一个BlockHandle，BlockHandle中的偏移量指向要查找的块在SSTable中的偏移量，
  // BlockHandle中的大小表明要查找的块的大小

  // 将arg变量转换为一个Table结构
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  // 将index_value解码到BlockHandle类型的变量中index block
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  // 我们有意在索引值中允许额外的内容，以便将来可以添加更多的功能。

  // @todo 没太理解这段内容想要表达的意思 ？？？ 
  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) { // 如果缓存块不为空，则使用指定的缓存块
      char cache_key_buffer[16];
      // 获取对应的缓存块
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) { // 缓存块handle不为空，则直接获取
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {  // 为空则自己创建一个缓存块，并添加到LRU_Cache中
        // 在SSTable文件中，通过BlockHandle变量handle变量所指向的偏移量以及大小读取一个块的内容到contents变量
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) { // 如果index block的内容可以被缓存到一个内存块上（不是通过mmap内存映射）
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else { // 不能通过缓存块，则直接创建一个Block，不添加到LRU_Cache中
      // 在SSTable文件中，通过BlockHandle变量handle变量所指向的偏移量以及大小读取一个块的内容到contents变量
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  // 基于这个Block创建迭代器
  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) { // 在堆上分配的空间，所以需要注册销毁函数
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else { // 注册释放缓存上的Block函数
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else { // 如果Block为空，则直接创建NewErrorIterator
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 通过Table类来生成一个迭代器
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      // 第一层迭代器，为一个块迭代器
      rep_->index_block->NewIterator(rep_->options.comparator),
      // 第二层迭代器，也是一个块迭代器
      &Table::BlockReader, const_cast<Table*>(this), options);
}

// 调用(*handle result)(arg，…)，并在调用Seek(key)后找到条目。如果筛选策略说该键不存在，可能不会进行这样的调用。
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  // 创建index block的迭代器
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k); // 定位到key所在的位置
  if (iiter->Valid()) { // 如果迭代器合理
    Slice handle_value = iiter->value(); // 用于解析出key所在的DataBlock的BlockHandle
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // 如果有过滤器，则通过过滤器判断key是否存在
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) { // key不存在
      // Not found
    } else { // key存在
      // 创建DataBlock的迭代器  
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k); // 找到key
      if (block_iter->Valid()) { // 对找到的key做处理
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

// 估计某个key大致在SSTable中的大致偏移量是多少
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  // 创建迭代器并定位到key
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) { // 如果迭代器指向的位置合理
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input); // 解析出key所在的DataBlock的BlockHandle
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      // 奇怪:我们无法解码索引块中的块句柄。我们只返回元索引块的偏移量，在这种情况下，它接近于整个文件的大小。
      result = rep_->metaindex_handle.offset(); // 返回的是DataBlock的末尾
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    // key超过了文件中的最后一个key。通过返回metaindex块的偏移量(在文件末尾附近)来近似偏移量。
    result = rep_->metaindex_handle.offset(); // 返回的是DataBlock的末尾
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
