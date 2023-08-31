// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
// BlockHandle是一个指针，指向DataBlock或MetaBlock的文件的范围。
class BlockHandle {
 public:
  // Maximum encoding length of a BlockHandle
  // BlockHandle的最大编码长度（offset_和size_都采用varint32编码，每一个最多10字节）
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle();

  // The offset of the block in the file.
  // block在SSTable中的偏移量
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  // block的实际大小（不包含压缩类型和CRC检验的5字节内容的，它们是额外添加的）
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  // 对offset_和size_编码解码
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  uint64_t offset_; // 在SSTable中偏移量
  uint64_t size_; // Block实际存储数据的大小（不包含压缩类型和CRC检验的5字节内容的，它们是额外添加的）
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
// Footer封装了存储在每个表文件尾部的固定信息。
class Footer {
 public:
  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  // Footer的编码长度。请注意，Footer的序列化将始终占用这么多字节。它由两个BlockHandle和一个MagicNumber组成。
  // 最大为48字节内容
  enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

  Footer() = default;

  // The block handle for the metaindex block of the table
  // MetaIndexBlock Handle的内容
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  // IndexBlock Handle的内容
  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  // 对metaindex_handle_和index_handle_的编码解码
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

 private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
// kTableMagicNumber是固定的值，通过它来判断是否为一个SSTable文件（主要），以及文件是否存在损坏的情况（次要，主要是通过CRC检验来判断的）
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
// 压缩类型和CRC检验一共占据5字节内容
static const size_t kBlockTrailerSize = 5;

// Block实际存放的数据内容
struct BlockContents {
  Slice data;           // Actual contents of data 真实的数据
  bool cachable;        // True iff data can be cached 数据是否可以被缓存（或者是在mmap内存映射上的）
  bool heap_allocated;  // True iff caller should delete[] data.data() 是否是在堆上分配的（或者是在mmap内存映射上的）
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
// 从文件中读取由BlockHandle标识的块。失败时返回非ok。成功时，填充*结果并返回OK。
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result);

// Implementation details follow.  Clients should ignore,

// BlockHandle的初始化构造函数
inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_
