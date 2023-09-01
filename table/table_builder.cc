// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h" // #include "include/leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

// 这里之所以要特意用一个结构体来存储变量而不直接在类中定义变量
// 是因为table_builder.hh是供用户使用的
// leveldb不希望底层的参数被用户访问或者修改
// 此外也是为了把参数和接口解耦，便于升级
// https://stackoverflow.com/questions/33427916/why-table-and-tablebuilder-in-leveldb-use-struct-rep
struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) { // 刚刚开始时，不向index block写入数据
    index_block_options.block_restart_interval = 1;
  }

  Options options;  // SSTable的一些基本信息
  Options index_block_options; // index_block的一些基本信息
  WritableFile* file; // SSTable生成后的文件
  uint64_t offset; // DataBlock在SSTable中的偏移量，用于生成Index BlockHandle
  Status status; // 状态码
  BlockBuilder data_block; // 生成SSTable中的数据区域
  BlockBuilder index_block; // 生成SSTable中的数据索引区域
  std::string last_key; // 上一次添加的key
  int64_t num_entries; // 总的Entry的个数
  bool closed;  // Either Finish() or Abandon() has been called. 如果closed是true，则要么调用Finish()，要么调用Abadon()
  FilterBlockBuilder* filter_block; // 生成SSTable中的元数据区域

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  // 在看到下一个数据块的第一个键之前，我们不会发出一个块的索引项。这允许我们在索引块中使用更短的键。
  // 例如，考虑一个键之间的块边界"the quick brown fox" 和 "the who"。我们可以使用"the r"作为索引块条目的键，
  // 因为它是>=第一个块中的所有条目和<后续块中的所有条目。
  // 不变式:只有当数据块为空时，r->pending_index_entry才为真。
  bool pending_index_entry; 
  // 记录需要生成数据索引的数据块在SSTable中的偏移量和大小
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output; // 存放压缩后的Block数据
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) { // 如果filter_block不为空的话，则需要创建一个块
    rep_->filter_block->StartBlock(0); 
  }
}

// 销毁
TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish() 捕获调用者忘记调用Finish()的错误
  delete rep_->filter_block; // 销毁filter_block(单独创建的)
  delete rep_; // 销毁rep_
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  // 注意:如果在Options中添加了更多的字段，请更新此函数以捕获在构建表的过程中不允许更改的更改。
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  // 请注意，任何活动的BlockBuilders都指向rep_->options，因此会自动选择更新的选项。
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;

  // 如果num_entries大于0，判断要添加的key是否满足大于上次添加的key，确保key是按照从小到大的顺序添加的
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 如果之前持久化了一个datablock，则准备向index block插入一条指向它的kv对
  if (r->pending_index_entry) {
    // 上一次生成了一个DataBlock，并将其刷入到缓存区中，那么在插入新的key到下一个DataBlock之前，data_block应该为空的
    assert(r->data_block.empty()); 

    // 找一个介于last_key和key之间的，最短的key
    // 比如说zzzzb + zzc = zzd
    r->options.comparator->FindShortestSeparator(&r->last_key, key); 

    // 把datablock的handle编码为字符串
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);

    // 写入index block
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false; // 将其更改为false，当前的index
  }

  // 判断filter_block是否为空，如果不为空，则将当前的key添加到filter_block中
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 更新last_key，由于不用前缀压缩，所以直接把key复制进来
  r->last_key.assign(key.data(), key.size());
  // 计数已经添加了多少条Entry
  r->num_entries++;
  // 写入datablock
  r->data_block.Add(key, value);

  // 估计datablock的大小
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) { //如果 Block的大小达到了阈值（4KB），则此Block已经满了，需要生成一个DataBlock
    Flush(); // 调用Flush函数，将生成的DataBlock写入磁盘文件中
  }
}

// 将DataBlock写入SSTable文件中，并刷新到磁盘
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed); // 必须是还没有关闭
  if (!ok()) return;
  if (r->data_block.empty()) return; // 如果data_block是空的，则直接返回
  assert(!r->pending_index_entry); // pending_index_entry必须为false，即上一个DataBlock的index BlockHandle已经添加到了Index Block中
  
  // 持久化到磁盘，并生成BlockHandle到pending_handle
  // 先用snappy压缩，后进行crc编码，最终持久化到磁盘
  WriteBlock(&r->data_block, &r->pending_handle);

  // 如果没有错误，将其落盘
  if (ok()) {
    r->pending_index_entry = true; // 待添加到的index BlockHandle
    r->status = r->file->Flush(); // 刷盘
  }
  // 如果filter_block不为空，则需要将
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset); // 这是DataBlock在SSTable中的偏移量
  }
}

// 真正持久化经过压缩处理的block数据
// 持久化前进行crc编码，方便校验
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish(); // 完成Block纯数据部分内容的构建

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  // 根据压缩类型进行数据压缩
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        // @todo 这里怎么理解将std::string类型的数据赋值给Slice类类型的数据的，Slice也没有定义operator=(std::string&)的拷贝赋值函数
        block_contents = *compressed;  
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }

    case kZstdCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(),
                              raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Zstd not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  
  // 将处理后的数据写入到
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

// 生成完成的Block，包含压缩类型和CRC校验部分的内容
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;

  // 更新handle，赋值block在文件中的头偏移量和长度
  // 当前文件的尾偏移量就是要持久化block的开头偏移量
  handle->set_offset(r->offset);
  // size不包括type和crc值
  // 这样也可以读取到，也type和crc的长度都是固定的
  handle->set_size(block_contents.size());
  
  // 向文件末尾追加这个block数据
  r->status = r->file->Append(block_contents);

  // 如果写入成功，就添加压缩类型和crc检验码
  if (r->status.ok()) {
    // kBlockTrailerSize = type + crc值
    // type(8bit, 1Byte) + crc(uint32_t, 32bit, 4Byte) = 5Byte
    char trailer[kBlockTrailerSize];
    // 赋值入压缩的类型
    trailer[0] = type;

    // 计算block数据的crc值
    // 底层调用了下面的crc32c::Extend
    // Extend(0, data, n)
    // 计算data[0, n - 1]的crc值
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());

    // 计算trailer[0, 0]的crc值
    // trailer[0, 0]就是type
    // 使用block的crc值作为种子
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type

    // 计算crc的掩码，写入到trailer的后4个Byte
    // 注释说只计算crc值会有问题
    // 这里有更详细的讨论: https://stackoverflow.com/questions/61639618/why-leveldb-and-rocksdb-need-a-masked-crc32
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));

    // 将type和crc校验码写入文件
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 更新文件偏移量（作为下一个DataBlock在SSTable中开始位置的偏移量）
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

// 返回状态码
Status TableBuilder::status() const { return rep_->status; }

// 按照SSTable的格式，分别写入数据块、元数据块、元数据索引块、数据块索引，最后写入Footer
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush(); // 将DataBlock写入SSTable文件并且刷新到磁盘
  assert(!r->closed); // 
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 写入filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 写入metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      // 元数据索引块的key为"filter."加上配置的过滤器名称，默认为
      // filter.leveldb.BuiltinBloomFilter2
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      // 元数据索引块的值也为一个BlockHandle，该BlockHandle包括一个指向元数据块的偏移量以及元数据块的大小
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 写入index block
  if (ok()) {
    if (r->pending_index_entry) { // 判断是否还有需要待添加的index entry到index block中
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 写入Footer到文件尾部
  if (ok()) {
    Footer footer;
    // 将元数据索引区域的BlockHandle值设置到尾部
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);

    // 给footer加入padding和magic number
    // 把它编码为字符串
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);

    // 写入footer数据
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size(); // 加上这个Footer的大小
    }
  }
  return r->status;
}

// 中止
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

// 返回所有DataBlock存放的总的Entry数量
uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

// 返回总的文件的大小
uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
