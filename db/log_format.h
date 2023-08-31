// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

/*
多个块(每个块是32KB):
*******|*******|*******|*******|*******|*******
块有四种情况:(length记录的是value的长度)
第一种情况:
****************|***************|**************|**********************************************
<----crc-4B---->|<--length-2B-->|<-recordType->|<----------------value刚好填满---------------->
第二种情况:(如果剩余6个字节的空间,无法放下一个record的header内容,然后用"\x00\x00\x00\x00\x00\x00"填充)
****************|***************|**************|**********************************************
<----crc-4B---->|<--length-2B-->|<-recordType->|<----剩余空间小于7B,此时全部填满,开启新的block---->
第三种情况:
****************|***************|**************|**********************************************
<----crc-4B---->|<--length-2B-->|<-recordType->|<----------------value可以装下多个------------->
第四种情况:
****************|***************|**************|**********************************************
<----crc-4B---->|<--length-2B-->|<-recordType->|<-------------记录value的中间部分-------------->
*/


enum RecordType {
  // Zero is reserved for preallocated files
  // 0为预分配文件保留
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768; // 32KB

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
