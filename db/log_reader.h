// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader { 
 public:
  // Interface for reporting errors.
  // 报告错误的接口
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "bytes" is the approximate number
    // of bytes dropped due to the corruption.
    // 发现了一些损坏。bytes是由于损坏而丢失的字节数。
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-null, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  /*
  创建一个读取器，从*file返回日志记录。"*file"必须在Reader使用时保持活动状态。
  如果reporter为非空，则每当由于检测到损坏而删除某些数据时，都会通知它。
  *Reader在使用时，reporter必须保持在线状态。如果checksum为true，则验证checksum是否可用。
  Reader将从位于文件中物理位置&gt;=初始偏移量的第一条记录开始读取。
  */
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  // 将下一条记录读入*record。如果读取成功返回true，如果到达输入结束返回false。
  // 可以使用*scratch作为临时存储。在*record中填写的内容将只在该阅读器上的下一个突变操作或下一个突变之前有效。
  bool ReadRecord(Slice* record, std::string* scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  // 返回由ReadRecord返回的最后一条记录的物理偏移量。在第一次调用ReadRecord之前未定义。
  uint64_t LastRecordOffset();

 private:
  // Extend record types with the following special values
  // 使用以下特殊值扩展记录类型
  enum {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    /*
    当我们发现一个无效的物理记录时返回。目前有三种情况发生:
    *record有一个无效的CRC (ReadPhysicalRecord报告一个drop) 
    *record是一个0长度的记录(没有drop被报告)
    *record(低于构造器初始偏移量(没有drop被报告)
    */
    kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  // 跳过所有完全在初始偏移量之前的块。成功时返回true。处理报告。
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  // 返回类型，或上述特殊值之一
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  // 报告将字节丢给报告程序。必须更新缓冲区，以便在调用之前删除丢失的字节。
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  SequentialFile* const file_; // 文件顺序读
  Reporter* const reporter_; // 用于报告错误
  bool const checksum_; // 标记是否校验
  char* const backing_store_; // 实际存放读取数据的变量,buffer_只是引用这段内存
  Slice buffer_; // 缓冲区
  // Last Read()通过returning < kBlockSize指示EOF
  bool eof_;  // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  // ReadRecord返回的最后一条记录的偏移量。
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  // 缓冲区结束后第一个位置的偏移量。
  uint64_t end_of_buffer_offset_; // initial_offset_所在块的开头位置(所有读取的内容的偏移量总和)

  // Offset at which to start looking for the first record to return
  // 开始查找要返回的第一条记录的偏移量
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  // 如果我们在seek(initial_offset_ > 0)之后重新同步，则为True。
  // 特别是，在这种模式下，kMiddleType和kLastType记录的运行可以被静默跳过
  bool resyncing_;
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
