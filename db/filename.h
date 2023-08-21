// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#ifndef STORAGE_LEVELDB_DB_FILENAME_H_
#define STORAGE_LEVELDB_DB_FILENAME_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"

namespace leveldb {

class Env;

enum FileType {
  kLogFile,  // dbname/[0-9]+.log
  kDBLockFile, // dbname/LOCK
  kTableFile, // dbname/[0-9]+.(sst|ldb)
  kDescriptorFile, // dbname/MANIFEST-[0-9]+
  kCurrentFile, // dbname/CURRENT 记载当前的manifest文件名
  kTempFile,    // dbname/[0-9]+.dbtmp 在repair数据库时,会重放wal日志,将这些和已有的sst合并写到临时文件中去,成功之后调用rename原子重命名
  kInfoLogFile  // dbname/LOG.old或者dbname/LOG Either the current one, or an old one
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
// 返回以dbname命名的数据库中带有指定编号的日志文件的名称。结果将以dbname作为前缀。
std::string LogFileName(const std::string& dbname, uint64_t number);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
// 返回以dbname命名的数据库中带有指定编号的sstable的名称。结果将以dbname作为前缀。
std::string TableFileName(const std::string& dbname, uint64_t number);

// Return the legacy file name for an sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
// 在以dbname命名的数据库中，返回具有指定编号的sstable的旧文件名。结果将以dbname作为前缀。
std::string SSTTableFileName(const std::string& dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
// 返回以dbname命名的db的描述符文件名和指定的化身号。结果将以dbname作为前缀。
std::string DescriptorFileName(const std::string& dbname, uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
// 返回当前文件的名称。此文件包含当前清单文件的名称。结果将以dbname作为前缀。
std::string CurrentFileName(const std::string& dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
// 返回以dbname命名的db的锁文件的名称。结果将以dbname作为前缀。
std::string LockFileName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
// 返回名为dbname的db所拥有的临时文件的名称。结果将以dbname作为前缀。
std::string TempFileName(const std::string& dbname, uint64_t number);

// Return the name of the info log file for "dbname".
// 返回dbname的info日志文件名。
std::string InfoLogFileName(const std::string& dbname);

// Return the name of the old info log file for "dbname".
// 返回dbname的旧info日志文件名。
std::string OldInfoLogFileName(const std::string& dbname);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
// 如果filename是leveldb文件，将文件类型存储在*type中。
// 文件名中编码的数字存储在*number中。如果文件名已成功解析，则返回true。否则返回false。
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type);

// Make the CURRENT file point to the descriptor file with the
// specified number.
// 使CURRENT文件指向具有指定编号的描述符文件。
Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_FILENAME_H_
