// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/status.h"

#include <cstdio>

#include "port/port.h"

namespace leveldb {

/*
Status用于记录levelDB中状态信息，保存错误码和对应的字符串错误信息（不过不支持自定义），其组成是：
[0,4)前4个字节存放消息长度（指定是具体存放消息内容的字节数），[4,5)第5个字节存在整型code，后面的字节存放具体的消息内容
*/

const char* Status::CopyState(const char* state) {
  // 使用std::memcpy()将state拷贝到result中，并且将size赋值给size
  uint32_t size;
  std::memcpy(&size, state, sizeof(size));
  // 创建一个新的char数组，大小为size+5
  char* result = new char[size + 5];
  // 使用std::memcpy()将state拷贝到result中，并且将size赋值给result
  std::memcpy(result, state, size + 5);
  // 返回result
  return result;
}

// Status构造
Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  // 检查状态码是否为kOk
  assert(code!= kOk);
  // 计算长度
  const uint32_t len1 = static_cast<uint32_t>(msg.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0); // msg和msg2会进行拼接
  // 分配结果
  char* result = new char[size + 5];
  // 设置结果大小
  std::memcpy(result, &size, sizeof(size));
  // 设置状态码
  result[4] = static_cast<char>(code);
  // 如果有消息，则设置消息
  std::memcpy(result + 5, msg.data(), len1);
  if (len2) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    // 如果有消息2，则设置消息2
    std::memcpy(result + 7 + len1, msg2.data(), len2);
  }
  // 设置状态码
  state_ = result;
}

// 转换成字符串
std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        std::snprintf(tmp, sizeof(tmp),
                      "Unknown code(%d): ", static_cast<int>(code()));
        type = tmp;
        break;
    }
    // 将state_中存放具体消息的内容append到result上
    std::string result(type);
    uint32_t length;
    std::memcpy(&length, state_, sizeof(length));
    result.append(state_ + 5, length);
    return result;
  }
}

}  // namespace leveldb
