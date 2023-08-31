// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

/*
优先使用栈对象,不够再去使用堆对象,减少堆内存的申请和释放过程
*/

class PosixLogger final : public Logger {
 public:
  // Creates a logger that writes to the given file.
  //
  // The PosixLogger instance takes ownership of the file handle.
  // 创建写入给定文件的日志记录器。PosixLogger实例获取文件句柄的所有权。
  explicit PosixLogger(std::FILE* fp) : fp_(fp) { assert(fp != nullptr); }

  ~PosixLogger() override { std::fclose(fp_); }

  void Logv(const char* format, std::va_list arguments) override {
    // Record the time as close to the Logv() call as possible.
    // 尽可能在接近Logv()调用时记录时间。
    struct ::timeval now_timeval;
    ::gettimeofday(&now_timeval, nullptr);
    const std::time_t now_seconds = now_timeval.tv_sec;
    struct std::tm now_components;
    ::localtime_r(&now_seconds, &now_components);

    // Record the thread ID.
    // 记录线程ID
    constexpr const int kMaxThreadIdSize = 32; // 最大线程ID的size
    std::ostringstream thread_stream; // 将输出暂存到该变量中
    thread_stream << std::this_thread::get_id();
    std::string thread_id = thread_stream.str();
    if (thread_id.size() > kMaxThreadIdSize) {
      thread_id.resize(kMaxThreadIdSize);
    }

    // We first attempt to print into a stack-allocated buffer. If this attempt
    // fails, we make a second attempt with a dynamically allocated buffer.
    // 我们首先尝试打印到堆栈分配的缓冲区中。如果此尝试失败，则使用动态分配的缓冲区进行第二次尝试。
    constexpr const int kStackBufferSize = 512; // 栈的空间大小
    char stack_buffer[kStackBufferSize];
    static_assert(sizeof(stack_buffer) == static_cast<size_t>(kStackBufferSize),
                  "sizeof(char) is expected to be 1 in C++");

    int dynamic_buffer_size = 0;  // Computed in the first iteration.在第一次迭代中计算
    for (int iteration = 0; iteration < 2; ++iteration) { // 第一次迭代使用栈空间
      const int buffer_size =
          (iteration == 0) ? kStackBufferSize : dynamic_buffer_size;
      char* const buffer =
          (iteration == 0) ? stack_buffer : new char[dynamic_buffer_size];

      // Print the header into the buffer.
      // 将标题打印到缓冲区中(将当前时间和线程ID格式化为一个字符串，并将其存储在一个缓冲区中)
      int buffer_offset = std::snprintf(
          buffer, buffer_size, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
          now_components.tm_year + 1900, now_components.tm_mon + 1,
          now_components.tm_mday, now_components.tm_hour, now_components.tm_min,
          now_components.tm_sec, static_cast<int>(now_timeval.tv_usec),
          thread_id.c_str()); // buffer_offset为格式化之后的字符串的长度

      // The header can be at most 28 characters (10 date + 15 time +
      // 3 delimiters) plus the thread ID, which should fit comfortably into the
      // static buffer.
      // 标题最多可以是28个字符(10个日期+ 15个时间+ 3个分隔符)加上线程ID，这应该适合静态缓冲区(栈)。
      assert(buffer_offset <= 28 + kMaxThreadIdSize);
      static_assert(28 + kMaxThreadIdSize < kStackBufferSize,
                    "stack-allocated buffer may not fit the message header");
      assert(buffer_offset < buffer_size);

      // Print the message into the buffer.
      // 将消息打印到缓冲区中
      std::va_list arguments_copy; // 存储可变参数的副本
      va_copy(arguments_copy, arguments);
      buffer_offset +=
          std::vsnprintf(buffer + buffer_offset, buffer_size - buffer_offset,
                         format, arguments_copy); // 对于可变参数进行格式化输出的函数
      va_end(arguments_copy); // 释放arguments_copy的相关资源

      // The code below may append a newline at the end of the buffer, which
      // requires an extra character.
      // 下面的代码可能会在缓冲区的末尾附加一个换行符，这需要一个额外的字符。
      // @todo 为什么等于不可以 ??? 而且这里也没有要额外添加一个空结束符,如果需要则栈中已经添加了 ???
      if (buffer_offset >= buffer_size - 1) { // 说明栈空间太小,无法存放,进行第二次循环,使用堆内存
        // The message did not fit into the buffer.
        if (iteration == 0) {
          // Re-run the loop and use a dynamically-allocated buffer. The buffer
          // will be large enough for the log message, an extra newline and a
          // null terminator.
          // 重新运行循环并使用动态分配的缓冲区。缓冲区将足够大，可以容纳日志消息、一个额外的换行符和一个空结束符。
          dynamic_buffer_size = buffer_offset + 2;
          continue;
        }

        // The dynamically-allocated buffer was incorrectly sized. This should
        // not happen, assuming a correct implementation of std::(v)snprintf.
        // Fail in tests, recover by truncating the log message in production.
        // 动态分配的缓冲区大小不正确。假设std::(v)snprintf的正确实现，就不应该发生这种情况。
        // 测试失败，通过在生产中截断日志消息进行恢复。
        assert(false);
        buffer_offset = buffer_size - 1; // 截断日志消息,使得必定能够存放下
      }

      // Add a newline if necessary.
      // 添加换行符
      if (buffer[buffer_offset - 1] != '\n') {
        buffer[buffer_offset] = '\n';
        ++buffer_offset;
      }

      assert(buffer_offset <= buffer_size);
      std::fwrite(buffer, 1, buffer_offset, fp_);
      std::fflush(fp_);

      // 对于堆上的内存需要释放掉
      if (iteration != 0) {
        delete[] buffer;
      }
      break;
    }
  }

 private:
  std::FILE* const fp_; // 日志文件句柄
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
