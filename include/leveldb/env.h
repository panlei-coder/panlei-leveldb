// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_H_

#include <cstdarg>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/export.h"
#include "leveldb/status.h"

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
#if defined(_WIN32)
// On Windows, the method name DeleteFile (below) introduces the risk of
// triggering undefined behavior by exposing the compiler to different
// declarations of the Env class in different translation units.
//
// This is because <windows.h>, a fairly popular header file for Windows
// applications, defines a DeleteFile macro. So, files that include the Windows
// header before this header will contain an altered Env declaration.
//
// This workaround ensures that the compiler sees the same Env declaration,
// independently of whether <windows.h> was included.
/*
在Windows上，方法名称DeleteFile(如下所示)通过将编译器暴露给不同翻译单元中Env类的不同声明，
引入了触发未定义行为的风险。这是因为&lt; Windows .h&gt; (Windows应用程序中相当流行的头文件)定义了一个DeleteFile宏。
因此，在此头文件之前包含Windows头文件的文件将包含修改后的Env声明。
这种解决方法确保编译器看到相同的Env声明，而与是否包含&lt;windows.h&gt;无关。
*/

#if defined(DeleteFile)
#undef DeleteFile
#define LEVELDB_DELETEFILE_UNDEFINED
#endif  // defined(DeleteFile)
#endif  // defined(_WIN32)

/* 
Env是一个抽象接口类，用纯虚函数的形式定义了一些与平台操作的相关接口，如文件系统、多线程、时间操作等。
*/

namespace leveldb {

class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
class WritableFile;

class LEVELDB_EXPORT Env {
 public:
  Env();

  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;

  virtual ~Env();

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to leveldb and must never be deleted.
  //  返回适合当前操作系统的默认环境。资深用户可能希望提供他们自己的Env实现，
  // 而不是依赖于这个默认环境。Default()的结果属于leveldb，永远不能删除。
  static Env* Default();

  // Create an object that sequentially reads the file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.  Implementations should return a
  // NotFound status when the file does not exist.
  //
  // The returned file will only be accessed by one thread at a time.
  // 创建一个对象，以顺序读取具有指定名称的文件。如果成功，将指向新文件的指针存储在*result中并返回OK。
  // 失败时将nullptr存储在*result中并返回非ok。如果文件不存在，则返回非ok状态。
  // 当文件不存在时，实现应该返回NotFound状态。返回的文件一次只能被一个线程访问。
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) = 0;

  // Create an object supporting random-access reads from the file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.  Implementations should return a NotFound status when the file does
  // not exist.
  //
  // The returned file may be concurrently accessed by multiple threads.
  //  创建一个支持从指定名称的文件中随机读取的对象。如果成功，将指向新文件的指针存储在*result中并返回OK。
  // 失败时将nullptr存储在*result中并返回非ok。如果文件不存在，则返回非ok状态。
  // 当文件不存在时，实现应该返回NotFound状态。返回的文件可以被多个线程并发访问。
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) = 0;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  // 创建一个对象，写入具有指定名称的新文件。删除所有同名的现有文件，并创建一个新文件。
  // 如果成功，将指向新文件的指针存储在*result中并返回OK。
  // 失败时将nullptr存储在*result中并返回非ok。返回的文件一次只能被一个线程访问。
  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) = 0;

  // Create an object that either appends to an existing file, or
  // writes to a new file (if the file does not exist to begin with).
  // On success, stores a pointer to the new file in *result and
  // returns OK.  On failure stores nullptr in *result and returns
  // non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  //
  // May return an IsNotSupportedError error if this Env does
  // not allow appending to an existing file.  Users of Env (including
  // the leveldb implementation) must be prepared to deal with
  // an Env that does not support appending.
  // 创建一个对象，该对象要么追加到现有文件，要么写入新文件(如果该文件一开始就不存在)。
  // 如果成功，将指向新文件的指针存储在*result中并返回OK。失败时将nullptr存储在*result中并返回非ok。
  // 返回的文件一次只能被一个线程访问。如果此环境不允许追加到现有文件，可能会返回IsNotSupportedError错误。
  // Env的用户(包括leveldb实现)必须准备好处理不支持追加的Env。
  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result);

  // Returns true iff the named file exists.
  // 如果指定的文件存在，则返回true
  virtual bool FileExists(const std::string& fname) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  // 在*result中存储指定目录的子目录名。这些名称是相对于dir的。*结果的原始内容将被删除。
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) = 0;
  // Delete the named file.
  //
  // The default implementation calls DeleteFile, to support legacy Env
  // implementations. Updated Env implementations must override RemoveFile and
  // ignore the existence of DeleteFile. Updated code calling into the Env API
  // must call RemoveFile instead of DeleteFile.
  //
  // A future release will remove DeleteDir and the default implementation of
  // RemoveDir.
  // 删除命名文件。默认实现调用DeleteFile，以支持旧的Env实现。
  // 更新后的Env实现必须覆盖RemoveFile并忽略DeleteFile的存在。
  // 更新后的代码调用Env API必须调用RemoveFile而不是DeleteFile。
  // 未来的版本将删除DeleteDir和RemoveDir的默认实现。
  virtual Status RemoveFile(const std::string& fname);

  // DEPRECATED: Modern Env implementations should override RemoveFile instead.
  //
  // The default implementation calls RemoveFile, to support legacy Env user
  // code that calls this method on modern Env implementations. Modern Env user
  // code should call RemoveFile.
  //
  // A future release will remove this method.
  // 已弃用:现代Env实现应该重写RemoveFile。默认实现调用RemoveFile，
  // 以支持在现代Env实现中调用此方法的遗留Env用户代码。
  // 现代Env用户代码应该调用RemoveFile。未来的版本将删除此方法。
  virtual Status DeleteFile(const std::string& fname);

  // Create the specified directory.
  // 创建指定的文件夹
  virtual Status CreateDir(const std::string& dirname) = 0;

  // Delete the specified directory.
  //
  // The default implementation calls DeleteDir, to support legacy Env
  // implementations. Updated Env implementations must override RemoveDir and
  // ignore the existence of DeleteDir. Modern code calling into the Env API
  // must call RemoveDir instead of DeleteDir.
  //
  // A future release will remove DeleteDir and the default implementation of
  // RemoveDir.
  //  删除指定目录。默认实现调用DeleteDir，以支持旧的Env实现。
  // 更新后的Env实现必须覆盖RemoveDir并忽略DeleteDir的存在。
  // 现代代码调用Env API必须调用RemoveDir而不是DeleteDir。
  // 未来的版本将删除DeleteDir和RemoveDir的默认实现。
  virtual Status RemoveDir(const std::string& dirname);

  // DEPRECATED: Modern Env implementations should override RemoveDir instead.
  //
  // The default implementation calls RemoveDir, to support legacy Env user
  // code that calls this method on modern Env implementations. Modern Env user
  // code should call RemoveDir.
  //
  // A future release will remove this method.
  // 已弃用:现代的Env实现应该重写RemoveDir。默认实现调用RemoveDir，
  // 以支持在现代Env实现中调用此方法的遗留Env用户代码。
  // 现代Env用户代码应该调用RemoveDir。未来的版本将删除此方法。
  virtual Status DeleteDir(const std::string& dirname);

  // Store the size of fname in *file_size.
  // 获取指定文件的大小
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // Rename file src to target.
  // 对指定文件进行重命名
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) = 0;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  //  锁定指定的文件。用于防止多个进程并发访问同一个数据库。
  // 如果失败，将nullptr存储在*lock中并返回非ok。
  // 成功时，在*lock中存储一个指向表示获取的锁的对象的指针，并返回OK。
  // 调用者应该调用UnlockFile(*lock)来释放锁。如果进程退出，锁将自动释放。
  // 如果其他人已经持有锁，则立即以失败结束。也就是说，这个调用不会等待现有的锁消失。可以创建命名文件吗
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  //  释放先前成功调用LockFile所获得的锁。由一个成功的LockFile()调用返回的需要:锁还没有被解锁。
  virtual Status UnlockFile(FileLock* lock) = 0;

  // Arrange to run "(*function)(arg)" once in a background thread.
  //
  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  //  安排在后台线程中运行一次(*function)(arg)。函数可以在未指定的线程中运行。
  // 添加到同一个Env中的多个函数可以在不同的线程中并发运行。也就是说，调用者不能假定后台工作项是序列化的。
  virtual void Schedule(void (*function)(void* arg), void* arg) = 0;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  //  启动一个新线程，在新线程中调用函数(arg)。当function(arg)返回时，线程将被销毁。
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or may not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  //  *path被设置为一个临时目录，可以用于测试。它可能是刚刚创建的，也可能不是。
  // 在运行同一进程之间，目录可能不同，也可能相同，但后续调用将返回相同的目录。
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Create and return a log file for storing informational messages.
  // 创建并返回一个日志文件，用于存储日志信息
  virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  // 返回自某个固定时间点以来的微秒数。只对计算时间函数有用。
  virtual uint64_t NowMicros() = 0;

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  // 休眠/将线程延迟指定的微秒数。
  virtual void SleepForMicroseconds(int micros) = 0;
};

// A file abstraction for reading sequentially through a file
// 按顺序读取文件的文件抽象
class LEVELDB_EXPORT SequentialFile {
 public:
  SequentialFile() = default;

  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(const SequentialFile&) = delete;

  virtual ~SequentialFile();

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  //  从文件中读取最多n个字节。划痕(0 . .N-1]可以用这个例程来写。
  // 将*result设置为读取的数据(包括成功读取少于n字节的数据)。
  // 可以设置*result指向scratch中的数据[0..]N-1]，所以scratch[0..]当使用*result时，N-1]必须是活的。
  // 如果遇到错误，则返回非ok状态。要求:外部同步
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  // 从文件中跳过n个字节。这保证不会比读取相同的数据慢，但可能会更快。
  // 如果到达文件末尾，跳过将在文件末尾停止，Skip将返回OK。要求:外部同步
  virtual Status Skip(uint64_t n) = 0;
};

// A file abstraction for randomly reading the contents of a file.
// 随机读取文件的抽象
class LEVELDB_EXPORT RandomAccessFile {
 public:
  RandomAccessFile() = default;

  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  //  从offset开始从文件中读取最多n个字节。划痕(0 . .N-1]可以用这个例程来写。将*result设置为读取的数据(包括成功读取少于n字节的数据)。
  // 可以设置*result指向scratch中的数据[0..]N-1]，所以scratch[0..]当使用*result时，N-1]必须是活的。如果遇到错误，则返回非ok状态。对于多线程并发使用是安全的。
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
//  用于顺序写入的文件抽象。实现必须提供缓冲，因为调用者可能一次向文件追加一小段。
class LEVELDB_EXPORT WritableFile {
 public:
  WritableFile() = default;

  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;
};

// An interface for writing log messages.
// 写日志文件的抽象
class LEVELDB_EXPORT Logger {
 public:
  Logger() = default;

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  virtual ~Logger();

  // Write an entry to the log file with the specified format.
  // 使用指定的格式向日志文件中写入entry
  virtual void Logv(const char* format, std::va_list ap) = 0;
};

// Identifies a locked file.
// 标识一个被锁的文件
class LEVELDB_EXPORT FileLock {
 public:
  FileLock() = default;

  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;

  virtual ~FileLock();
};

// Log the specified data to *info_log if info_log is non-null.
//  如果info Log不为空，将指定的数据记录到*info Log中。
void Log(Logger* info_log, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 2, 3)))
#endif
    ;

// A utility routine: write "data" to the named file.
// 实用程序例程:将数据写入指定文件。
LEVELDB_EXPORT Status WriteStringToFile(Env* env, const Slice& data,
                                        const std::string& fname);

// A utility routine: read contents of named file into *data
//  实用程序例程:将命名文件的内容读入*data
LEVELDB_EXPORT Status ReadFileToString(Env* env, const std::string& fname,
                                       std::string* data);

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
//  Env的实现，将所有调用转发到另一个Env。对于希望覆盖另一个Env的部分功能的客户端可能很有用。
// 使用代理模式，尽可能的减少重复代码量
class LEVELDB_EXPORT EnvWrapper : public Env {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t.
  explicit EnvWrapper(Env* t) : target_(t) {}
  virtual ~EnvWrapper();

  // Return the target to which this Env forwards all calls.
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target().
  // 创建对文件的顺序读
  Status NewSequentialFile(const std::string& f, SequentialFile** r) override {
    return target_->NewSequentialFile(f, r);
  }

// 创建对文件的随机读
  Status NewRandomAccessFile(const std::string& f,
                             RandomAccessFile** r) override {
    return target_->NewRandomAccessFile(f, r);
  }

  // 创建对文件的写
  Status NewWritableFile(const std::string& f, WritableFile** r) override {
    return target_->NewWritableFile(f, r);
  }

  // 创建对文件的最佳操作
  Status NewAppendableFile(const std::string& f, WritableFile** r) override {
    return target_->NewAppendableFile(f, r);
  }
  
  // 判断文件是否存在
  bool FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }

  // 获取指定文件的子目录
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }

  // 删除文件
  Status RemoveFile(const std::string& f) override {
    return target_->RemoveFile(f);
  }

  // 创建文件夹
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }

  // 删除文件夹
  Status RemoveDir(const std::string& d) override {
    return target_->RemoveDir(d);
  }

  // 获取指定文件的大小
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }

  // 对指定的文件进行重命名
  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }

  // 锁定指定的文件
  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }

  // 对指定的文件进行解锁
  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
  void Schedule(void (*f)(void*), void* a) override {
    return target_->Schedule(f, a);
  }

  // 开启线程
  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }

  // 获取指定路径的文件
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  
  // 创建日志文件
  Status NewLogger(const std::string& fname, Logger** result) override {
    return target_->NewLogger(fname, result);
  }

  // 返回从某一时刻到现在所经历的时间
  uint64_t NowMicros() override { return target_->NowMicros(); }

  // 休眠指定的时间
  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }

 private:
  Env* target_;  // 封装了Env对象，使用代理模式
};

}  // namespace leveldb

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
// Redefine DeleteFile if it was undefined earlier.
// 当leveldb::Env::DeleteFile被删除时，此解决方案可以被删除。如果之前未定义DeleteFile，则重新定义它。
#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#if defined(UNICODE)
#define DeleteFile DeleteFileW
#else
#define DeleteFile DeleteFileA
#endif  // defined(UNICODE)
#endif  // defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)

#endif  // STORAGE_LEVELDB_INCLUDE_ENV_H_
