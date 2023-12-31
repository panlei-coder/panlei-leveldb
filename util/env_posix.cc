// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#ifndef __Fuchsia__
#include <sys/resource.h>
#endif
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/env_posix_test_helper.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

/*
使用全局作用域“::”来访问系统调用函数
*/

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
// 设置由EnvPosixTestHelper:: SetReadOnlyMMapLimit()和MaxOpenFiles()。
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
// 64位二进制文件最多1000个mmap区域;32位无。
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReadOnlyMMapLimit().
// 可以使用EnvPosixTestHelper:: SetReadOnlyMMapLimit()设置。
int g_mmap_limit = kDefaultMmapLimit;

// Common flags defined for all posix open operations
// 为所有posix打开操作定义的公共标志
#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif  // defined(HAVE_O_CLOEXEC)

constexpr const size_t kWritableFileBufferSize = 65536; // 64KB

// 返回错误
Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
// Helper类限制资源使用以避免资源耗尽。目前用于限制只读文件描述符和mmap文件的使用，
// 这样我们就不会耗尽文件描述符或虚拟内存，或者在非常大的数据库中遇到内核性能问题。
class Limiter {
 public:
  // Limit maximum number of resources to |max_acquires|.
  // 限制资源的最大数量为|max_acquires|
  // 使用了条件编译
  Limiter(int max_acquires)
      : // 初始化列表
#if !defined(NDEBUG) // 使用了条件编译
        max_acquires_(max_acquires),
#endif  // !defined(NDEBUG)
        acquires_allowed_(max_acquires) {
    assert(max_acquires >= 0); // 
  }

  Limiter(const Limiter&) = delete;
  Limiter operator=(const Limiter&) = delete;

  // If another resource is available, acquire it and return true.
  // Else return false.
  // 如果其他资源可用，则获取它并返回true。否则返回false。
  bool Acquire() {
    int old_acquires_allowed =
        acquires_allowed_.fetch_sub(1, std::memory_order_relaxed); // 原子操作减一

    if (old_acquires_allowed > 0) return true;

    int pre_increment_acquires_allowed =
        acquires_allowed_.fetch_add(1, std::memory_order_relaxed); // 原子操作加一

    // Silence compiler warnings about unused arguments when NDEBUG is defined.
    // 当定义NDEBUG时，默认编译器对未使用参数的警告。
    (void)pre_increment_acquires_allowed;
    // If the check below fails, Release() was called more times than acquire.
    // 如果下面的检查失败，Release()被调用的次数多于acquire。
    assert(pre_increment_acquires_allowed < max_acquires_);

    return false;
  }

  // Release a resource acquired by a previous call to Acquire() that returned
  // true.
  // 释放之前调用Acquire()获得的资源，该方法返回true。
  void Release() {
    int old_acquires_allowed =
        acquires_allowed_.fetch_add(1, std::memory_order_relaxed);

    // Silence compiler warnings about unused arguments when NDEBUG is defined.
    (void)old_acquires_allowed;
    // If the check below fails, Release() was called more times than acquire.
    assert(old_acquires_allowed < max_acquires_);
  }

 private:
#if !defined(NDEBUG)
  // Catches an excessive number of Release() calls.
  // 捕获过多的Release()调用。
  const int max_acquires_;  // 最大允许获取的资源数
#endif  // !defined(NDEBUG)

  // The number of available resources.
  //
  // This is a counter and is not tied to the invariants of any other class, so
  // it can be operated on safely using std::memory_order_relaxed.
  // 可用资源的数量。这是一个计数器，不绑定到任何其他类的不变量，因此可以使用std::memory顺序放松安全地对其进行操作。
  std::atomic<int> acquires_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
// 使用read()在文件中实现顺序读访问。这个类的实例是线程友好的，但不是线程安全的，这是SequentialFile API所要求的。
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(std::move(filename)) {}
  ~PosixSequentialFile() override { close(fd_); }

// 从fd_中读取指定长度的内容到缓存区scratch中（顺序读取，非线程安全）
  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    while (true) {
      ::ssize_t read_size = ::read(fd_, scratch, n); // 从fd_中读取指定长度的内容到缓存区scratch中（系统调用函数）
      if (read_size < 0) {  // Read error.
        if (errno == EINTR) {
          continue;  // Retry
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

// 
  Status Skip(uint64_t n) override {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) { // 系统调用函数，用于在文件描述符指定的文件中进行定位操作，n为偏移量，SEEK_CUR为起始位置
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;  // 文件描述符
  const std::string filename_; // 文件名称
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
// 使用pread()在文件中实现随机读访问。这个类的实例是线程安全的，
// 这是RandomAccessFile API所要求的。实例是不可变的，并且Read()只调用线程安全的库函数。
class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
  // instance, and will be used to determine if .
  PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
      : has_permanent_fd_(fd_limiter->Acquire()),
        fd_(has_permanent_fd_ ? fd : -1),
        fd_limiter_(fd_limiter),
        filename_(std::move(filename)) {
    if (!has_permanent_fd_) {
      assert(fd_ == -1);
      ::close(fd);  // The file will be opened on every read.
    }
  }

  ~PosixRandomAccessFile() override {
    if (has_permanent_fd_) {
      assert(fd_ != -1);
      ::close(fd_);
      fd_limiter_->Release();
    }
  }

// 
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    int fd = fd_;
    if (!has_permanent_fd_) { // 判断文件是否打开，如果没有则以只读的方式打开
      fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
      if (fd < 0) {
        return PosixError(filename_, errno);
      }
    }

    assert(fd != -1);

    Status status;
    ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset)); // pread函数在多线程环境下是安全的，但是可能需要额外的同步措施来保护文件描述符的并发访问
    *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
    if (read_size < 0) {
      // An error: return a non-ok status.
      status = PosixError(filename_, errno);
    }
    if (!has_permanent_fd_) {
      // Close the temporary file descriptor opened earlier.
      assert(fd != fd_);
      ::close(fd);
    }
    return status;
  }

 private:
 //如果为false，每次读取都打开文件。
  const bool has_permanent_fd_;  // If false, the file is opened on every read.
  // 如果has_permanent_fd_为true，则为文件描述符
  const int fd_;                 // -1 if has_permanent_fd_ is false.
  // 限制只读文件描述符的使用
  Limiter* const fd_limiter_;
  // 文件名
  const std::string filename_;
};

/*
使用mmap共享内存来提高性能
1. mmap()函数可以把一个文件或者Posix共享内存对象映射到调用进程的地址空间。映射分为两种。
  a. 文件映射：文件映射将一个文件的一部分直接映射到调用进程的虚拟内存中。
  一旦一个文件被映射之后就可以通过在相应的内存区域中操作字节来访问文件内容了。
  映射的分页会在需要的时候从文件中（自动）加载。这种映射也被称为基于文件的映射或内存映射文件。
  b.匿名映射：一个匿名映射没有对应的文件。相反，这种映射的分页会被初始化为 0。
2.mmap()系统调用在调用进程的虚拟地址空间中创建一个新映射。
3.munmap()系统调用执行与mmap()相反的操作，即从调用进程的虚拟地址空间中删除映射。
4.msync:用于刷盘策略。内核的虚拟内存算法保持内存映射文件与内存映射区的同步，因此内核会在未来某个时刻同步，如果我们需要实时，则需要msync同步刷。
*/
// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
// 使用mmap()在文件中实现随机读访问。这个类的实例是线程安全的，
// 这是RandomAccessFile API所要求的。实例是不可变的，并且Read()只调用线程安全的库函数
class PosixMmapReadableFile final : public RandomAccessFile {
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // acquired the right to use one mmap region, which will be released when this
  // instance is destroyed.
  // Mmap base[0, length-1]指向文件的内存映射内容。它必须是成功调用mmap()的结果。该实例接管了该区域的所有权。
  // mmap limiter|必须活过这个实例。调用方必须已经获得使用一个mmap区域的权限，该权限将在销毁此实例时被释放。
  PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,
                        Limiter* mmap_limiter)
      : mmap_base_(mmap_base),
        length_(length),
        mmap_limiter_(mmap_limiter),
        filename_(std::move(filename)) {}

  ~PosixMmapReadableFile() override {
    ::munmap(static_cast<void*>(mmap_base_), length_);
    mmap_limiter_->Release();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char* const mmap_base_; // 内存映射区域的起始地址
  const size_t length_; // 内存映射区域的长度
  Limiter* const mmap_limiter_; // mmap限制器，用于控制内存映射的使用权限
  const std::string filename_; // 文件名
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
      : pos_(0),
        fd_(fd),
        is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)),
        dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }

// 向文件中追加数据
  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    // 尽可能的填充缓存区
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_); 
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // 如果data数据太大,那么需要分多次写入,所以至少需要刷盘一次
    // Can't fit in buffer, so need to do at least one write.
    // 不能填满缓存区，所以需要至少一个写（刷盘操作）
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    // 经过前面一次写入到缓存区之后，剩余的大小如果小于则写入缓冲区，大于则直接写入文件
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
  }

// 关闭操作
  Status Close() override {
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  Status Flush() override { return FlushBuffer(); }

// 同步操作
  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    // 确保manifest所引用的新文件在文件系统中。这需要在将清单文件刷新到磁盘之前进行，以避免在清单引用尚未在磁盘上的文件的状态下崩溃。
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }

    status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    return SyncFd(fd_, filename_); // 将文件所在的目录刷新到文件系统
  }

 private:
  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size); // 将缓存区中的内容刷到指定的文件中
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;  // Retry
        }
        return PosixError(filename_, errno);
      }
      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

// 如果是"mainifest"开头的文件，则将文件同步到文件系统中
  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  // 确保与给定文件描述符数据相关联的所有缓存一直刷新到持久介质，
  // 并且可以承受电源故障。如果发生错误，path参数仅用于在返回的Status中填充描述字符串。
  static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
    // On macOS and iOS, fsync() doesn't guarantee durability past power
    // failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
    // filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
    // fsync().
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
      return Status::OK();
    }
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
    bool sync_success = ::fdatasync(fd) == 0;
#else
    bool sync_success = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fd_path, errno);
  }

  // Returns the directory name in a path pointing to a file.
  //
  // Returns "." if the path does not contain any directory separator.
  // 返回指向文件的路径中的目录名。如果路径不包含任何目录分隔符，则返回“.”。
  static std::string Dirname(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    // filename组件不应该包含路径分隔符。如果是，则表示拆分操作不正确。
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  // 从指向文件的路径中提取文件名。返回的Slice指向|filename|s数据缓冲区，因此只有当|filename|存在且未改变时才有效。
  static Slice Basename(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return Slice(filename.data() + separator_pos + 1,
                 filename.length() - separator_pos - 1);
  }

  // True if the given file is a manifest file.
  // 判断文件名是否是以"MANIFEST"开始
  static bool IsManifest(const std::string& filename) {
    return Basename(filename).starts_with("MANIFEST");
  }

  // buf_[0, pos_ - 1] contains data to be written to fd_.
  char buf_[kWritableFileBufferSize]; // 缓存区
  size_t pos_; // 偏移量
  int fd_; // 文件描述符

  const bool is_manifest_;  // True if the file's name starts with MANIFEST. 如果文件名以MANIFEST开头，则为True。
  const std::string filename_; // 文件名
  const std::string dirname_;  // The directory of filename_. 文件夹名称
};

// posix的文件加解锁操作::flock和::fcntl
int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);  // 设置加解锁的类型
  file_lock_info.l_whence = SEEK_SET; // 表示加解锁操作从文件的SEEK_SET起始位置开始
  file_lock_info.l_start = 0; // 偏移量为0
  file_lock_info.l_len = 0;  // Lock/unlock entire file. 加解锁具体的内容大小
  return ::fcntl(fd, F_SETLK, &file_lock_info); // 对文件进行加解锁操作
}

// Instances are thread-safe because they are immutable.
// 实例是线程安全的，因为它们是不可变的。
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}

  int fd() const { return fd_; }
  const std::string& filename() const { return filename_; }

 private:
  const int fd_; // 文件描述符
  const std::string filename_; // 文件名
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
// 跟踪PosixEnv::LockFile()锁定的文件。
// 我们维护一个单独的集合，而不是依赖于fcntl(F SETLK)，因为fcntl(F SETLK)对来自同一进程的多个使用不提供任何保护。
// 实例是线程安全的，因为所有成员数据都由互斥锁保护。
class PosixLockTable {
 public:
 // 添加被锁住的文件到locked_files_中
  bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    bool succeeded = locked_files_.insert(fname).second;
    mu_.Unlock();
    return succeeded;
  }
  // 从locked_files_中删除文件
  void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    locked_files_.erase(fname);
    mu_.Unlock();
  }

 private:
  port::Mutex mu_; // 使用c++的mutex封装的一个锁类型
  std::set<std::string> locked_files_ GUARDED_BY(mu_); // 被锁住的文件集合（使用GUARDED_BY(mu_)标记该变量收到互斥锁的保护）
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static const char msg[] =
        "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr);
    std::abort();
  }

// 创建一个顺序可读的文件
  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }

// 创建一个随机可读的文件
  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    *result = nullptr;
    int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    // 只有当内存映射文件个数超出限制或内存映射文件操作失败时，才会采用pread的方法，实现文件随机读取操作对象
    /*
    ::mmap 将文件映射到内存中，通过直接访问内存来读取文件内容，适用于大型文件的随机读取操作，
    具有高效性和方便性，但需要注意内存管理和同步的问题。
    ::pread 从文件指定位置读取数据，适用于随机读取操作，不会修改文件的当前偏移量，
    适用于多线程或多进程环境下的并发读取操作，但相对于 ::mmap 会涉及到文件打开和关闭的操作。
    */
    if (!mmap_limiter_.Acquire()) {
      *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
      return Status::OK();
    }

    uint64_t file_size;
    Status status = GetFileSize(filename, &file_size);
    if (status.ok()) {
      void* mmap_base =
          ::mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmap_base != MAP_FAILED) {
        *result = new PosixMmapReadableFile(filename,
                                            reinterpret_cast<char*>(mmap_base),
                                            file_size, &mmap_limiter_);
      } else {
        status = PosixError(filename, errno);
      }
    }
    ::close(fd);
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }

// 创建一个顺序可写的文件，不论文件存在与否，均创建新文件
  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644); // 表示文件所有者具有读写权限，其他用户只有读权限（110 100 100）
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

// 创建一个顺序可写的文件，如果文件存在，则在原文件中继续添加，如果文件不存在，则创建新文件
  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

// 判断文件是否存在
  bool FileExists(const std::string& filename) override {
    return ::access(filename.c_str(), F_OK) == 0;  // 文件是否访问成功
  }

// 返回指定路径下所有的子文件
  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();
    ::DIR* dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
      return PosixError(directory_path, errno);
    }
    struct ::dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
      result->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
  }

// 删除指定文件
  Status RemoveFile(const std::string& filename) override {
    /*
    unlink实际上是通过减少当前文件的链接计数，如果链接计数减少到0，
    表示该文件没有任何链接指向它，操作系统内核会将该文件的数据块标记为可重用，并从文件系统中删除该文件的inode。
    */
    if (::unlink(filename.c_str()) != 0) {
      return PosixError(filename, errno);
    }
    return Status::OK();
  }

// 创建新的文件夹
  Status CreateDir(const std::string& dirname) override {
    if (::mkdir(dirname.c_str(), 0755) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

// 删除指定文件夹
  Status RemoveDir(const std::string& dirname) override {
    if (::rmdir(dirname.c_str()) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

// 获取文件大小
  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) != 0) { // ::stat检查指定文件的状态
      *size = 0;
      return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
  }

// 文件重命名
  Status RenameFile(const std::string& from, const std::string& to) override {
    if (std::rename(from.c_str(), to.c_str()) != 0) {
      return PosixError(from, errno);
    }
    return Status::OK();
  }

// 锁定指定文件，避免引发多线程操作对同一个文件的竞争访问
  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!locks_.Insert(filename)) {
      ::close(fd);
      return Status::IOError("lock " + filename, "already held by process");
    }

    if (LockOrUnlock(fd, true) == -1) {
      int lock_errno = errno;
      ::close(fd);
      locks_.Remove(filename);
      return PosixError("lock " + filename, lock_errno);
    }

    *lock = new PosixFileLock(fd, filename);
    return Status::OK();
  }

// 释放文件锁
  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
      return PosixError("unlock " + posix_file_lock->filename(), errno);
    }
    locks_.Remove(posix_file_lock->filename());
    ::close(posix_file_lock->fd());
    delete posix_file_lock;
    return Status::OK();
  }

// Schedule函数将某个函数调度到后台线程中执行，后台线程长期存在，并不会随着函数执行完毕而销毁
// 而如果没有需要执行的函数，后台线程处于等待状态。
  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;
// StartThread函数则是启动一个新的线程，并且在新的线程中执行指定的函数操作，当指定的函数执行完毕后，该线程也将被销毁
  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override {
    std::thread new_thread(thread_main, thread_main_arg);
    new_thread.detach(); // 非阻塞式
  }

// 返回用于测试任务的临时文件夹
  Status GetTestDirectory(std::string* result) override {
    const char* env = std::getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                    static_cast<int>(::geteuid()));
      *result = buf;
    }

    // The CreateDir status is ignored because the directory may already exist.
    CreateDir(*result);

    return Status::OK();
  }

// 创建并返回一个Log文件
  Status NewLogger(const std::string& filename, Logger** result) override {
    int fd = ::open(filename.c_str(),
                    O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    std::FILE* fp = ::fdopen(fd, "w");
    if (fp == nullptr) {
      ::close(fd);
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
  }

// 返回当前的时间戳，单位为ms，可用计算某段代码的执行时间
  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }

// 利用高精度计时器做休眠指定的时间
  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

 private:
  void BackgroundThreadMain();

  static void BackgroundThreadEntryPoint(PosixEnv* env) {
    env->BackgroundThreadMain();
  }

  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe because it is immutable.
  // 将工作项数据存储在Schedule()调用中。实例在调用Schedule()的线程上构造，
  // 并在后台线程上使用。这个结构是线程安全的，因为它是不可变的。
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;  // 后台运行的互斥锁
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);  // 后台运行的条件变量
  bool started_background_thread_ GUARDED_BY(background_work_mutex_); // 是否开启了后台线程

  std::queue<BackgroundWorkItem> background_work_queue_  
      GUARDED_BY(background_work_mutex_); // 后台工作队列

  PosixLockTable locks_;  // Thread-safe.   // 文件锁的集合
  /*
  限制只读文件描述符和 mmap 文件使用，这样我们就不会用完文件描述符或虚拟内存，或者遇到非常大的数据库的内核性能问题。
  */
  Limiter mmap_limiter_;  // Thread-safe.  // mmap资源限制
  Limiter fd_limiter_;    // Thread-safe. // 文件描述符资源限制
};

// Return the maximum number of concurrent mmaps.
int MaxMmaps() { return g_mmap_limit; }

// Return the maximum number of read-only files to keep open.
// 返回要保持打开的只读文件的最大数目。
int MaxOpenFiles() {
  if (g_open_read_only_file_limit >= 0) {
    return g_open_read_only_file_limit;
  }
#ifdef __Fuchsia__
  // Fuchsia doesn't implement getrlimit.
  // 如果在 Fuchsia 操作系统上运行，则将 g_open_read_only_file_limit 设置为50，因为该操作系统不支持 getrlimit 函数。 
  g_open_read_only_file_limit = 50;
#else
// 如果在其他操作系统上运行，则使用 getrlimit 函数获取当前系统中可以打开的最大文件数量，并将结果存储在变量 rlim 中。 
  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim)) { // 获取失败
    // getrlimit failed, fallback to hard-coded default.
    g_open_read_only_file_limit = 50;
  } else if (rlim.rlim_cur == RLIM_INFINITY) { // 如果获取到的最大文件数量是无限的
    g_open_read_only_file_limit = std::numeric_limits<int>::max();
  } else {
    // Allow use of 20% of available file descriptors for read-only files.
    g_open_read_only_file_limit = rlim.rlim_cur / 5;
  }
#endif
  return g_open_read_only_file_limit;
}

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()) {}

// 创建后台线程
void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  // 如果后台线程的队列是空的，则后台线程需要进行等待
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal(); // 唤醒处于等待的线程
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

void PosixEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();

    // Wait until there is work to be done.
    // 如果队列为空，则后台线程处理等待状态
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait();
    }

// 执行队首的任务
    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg);
  }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
/*
包装一个从未创建析构函数的Env实例。预期用法:using PlatformSingletonEnv = SingletonEnv&lt;PlatformEnv&gt;;void ConfigurePosixEnv(int param) {PlatformSingletonEnv::AssertEnvNotInitialized();设置全局配置标志。} Env* Env::Default(){静态PlatformSingletonEnv默认Env;返回默认env.env();
*/
// 单例模式
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_; // 使用了std::aligned_storage来指定对象的对齐方式
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_; // 标记是否被初始化了
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

// 设置只读文件描述符限制
void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

// 设置只读内存映射限制
void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

// 创建一个静态局部变量 env_container，该变量是 PosixDefaultEnv 类的对象，保证只会被初始化一次。
Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb
