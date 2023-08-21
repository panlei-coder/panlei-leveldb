leveldb
=======

_Jeff Dean, Sanjay Ghemawat_

The leveldb library provides a persistent key value store. Keys and values are
arbitrary byte arrays.  The keys are ordered within the key value store
according to a user-specified comparator function.

## Opening A Database

A leveldb database has a name which corresponds to a file system directory. All
of the contents of database are stored in this directory. The following example
shows how to open a database, creating it if necessary:

```c++
#include <cassert>
#include "leveldb/db.h"

leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
assert(status.ok());
...
```

If you want to raise an error if the database already exists, add the following
line before the `leveldb::DB::Open` call:

```c++
options.error_if_exists = true;
```

## Status

You may have noticed the `leveldb::Status` type above. Values of this type are
returned by most functions in leveldb that may encounter an error. You can check
if such a result is ok, and also print an associated error message:

```c++
leveldb::Status s = ...;
if (!s.ok()) cerr << s.ToString() << endl;
```

## Closing A Database

When you are done with a database, just delete the database object. Example:

```c++
... open the db as described above ...
... do something with db ...
delete db;
```

## Reads And Writes

The database provides Put, Delete, and Get methods to modify/query the database.
For example, the following code moves the value stored under key1 to key2.

```c++
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);
```

## Atomic Updates

Note that if the process dies after the Put of key2 but before the delete of
key1, the same value may be left stored under multiple keys. Such problems can
be avoided by using the `WriteBatch` class to atomically apply a set of updates:

```c++
#include "leveldb/write_batch.h"
...
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) {
  leveldb::WriteBatch batch;
  batch.Delete(key1);
  batch.Put(key2, value);
  s = db->Write(leveldb::WriteOptions(), &batch);
}
```

The `WriteBatch` holds a sequence of edits to be made to the database, and these
edits within the batch are applied in order. Note that we called Delete before
Put so that if key1 is identical to key2, we do not end up erroneously dropping
the value entirely.

Apart from its atomicity benefits, `WriteBatch` may also be used to speed up
bulk updates by placing lots of individual mutations into the same batch.

## Synchronous Writes

By default, each write to leveldb is asynchronous: it returns after pushing the
write from the process into the operating system. The transfer from operating
system memory to the underlying persistent storage happens asynchronously. The
sync flag can be turned on for a particular write to make the write operation
not return until the data being written has been pushed all the way to
persistent storage. (On Posix systems, this is implemented by calling either
`fsync(...)` or `fdatasync(...)` or `msync(..., MS_SYNC)` before the write
operation returns.)

```c++
leveldb::WriteOptions write_options;
write_options.sync = true;
db->Put(write_options, ...);
```

Asynchronous writes are often more than a thousand times as fast as synchronous
writes. The downside of asynchronous writes is that a crash of the machine may
cause the last few updates to be lost. Note that a crash of just the writing
process (i.e., not a reboot) will not cause any loss since even when sync is
false, an update is pushed from the process memory into the operating system
before it is considered done.

Asynchronous writes can often be used safely. For example, when loading a large
amount of data into the database you can handle lost updates by restarting the
bulk load after a crash. A hybrid scheme is also possible where every Nth write
is synchronous, and in the event of a crash, the bulk load is restarted just
after the last synchronous write finished by the previous run. (The synchronous
write can update a marker that describes where to restart on a crash.)

`WriteBatch` provides an alternative to asynchronous writes. Multiple updates
may be placed in the same WriteBatch and applied together using a synchronous
write (i.e., `write_options.sync` is set to true). The extra cost of the
synchronous write will be amortized across all of the writes in the batch.

## Concurrency

A database may only be opened by one process at a time. The leveldb
implementation acquires a lock from the operating system to prevent misuse.
Within a single process, the same `leveldb::DB` object may be safely shared by
multiple concurrent threads. I.e., different threads may write into or fetch
iterators or call Get on the same database without any external synchronization
(the leveldb implementation will automatically do the required synchronization).
However other objects (like Iterator and `WriteBatch`) may require external
synchronization. If two threads share such an object, they must protect access
to it using their own locking protocol. More details are available in the public
header files.

## Iteration

The following example demonstrates how to print all key,value pairs in a
database.

```c++
leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
}
assert(it->status().ok());  // Check for any errors found during the scan
delete it;
```

The following variation shows how to process just the keys in the range
[start,limit):

```c++
for (it->Seek(start);
   it->Valid() && it->key().ToString() < limit;
   it->Next()) {
  ...
}
```

You can also process entries in reverse order. (Caveat: reverse iteration may be
somewhat slower than forward iteration.)

```c++
for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ...
}
```

## Snapshots

Snapshots provide consistent read-only views over the entire state of the
key-value store.  `ReadOptions::snapshot` may be non-NULL to indicate that a
read should operate on a particular version of the DB state. If
`ReadOptions::snapshot` is NULL, the read will operate on an implicit snapshot
of the current state.

Snapshots are created by the `DB::GetSnapshot()` method:

```c++
leveldb::ReadOptions options;
options.snapshot = db->GetSnapshot();
... apply some updates to db ...
leveldb::Iterator* iter = db->NewIterator(options);
... read using iter to view the state when the snapshot was created ...
delete iter;
db->ReleaseSnapshot(options.snapshot);
```

Note that when a snapshot is no longer needed, it should be released using the
`DB::ReleaseSnapshot` interface. This allows the implementation to get rid of
state that was being maintained just to support reading as of that snapshot.

## Slice

The return value of the `it->key()` and `it->value()` calls above are instances
of the `leveldb::Slice` type. Slice is a simple structure that contains a length
and a pointer to an external byte array. Returning a Slice is a cheaper
alternative to returning a `std::string` since we do not need to copy
potentially large keys and values. In addition, leveldb methods do not return
null-terminated C-style strings since leveldb keys and values are allowed to
contain `'\0'` bytes.

C++ strings and null-terminated C-style strings can be easily converted to a
Slice:

```c++
leveldb::Slice s1 = "hello";

std::string str("world");
leveldb::Slice s2 = str;
```

A Slice can be easily converted back to a C++ string:

```c++
std::string str = s1.ToString();
assert(str == std::string("hello"));
```

Be careful when using Slices since it is up to the caller to ensure that the
external byte array into which the Slice points remains live while the Slice is
in use. For example, the following is buggy:

```c++
leveldb::Slice slice;
if (...) {
  std::string str = ...;
  slice = str;
}
Use(slice);
```

When the if statement goes out of scope, str will be destroyed and the backing
storage for slice will disappear.

## Comparators

The preceding examples used the default ordering function for key, which orders
bytes lexicographically. You can however supply a custom comparator when opening
a database.  For example, suppose each database key consists of two numbers and
we should sort by the first number, breaking ties by the second number. First,
define a proper subclass of `leveldb::Comparator` that expresses these rules:

```c++
class TwoPartComparator : public leveldb::Comparator {
 public:
  // Three-way comparison function:
  //   if a < b: negative result
  //   if a > b: positive result
  //   else: zero result
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    int a1, a2, b1, b2;
    ParseKey(a, &a1, &a2);
    ParseKey(b, &b1, &b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }

  // Ignore the following methods for now:
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
  void FindShortSuccessor(std::string*) const {}
};
```

Now create a database using this custom comparator:

```c++
TwoPartComparator cmp;
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
options.comparator = &cmp;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
...
```

### Backwards compatibility

The result of the comparator's Name method is attached to the database when it
is created, and is checked on every subsequent database open. If the name
changes, the `leveldb::DB::Open` call will fail. Therefore, change the name if
and only if the new key format and comparison function are incompatible with
existing databases, and it is ok to discard the contents of all existing
databases.

You can however still gradually evolve your key format over time with a little
bit of pre-planning. For example, you could store a version number at the end of
each key (one byte should suffice for most uses). When you wish to switch to a
new key format (e.g., adding an optional third part to the keys processed by
`TwoPartComparator`), (a) keep the same comparator name (b) increment the
version number for new keys (c) change the comparator function so it uses the
version numbers found in the keys to decide how to interpret them.

## Performance

Performance can be tuned by changing the default values of the types defined in
`include/options.h`.

### Block size

leveldb groups adjacent keys together into the same block and such a block is
the unit of transfer to and from persistent storage. The default block size is
approximately 4096 uncompressed bytes.  Applications that mostly do bulk scans
over the contents of the database may wish to increase this size. Applications
that do a lot of point reads of small values may wish to switch to a smaller
block size if performance measurements indicate an improvement. There isn't much
benefit in using blocks smaller than one kilobyte, or larger than a few
megabytes. Also note that compression will be more effective with larger block
sizes.

### Compression

Each block is individually compressed before being written to persistent
storage. Compression is on by default since the default compression method is
very fast, and is automatically disabled for uncompressible data. In rare cases,
applications may want to disable compression entirely, but should only do so if
benchmarks show a performance improvement:

```c++
leveldb::Options options;
options.compression = leveldb::kNoCompression;
... leveldb::DB::Open(options, name, ...) ....
```

### Cache

The contents of the database are stored in a set of files in the filesystem and
each file stores a sequence of compressed blocks. If options.block_cache is
non-NULL, it is used to cache frequently used uncompressed block contents.

```c++
#include "leveldb/cache.h"

leveldb::Options options;
options.block_cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB cache
leveldb::DB* db;
leveldb::DB::Open(options, name, &db);
... use the db ...
delete db
delete options.block_cache;
```

Note that the cache holds uncompressed data, and therefore it should be sized
according to application level data sizes, without any reduction from
compression. (Caching of compressed blocks is left to the operating system
buffer cache, or any custom Env implementation provided by the client.)

When performing a bulk read, the application may wish to disable caching so that
the data processed by the bulk read does not end up displacing most of the
cached contents. A per-iterator option can be used to achieve this:

```c++
leveldb::ReadOptions options;
options.fill_cache = false;
leveldb::Iterator* it = db->NewIterator(options);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ...
}
delete it;
```

### Key Layout

Note that the unit of disk transfer and caching is a block. Adjacent keys
(according to the database sort order) will usually be placed in the same block.
Therefore the application can improve its performance by placing keys that are
accessed together near each other and placing infrequently used keys in a
separate region of the key space.

For example, suppose we are implementing a simple file system on top of leveldb.
The types of entries we might wish to store are:

    filename -> permission-bits, length, list of file_block_ids
    file_block_id -> data

We might want to prefix filename keys with one letter (say '/') and the
`file_block_id` keys with a different letter (say '0') so that scans over just
the metadata do not force us to fetch and cache bulky file contents.

### Filters

Because of the way leveldb data is organized on disk, a single `Get()` call may
involve multiple reads from disk. The optional FilterPolicy mechanism can be
used to reduce the number of disk reads substantially.

```c++
leveldb::Options options;
options.filter_policy = NewBloomFilterPolicy(10);
leveldb::DB* db;
leveldb::DB::Open(options, "/tmp/testdb", &db);
... use the database ...
delete db;
delete options.filter_policy;
```

The preceding code associates a Bloom filter based filtering policy with the
database.  Bloom filter based filtering relies on keeping some number of bits of
data in memory per key (in this case 10 bits per key since that is the argument
we passed to `NewBloomFilterPolicy`). This filter will reduce the number of
unnecessary disk reads needed for Get() calls by a factor of approximately
a 100. Increasing the bits per key will lead to a larger reduction at the cost
of more memory usage. We recommend that applications whose working set does not
fit in memory and that do a lot of random reads set a filter policy.

If you are using a custom comparator, you should ensure that the filter policy
you are using is compatible with your comparator. For example, consider a
comparator that ignores trailing spaces when comparing keys.
`NewBloomFilterPolicy` must not be used with such a comparator. Instead, the
application should provide a custom filter policy that also ignores trailing
spaces. For example:

```c++
class CustomFilterPolicy : public leveldb::FilterPolicy {
 private:
  leveldb::FilterPolicy* builtin_policy_;

 public:
  CustomFilterPolicy() : builtin_policy_(leveldb::NewBloomFilterPolicy(10)) {}
  ~CustomFilterPolicy() { delete builtin_policy_; }

  const char* Name() const { return "IgnoreTrailingSpacesFilter"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const {
    // Use builtin bloom filter code after removing trailing spaces
    std::vector<leveldb::Slice> trimmed(n);
    for (int i = 0; i < n; i++) {
      trimmed[i] = RemoveTrailingSpaces(keys[i]);
    }
    builtin_policy_->CreateFilter(trimmed.data(), n, dst);
  }
};
```

Advanced applications may provide a filter policy that does not use a bloom
filter but uses some other mechanism for summarizing a set of keys. See
`leveldb/filter_policy.h` for detail.

## Checksums

leveldb associates checksums with all data it stores in the file system. There
are two separate controls provided over how aggressively these checksums are
verified:

`ReadOptions::verify_checksums` may be set to true to force checksum
verification of all data that is read from the file system on behalf of a
particular read.  By default, no such verification is done.

`Options::paranoid_checks` may be set to true before opening a database to make
the database implementation raise an error as soon as it detects an internal
corruption. Depending on which portion of the database has been corrupted, the
error may be raised when the database is opened, or later by another database
operation. By default, paranoid checking is off so that the database can be used
even if parts of its persistent storage have been corrupted.

If a database is corrupted (perhaps it cannot be opened when paranoid checking
is turned on), the `leveldb::RepairDB` function may be used to recover as much
of the data as possible

## Approximate Sizes

The `GetApproximateSizes` method can used to get the approximate number of bytes
of file system space used by one or more key ranges.

```c++
leveldb::Range ranges[2];
ranges[0] = leveldb::Range("a", "c");
ranges[1] = leveldb::Range("x", "z");
uint64_t sizes[2];
db->GetApproximateSizes(ranges, 2, sizes);
```

The preceding call will set `sizes[0]` to the approximate number of bytes of
file system space used by the key range `[a..c)` and `sizes[1]` to the
approximate number of bytes used by the key range `[x..z)`.

## Environment

All file operations (and other operating system calls) issued by the leveldb
implementation are routed through a `leveldb::Env` object. Sophisticated clients
may wish to provide their own Env implementation to get better control.
For example, an application may introduce artificial delays in the file IO
paths to limit the impact of leveldb on other activities in the system.

```c++
class SlowEnv : public leveldb::Env {
  ... implementation of the Env interface ...
};

SlowEnv env;
leveldb::Options options;
options.env = &env;
Status s = leveldb::DB::Open(options, ...);
```

## Porting

leveldb may be ported to a new platform by providing platform specific
implementations of the types/methods/functions exported by
`leveldb/port/port.h`.  See `leveldb/port/port_example.h` for more details.

In addition, the new platform may need a new default `leveldb::Env`
implementation.  See `leveldb/util/env_posix.h` for an example.

## Other Information

Details about the leveldb implementation may be found in the following
documents:

1. [Implementation notes](impl.md)
2. [Format of an immutable Table file](table_format.md)
3. [Format of a log file](log_format.md)


leveldb 库提供持久键值存储。键和值是任意字节数组。根据用户指定的比较器函数，键在键值存储中排序。

一.打开数据库

leveldb 数据库的名称对应于文件系统目录。数据库的所有内容都存储在该目录中。以下示例显示如何打开数据库，并在必要时创建数据库：

#include <cassert>
#include "leveldb/db.h"

leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
assert(status.ok());
...

如果您想在数据库已存在的情况下引发错误，请在调用之前添加以下行leveldb::DB::Open：

options.error_if_exists = true;

二.Status

您可能已经注意到leveldb::Status上面的类型。leveldb 中大多数可能遇到错误的函数都会返回这种类型的值。您可以检查这样的结果是否正确，并打印相关的错误消息：

leveldb::Status s = ...;
if (!s.ok()) cerr << s.ToString() << endl;

三.关闭数据库

当您使用完数据库后，只需删除数据库对象即可。例子：

... open the db as described above ...
... do something with db ...
delete db;

四.读取和写入

数据库提供Put、Delete、Get方法来修改/查询数据库。例如，以下代码将存储在 key1 下的值移动到 key2。

std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);

五.原子更新

请注意，如果进程在放置 key2 之后但在删除 key1 之前终止，则相同的值可能会存储在多个键下。WriteBatch通过使用该类以原子方式应用一组更新可以避免此类问题：

#include "leveldb/write_batch.h"
...
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) {
  leveldb::WriteBatch batch;
  batch.Delete(key1);
  batch.Put(key2, value);
  s = db->Write(leveldb::WriteOptions(), &batch);
}
它WriteBatch保存要对数据库进行的一系列编辑，并且批处理中的这些编辑将按顺序应用。请注意，我们在 Put 之前调用了 Delete，这样如果 key1 与 key2 相同，我们最终不会错误地完全删除该值。

除了其原子性优势之外，WriteBatch还可以通过将大量单独的突变放入同一批次中来加速批量更新。

六.同步写入

默认情况下，对 leveldb 的每次写入都是异步的：它在将写入从进程推送到操作系统后返回。从操作系统内存到底层持久存储的传输是异步发生的。可以针对特定写入打开同步标志，以使写入操作不会返回，直到正在写入的数据一直推送到持久存储为止。（在 Posix 系统上，这是通过在写操作返回之前调用 or fsync(...)或fdatasync(...)来实现的。）msync(..., MS_SYNC)

leveldb::WriteOptions write_options;
write_options.sync = true;
db->Put(write_options, ...);
异步写入通常比同步写入快一千倍以上。异步写入的缺点是机器崩溃可能会导致最后几次更新丢失。请注意，仅写入进程的崩溃（即不是重新启动）不会造成任何损失，因为即使同步为假，更新也会在被视为完成之前从进程内存推送到操作系统。

异步写入通常可以安全地使用。例如，当将大量数据加载到数据库中时，您可以通过在崩溃后重新启动批量加载来处理丢失的更新。混合方案也是可能的，其中每个第 N 次写入都是同步的，并且在发生崩溃的情况下，在上一次运行完成最后一次同步写入后立即重新启动批量加载。（同步写入可以更新描述崩溃时从何处重新启动的标记。）

WriteBatch提供异步写入的替代方案。多个更新可以放置在同一个 WriteBatch 中，并使用同步写入（即write_options.sync设置为 true）一起应用。同步写入的额外成本将分摊到批次中的所有写入中。

七.并发性

数据库一次只能由一个进程打开。leveldb 实现从操作系统获取锁以防止误用。在单个进程内，同一个leveldb::DB对象可以由多个并发线程安全地共享。即，不同的线程可以在同一个数据库上写入或获取迭代器或调用 Get，而无需任何外部同步（leveldb 实现将自动执行所需的同步）。然而其他对象（如 Iterator 和WriteBatch）可能需要外部同步。如果两个线程共享这样的对象，它们必须使用自己的锁定协议来保护对其的访问。更多详细信息可在公共头文件中找到。

八.迭代

以下示例演示如何打印数据库中的所有键、值对。

leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
}
assert(it->status().ok());  // Check for any errors found during the scan
delete it;
以下变体显示了如何仅处理 [start,limit) 范围内的键：

for (it->Seek(start);
   it->Valid() && it->key().ToString() < limit;
   it->Next()) {
  ...
}
您还可以按相反的顺序处理条目。（警告：反向迭代可能比正向迭代慢一些。）

for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ...
}

九.快照

快照提供键值存储整个状态的一致只读视图。 ReadOptions::snapshot可以是非 NULL 来指示读取应该在 DB 状态的特定版本上进行操作。如果 ReadOptions::snapshot为 NULL，则读取将对当前状态的隐式快照进行操作。

快照是通过以下DB::GetSnapshot()方法创建的：

leveldb::ReadOptions options;
options.snapshot = db->GetSnapshot();
... apply some updates to db ...
leveldb::Iterator* iter = db->NewIterator(options);
... read using iter to view the state when the snapshot was created ...
delete iter;
db->ReleaseSnapshot(options.snapshot);
请注意，当不再需要快照时，应使用该 DB::ReleaseSnapshot接口将其释放。这允许实现摆脱仅仅为了支持读取该快照而维护的状态。

十.Slice

it->key()上面的和调用的返回值it->value()是该leveldb::Slice类型的实例。Slice 是一个简单的结构，包含长度和指向外部字节数组的指针。返回 Slice 是返回 a 的更便宜的替代方案std::string，因为我们不需要复制可能很大的键和值。此外，leveldb 方法不会返回以 null 结尾的 C 样式字符串，因为 leveldb 键和值允许包含'\0'字节。

C++ 字符串和以 null 结尾的 C 风格字符串可以轻松转换为 Slice：

leveldb::Slice s1 = "hello";

std::string str("world");
leveldb::Slice s2 = str;
Slice 可以轻松转换回 C++ 字符串：

std::string str = s1.ToString();
assert(str == std::string("hello"));
使用切片时要小心，因为调用者需要确保切片点所在的外部字节数组在使用切片时保持活动状态。例如，以下内容是有问题的：

leveldb::Slice slice;
if (...) {
  std::string str = ...;
  slice = str;
}
Use(slice);
当 if 语句超出范围时，str 将被销毁，并且 slice 的后备存储将消失。

十一.比较器

前面的示例使用了 key 的默认排序函数，该函数按字典顺序对字节进行排序。但是，您可以在打开数据库时提供自定义比较器。例如，假设每个数据库键由两个数字组成，我们应该按第一个数字排序，按第二个数字打破平局。首先，定义一个适当的子类来leveldb::Comparator表达这些规则：

class TwoPartComparator : public leveldb::Comparator {
 public:
  // Three-way comparison function:
  //   if a < b: negative result
  //   if a > b: positive result
  //   else: zero result
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    int a1, a2, b1, b2;
    ParseKey(a, &a1, &a2);
    ParseKey(b, &b1, &b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }

  // Ignore the following methods for now:
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
  void FindShortSuccessor(std::string*) const {}
};
现在使用此自定义比较器创建一个数据库：

TwoPartComparator cmp;
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
options.comparator = &cmp;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
...

十二.向后兼容性

比较器的 Name 方法的结果在创建数据库时附加到数据库，并在每个后续数据库打开时进行检查。如果名称改变，leveldb::DB::Open调用将会失败。因此，当且仅当新的密钥格式和比较功能与现有数据库不兼容时，才需要更改名称，并且丢弃所有现有数据库的内容也可以。

然而，您仍然可以通过一些预先规划，随着时间的推移逐渐改进您的密钥格式。例如，您可以在每个密钥的末尾存储一个版本号（一个字节应该足以满足大多数用途）。当您希望切换到新的密钥格式（例如，向由 处理的密钥添加可选的第三部分 TwoPartComparator）时，(a) 保持相同的比较器名称 (b) 增加新密钥的版本号 (c) 更改比较器功能因此它使用密钥中找到的版本号来决定如何解释它们。

十三.Performance

可以通过更改 中定义的类型的默认值来调整性能 include/options.h。

1.块大小
leveldb 将相邻的键组合到同一个块中，这样的块是与持久存储之间传输的单位。默认块大小约为 4096 未压缩字节。主要对数据库内容进行批量扫描的应用程序可能希望增加此大小。如果性能测量表明性能有所改善，则对小值进行大量点读取的应用程序可能希望切换到较小的块大小。使用小于 1 KB 或大于几 MB 的块并没有多大好处。另请注意，块大小越大，压缩就越有效。

2.压缩
每个块在写入持久存储之前都会被单独压缩。压缩默认处于启用状态，因为默认压缩方法非常快，并且对于不可压缩的数据会自动禁用。在极少数情况下，应用程序可能希望完全禁用压缩，但只有在基准测试显示性能改进时才应该这样做：

leveldb::Options options;
options.compression = leveldb::kNoCompression;
... leveldb::DB::Open(options, name, ...) ....

3.缓存
数据库的内容存储在文件系统中的一组文件中，每个文件存储一系列压缩块。如果options.block_cache非NULL，则用于缓存经常使用的未压缩块内容。

#include "leveldb/cache.h"

leveldb::Options options;
options.block_cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB cache
leveldb::DB* db;
leveldb::DB::Open(options, name, &db);
... use the db ...
delete db
delete options.block_cache;
请注意，缓存保存未压缩的数据，因此应根据应用程序级别的数据大小调整其大小，而不会因压缩而减少任何大小。（压缩块的缓存留给操作系统缓冲区缓存，或客户端提供的任何自定义 Env 实现。）

当执行批量读取时，应用程序可能希望禁用缓存，以便批量读取处理的数据不会最终取代大部分缓存的内容。可以使用每个迭代器选项来实现此目的：

leveldb::ReadOptions options;
options.fill_cache = false;
leveldb::Iterator* it = db->NewIterator(options);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ...
}
delete it;

4.Key Layout
注意，磁盘传输和缓存的单位是块。相邻的键（根据数据库排序顺序）通常会放置在同一个块中。因此，应用程序可以通过将一起访问的密钥放置在彼此靠近的位置并将不常用的密钥放置在密钥空间的单独区域中来提高其性能。

例如，假设我们正在 leveldb 之上实现一个简单的文件系统。我们可能希望存储的条目类型是：

filename -> permission-bits, length, list of file_block_ids
file_block_id -> data
我们可能希望在文件名键前添加一个字母（例如“/”），并在键 file_block_id前添加一个不同的字母（例如“0”），这样仅扫描元数据就不会迫使我们获取和缓存大量文件内容。

5.过滤器
由于 leveldb 数据在磁盘上的组织方式，单个Get()调用可能涉及从磁盘进行多次读取。可选的 FilterPolicy 机制可用于大幅减少磁盘读取次数。

leveldb::Options options;
options.filter_policy = NewBloomFilterPolicy(10);
leveldb::DB* db;
leveldb::DB::Open(options, "/tmp/testdb", &db);
... use the database ...
delete db;
delete options.filter_policy;
前面的代码将基于布隆过滤器的过滤策略与数据库相关联。基于布隆过滤器的过滤依赖于在每个键的内存中保留一定数量的数据位（在本例中每个键 10 位，因为这是我们传递给的参数NewBloomFilterPolicy）。此过滤器会将 Get() 调用所需的不必要的磁盘读取次数减少大约 100 倍。增加每个键的位数将导致更大的减少，但代价是更多的内存使用。我们建议那些工作集不适合内存并且进行大量随机读取的应用程序设置过滤策略。

如果您使用自定义比较器，则应确保您使用的过滤策略与您的比较器兼容。例如，考虑一个在比较键时忽略尾随空格的比较器。 NewBloomFilterPolicy不得与此类比较器一起使用。相反，应用程序应该提供一个自定义过滤策略，该策略也忽略尾随空格。例如：

class CustomFilterPolicy : public leveldb::FilterPolicy {
 private:
  leveldb::FilterPolicy* builtin_policy_;

 public:
  CustomFilterPolicy() : builtin_policy_(leveldb::NewBloomFilterPolicy(10)) {}
  ~CustomFilterPolicy() { delete builtin_policy_; }

  const char* Name() const { return "IgnoreTrailingSpacesFilter"; }

  void CreateFilter(const leveldb::Slice* keys, int n, std::string* dst) const {
    // Use builtin bloom filter code after removing trailing spaces
    std::vector<leveldb::Slice> trimmed(n);
    for (int i = 0; i < n; i++) {
      trimmed[i] = RemoveTrailingSpaces(keys[i]);
    }
    builtin_policy_->CreateFilter(trimmed.data(), n, dst);
  }
};
高级应用程序可能提供不使用布隆过滤器但使用某些其他机制来汇总一组键的过滤策略。leveldb/filter_policy.h详情请参阅 。

十四.校验和

leveldb 将校验和与其存储在文件系统中的所有数据相关联。对于验证这些校验和的积极程度，提供了两种单独的控制：

ReadOptions::verify_checksums可以设置为 true 以强制对代表特定读取从文件系统读取的所有数据进行校验和验证。默认情况下，不进行此类验证。

Options::paranoid_checks可以在打开数据库之前设置为 true，以使数据库实现在检测到内部损坏时立即引发错误。根据数据库的哪个部分已损坏，打开数据库时或稍后由其他数据库操作可能会引发错误。默认情况下，偏执检查处于关闭状态，因此即使数据库的部分持久存储已损坏，也可以使用数据库。

如果数据库损坏（也许在打开偏执检查时无法打开），该leveldb::RepairDB功能可用于恢复尽可能多的数据

十五.Approximate Sizes

该GetApproximateSizes方法可用于获取一个或多个键范围使用的文件系统空间的大致字节数。

leveldb::Range ranges[2];
ranges[0] = leveldb::Range("a", "c");
ranges[1] = leveldb::Range("x", "z");
uint64_t sizes[2];
db->GetApproximateSizes(ranges, 2, sizes);
前面的调用将设置sizes[0]为键范围使用的文件系统空间的大约字节数[a..c)和sizes[1]键范围使用的大约字节数[x..z)。

十六.Environment

leveldb 实现发出的所有文件操作（以及其他操作系统调用）都通过leveldb::Env对象进行路由。经验丰富的客户可能希望提供自己的 Env 实现以获得更好的控制。例如，应用程序可能会在文件 IO 路径中引入人为延迟，以限制 leveldb 对系统中其他活动的影响。

class SlowEnv : public leveldb::Env {
  ... implementation of the Env interface ...
};

SlowEnv env;
leveldb::Options options;
options.env = &env;
Status s = leveldb::DB::Open(options, ...);

十七.Porting

leveldb 可以通过提供由 导出的类型/方法/函数的平台特定实现来移植到新平台 leveldb/port/port.h。请参阅leveldb/port/port_example.h了解更多详情。

此外，新平台可能需要新的默认leveldb::Env 实现。请leveldb/util/env_posix.h参阅示例。

十八.其他信息

有关 leveldb 实现的详细信息可以在以下文档中找到：

实施说明
不可变表文件的格式
日志文件的格式