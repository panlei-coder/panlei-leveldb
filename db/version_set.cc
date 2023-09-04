// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// 获取一个文件最大大小，这个大小是从Option实例中获取到的。
static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
// 计算level+2层级和此次进行compact的key值范围重叠的最大字节数。
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
// 所有压缩文件中的最大字节数。如果压缩的低级别文件集会使总压缩覆盖的字节数超过这么多，我们就避免扩展它。
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

// 计算每个层级最大可以有的字节数
static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  // 注意:level-0的结果并没有真正使用，因为我们根据文件数量设置了level-0的压缩阈值。
  // 返回level-0和level-1的结果
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

// 计算层级中每个文件的最大大小（每个层级包含多个文件）
static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

// 计算files动态数组中存放的文件元信息对应的文件总大小
static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

// 析构
Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  // 从versionSet链表中移除
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  // 文件解引用
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) { // 如果文件的引用<=0，则将文件删除
        delete f;
      }
    }
  }
}

/*
Level-0的文件需要单独处理，因为这个Level的.ldb文件间Key值的范围可能相交，多个.ldb文件都可能包含查找的Key-Value数据。
这时用线性搜索的方式逐一比对，可能包含Key-Value的多个.ldb文件，按照由新到旧（.ldb 文件 number 越大越新）的顺序在进一步处理。
其他Level的.ldb文件，Key值的范围彼此独立，最多只有一个.ldb文件包含查找的Key-Value数据。
这时用二分查找方式查找，加快查找效率。二分查找FindFile()如下，找到key属于哪个SSTable文件中

采用二分查找算法找到所存放的最大key值不小于参数key值的第一个文件元信息对象
在files数组中的下标。这里的"第一个"的意思就是值在files数组中所有最大key值
不小于参数key的元素中下标最小的那个。这个查找的结果也不一定就能说明参数key
一定会落在最终找到的那个文件中。我们知道虽然在一个层级(非0层)中所有文件的
key是有序的，递增的，但文件和文件之间的key值是不连续的，如下图所示：
|  files0 |    |  files1    |  | files2   | files3 |
----------------------------------------------------
假设上面的图中区间就是对应文件元信息存放的key值范围，从图中可以看到文件
和文件之间有可能存在部分key空间是不在两个文件中，如果某个key值不小于files2的
最大key值，而又大于files1的最大key值，那么FindFile()函数返回的文件元信息下标
就是图中files2对应的下标，但是不能说明参数key就一定会落在files2中，有可能会
在files1和files2之间那部分的空白key空间中。

即key落在哪个SSTable文件中
*/
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

// AfterFile()函数用于判断user_key是不是会落在文件元信息对象f对应的sstable文件后面，
// 换句话说就是判断user_key是不是比f对应的sstable文件的最大key值还要大，如果是，那么
// 就返回true;否则，返回false。
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

// BeforeFile()函数用于判断user_key是不是会落在文件元信息f对应的sstable文件前面，
// 换句话说就是判断user_key是不是比f对应的sstable文件中的最小key值还要小，如果是，那么
// 就返回true;否则，返回false。
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

// SomeFileOverlapsRange()函数用于判断files文件中是否有文件的key值范围
// 和[*smallest_user_key, *largest_user_key]有重叠，如果有的话，那么就返回
// true。
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  // disjoint_sorted_files为false的话，需要检查第0层的所有文件。
	// 而第0层的文件由于互相之间有重叠，所以需要检查所有文件。
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  // 因为从第1层开始，每一层的所有sstable文件的key是没有重叠的，所以
  // 可以用二分法来查找可能和[*smallest_user_key, *largest_user_key]有
  // 重叠的文件。
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    // 这里用smallest_user_key通过调用FindFile()到每一层的所有sstable中进行查找，
    // FindFile()会采用二分查找算法找到所存放的最大key值不小于smallest_user_key的
    // 第一个文件元信息对象。如果找到了这样的文件元信息对象，说明其对应的文件可能
    // 会和[*smallest_user_key, *largest_user_key]有重叠，但还需要进一步判断，即通过
    // 判断largest_user_key是否比该文件存放的最小key值还小，如果小的话，说明该文件
    // 其实和[*smallest_user_key, *largest_user_key]并没有重叠;如果大的话，说明
    // 该文件和[*smallest_user_key, *largest_user_key]是有重叠部分的。举例如下：
    //
    // |  files0 |    |  files1    |  | files2   | files3 |
    // ----------------------------------------------------
    // 假设通过FindFile()找到的文件元信息对象是files2的，那么[*smallest_user_key,
    // *largest_user_key]范围此时可能在(files1.max_key, files2.max_key]之间，所以
    // 需要进一步看是否[*smallest_user_key, *largest_user_key]会落在(files1.max_key,
    // files2.min_key)之间，如果是的话，说明其实并没有重叠。
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // 注意这里还需要判断最大值是否小于files[index]的最小值，
  // 因为files[index-1].largest <*smallest_user_key <= files[index].largest，
  // 并不能满足[*smallest_user_key, *largest_user_key]与files[index]有重叠
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
// Version::LevelFileNumIterator类是一个迭代器的实现类，用于迭代一个
// 存放这文件元信息对象的动态数组，在实际使用的时候通常用于迭代某个
// level中的所有sstable对应的文件元信息对象。
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }

  // Seek()接口用于使迭代器指向所存放的最大key值大于等于target的
	// 第一个文件元信息对象，这里的"第一个"的意思就是值在files数组中
	// 所有最大key值不小于target的元素中下标最小的那个。
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }

  // SeekToFirst()使迭代器指向flist_数组中的第一个文件元信息对象。
  void SeekToFirst() override { index_ = 0; }

  // SeekToLast()使迭代器指向flist_数组中的最后一个文件元信息对象。
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }

  // Next()使迭代器指向flist_数组中的下一个文件元信息对象，具体做法
	// 就是将数组的索引加1.
  void Next() override {
    assert(Valid());
    index_++;
  }

  // Prev()使迭代器指向flist_数组中的前一个文件元信息对象，具体做法就是
	// 将数组索引减1.如果在减1之前就已经指向第一个文件元信息对象的话，那么
	// 执行Prev()就应该是一个无效的迭代器了，所以这里将索引设置成flist_->size()。
	// 因为索引的有效范围是[0, flist_->size() -1]。
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }

  // key()方法用于获取迭代器指向的文件元信息对象中存放的最大key值。
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }

  // value()方法用于获取迭代器指向的文件元信息对象中存放的FileNumber和
	// file size，并编码在一个16字节的字符数组中
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }

  // Status()方法用于获取迭代器当前的状态，这里返回的是OK状态。
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_; // InternalKey的比较器
  const std::vector<FileMetaData*>* const flist_; // 存放文件元信息对象的动态数组
  uint32_t index_; // 存放文件元信息的动态数组的索引

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16]; // 用于存放获取SSTable文件中的key-value信息，其实这个迭代器本身也是一个二级迭代器
};

// GetFileIterator()函数用于获取某个指定sstable文件的迭代器，这个迭代器
// 可以用于获取sstable文件中的key-value信息，其实这个迭代器本身也是一个
// 二级迭代器。
// 迭代某个具体的SSTable文件
static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  // arg参数存放的是上层调用者传入的参数，在这里是一个TableCache类实例，
	// 可以参考下面的Version::NewConcatentingIterator()方法。
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) { 
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    // file_value中存放了目标sstable文件的FileNumber和file size，这两个信息
		// 在LevelFileNumberIterator迭代器的value()方法中可以获取到。然后根据
		// 这两个信息可以获取到TableCache类实例的迭代器
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

// NewConcatenatingIterator()方法用于获取一个二级迭代器，这个二级迭代器可以用于
// 迭代器某一个层级(level>0)中的所有sstable，并从中获取到key-value信息。当然key需要
// 由上层调用者传入，否则内部也不知道上层调用者需要Get什么key的value。
// LevelFileNumIterator迭代指定level的所有SSTable
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

// AddIterators()方法用于某个版本(Version)中所有sstable的迭代器，这样就可以
// 通过获取到的迭代器数组依次获取这个版本中的key-value信息。我们知道，level0
// 中的sstable之间可能存在key重叠的情况，所以对level0中的所有sstable文件作为
// 独立个体分别创建一个迭代器。而其余level在同level的sstable文件之间是不存在
// key重叠的，所以可以通过NewConcatenatingIterator来创建一个对于整个level的
// 迭代器。
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  // 合并所有零级文件，因为它们可能重叠
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  // 对于levels > 0，我们可以使用串联迭代器，顺序遍历level中不重叠的文件，惰性地打开它们。
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
// SaverState枚举用来标识查询key的结果状态。
enum SaverState {
  kNotFound, // 没找到
  kFound, // 找到了
  kDeleted, // 删除了
  kCorrupt, // 发生了异常了
};

// struct Saver结构体用来保存user_key对应的value信息。
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace

// SaveValue()函数一般用作TableCache::Get()方法的最后一个参数，用于保存
// 从sstable中查找user_key的结果。
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg); // 结果最终保存在arg中
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    // 如果parsed_key.user_key等于s->user_key，说明找到了这个key记录
  	// 然后根据parsed_key.type判断值是否有效，有效的话，就保存到s->value中。
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

// 比较两文件的编号哪个更新
static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

// ForEachOverlapping()方法用于对user_key所落在的sstable文件对应的文件元信息对象
// 执行一个func操作，并根据函数func的返回值判断是否需要对user_key所落在的其他
// 文件元信息对象执行相同的操作，如果不需要，则直接返回。
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  // 首先是从0层(level 0)开始，由于level 0中的sstable文件之间可能存在key重叠，
  // 所以在处理level 0的时候需要遍历该层中的所有文件，将包含了user_key的所有
  // sstable文件对应的文件元信息对象收集起来。然后对于目标集合，依次调用func
  // 并根据func的返回值判断是否需要返回。
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }

  // 不为空，则对这些file先按照文件编号从大到小进行排序（编号越大，说明文件越新）
  // 排序后依次执行func函数，如果返回值为false，直接结束
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  // 如果在level 0中没有包含了user_key的sstable，或者对于找到的满足条件的
  // sstable文件，func返回值并没有说要提前返回，那么就会继续搜寻更高层的
  // sstable文件，由于更高层的sstable文件同层级之间不存在key重叠，所以
  // 一个层级如果存在包含了user_key的文件的话，那么这个文件也是唯一的。
  // 那么就对这个文件对应的文件元信息对象执行func操作，并根据返回值判断
  // 是否需要从更高层中继续搜寻。以此类推。
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

// Get()方法用于从当前版本中获取key值k对应的value值，并设置访问统计。
// 查找时逐级搜索，如果在较低Level查找到，则以后的Level数据无关紧要，因为低Level的数据是最新的数据。
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  // 初始化访问对象
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  
  struct State {
    Saver saver; // 用于保存找到指定的key-value
    GetStats* stats; // 获取到文件的状态信息
    const ReadOptions* options; // 读取时的相关配置信息
    Slice ikey; // InternalKey
    FileMetaData* last_file_read; // 上一次读取的文件元信息
    int last_file_read_level; // 上一次读取的文件所处的level

    VersionSet* vset;
    Status s; 
    bool found; // 标记是否找到了指定的key

    // 在找到的SSTable文件中匹配指定的key（arg）
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      // 如果last_file_read不为NULL，说明之前已经搜寻过了一个或者多个sstable文件，
			// 但是都没有找到符合条件的key-value记录。如果stats->seek_file等于NULL,说明
			// 在之前的搜寻过程中并没有设置过访问统计。这两个条件结合在一起是为了约束
			// 这样的一个条件，即对于本次的读操作(Get)，已经执行了多次的查询，即搜寻了
			// 多个sstable文件，在这样的情况下，需要保存本次的读操作中被查询的第一个文件
			// 及其所在的level。
      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      // 更新state的last_file_read和last_file_read_level
      state->last_file_read = f;
      state->last_file_read_level = level;

      // 调用TableCache类实例的Get()方法执行查询动作，如果找到了对应的key-value
			// 记录，那么就可以直接返回了。对于level 0来说，因为所有符合条件的sstable
			// 文件已经按照FileNumber进行排序，所以如果在FileNumber大(即新的)sstable
			// 文件中找到了记录，那么就不再从更旧的sstable文件中查找了，直接返回。
      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }

      // 判断是否找到了指定的key-value
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      // 没有达到。添加以避免控制到达非空函数结束时的错误编译警告。
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

// UpdateStats()方法用于根据访问统计判断是否需要更新下一次compact的文件和level。
bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;// f允许在进行compact前最多被访问的次数递减一次。
    // 如果f允许在进行compact前最多被访问的次数小于等于0，并且之前没有设置
    // 下一次compact的文件的话，那么就将f设置为下次进行compact的文件，并将
    // 该文件所在的level设置为下次进行compact的level。
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

// 找到包含指定internal_key的至少两个SSTable文件
bool Version::RecordReadSample(Slice internal_key) {
  // 调用ParseInternalKey()从internal key中解析出user key、sequence number和type，
	// 并存放到ParsedInternalKey对象ikey中。
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  // struct State是一个统计类，通过函数Match来对匹配的文件元信息对象做一些访问统计。
	// 这里的说的匹配可以是对于某个key来说，如果某个sstable包含了这个key，那么我们
	// 就说匹配了。
  struct State {
    GetStats stats;  // Holds first matching file 记录第一个匹配的文件信息
    int matches; // 统计匹配对象的个数

    // Match()方法用于对匹配的文件元信息对象做进一步处理，包括：
		// 1. 统计匹配的文件元信息对象个数。
		// 2. 保存第一个匹配的文件元信息对象及其对应的sstable文件所在的level。
		// 3. 返回匹配的文件元信息对象个数是否小于2的逻辑结果。调用Match()函数
		//    的地方会根据Match()函数的返回值来判断是否需要对其他匹配的文件元信息
		//    对象再调用Match()函数做处理，如果已经有两个文件元信息对象已经匹配了，
		//    那么就不再对其他匹配的文件元信息对象做处理了。
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      // 一旦有了第二个匹配，我们就可以停止迭代了。（注意：这里是找到了两个之后才不会继续找）
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  // ForEachOverlapping()方法用于对user_key所落在的sstable文件对应的文件元信息对象
	// 执行一个State::Match操作，并根据该函数的返回值判断是否需要对user_key所落在的其他
	// 文件元信息对象执行相同的操作，如果不需要，则直接返回。
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  // 必须至少有两个匹配项，因为我们要跨文件合并。但是，如果我们有一个包含许多覆盖和删除的单一文件呢
  // 如果state.matches >= 2，说明对于user_key来说，至少有两个sstable文件包含了这个key
  // 那么就调用UpdateStats()方法用于根据访问统计判断是否需要更新下一次compact的文件和level
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

// 增加计数
void Version::Ref() { ++refs_; }

//  减少计数，当引用为0时，直接删除当前version对象
void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

// SomeFileOverlapsRange()函数用于判断files文件中是否有文件的key值范围
// 和[*smallest_user_key, *largest_user_key]有重叠，如果有的话，那么就返回
// true。那么OverlapInLevel()方法的用途就是判断level层所在的sstable文件中
// 是否有文件的key值范围和[*smallest_user_key, *largest_user_key]有重叠，
// 如果有的话，就返回true；否则返回false。
bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

// PickLevelForMemTableOutput()方法用于给memtable选择一个合适的level
// 作为memtable下沉为sstable的目标层数。
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;

  // 首选判断在第0层是否有sstable文件和[smallest_user_key,largest_user_key]
 // 有重叠，如果有的话，那么就将第0层作为memtable下沉为sstable文件的目标层数。
 // 如果第0层没有的话，那么选择的依据有两个，假定目前迭代的层数为level(仍从0开始)：
 // 1. 如果该层的下一层(level + 1)中的sstable文件key值范围有和[smallest_user_key,
 //    largest_user_key]重叠，那么level层就作为目标层数。
 // 2. 如果第一个条件没有符合，但是该层的下两层(level + 2)中和[smallest_user_key,
 //    largest_user_key]key值范围有重叠的sstable文件的总大小大于下两层最大重叠字节数的
 //    话，那么也将level层作为目标层数。
 /*
 @todo 针对第二种情况是基于什么理由这样做呢 ？？？
1. 大于 level 0的各层文件间是有序的，如果放到对应的层数会导致文件间不严格有序，会影响读取，则不再尝试。
2. 如果[smallest_user_key,largest_user_key]与下两层level+2的重叠数据很大，那么将其放入到level层，使得其能够与其他SSTable合并了之后再与level+2层合并，减少冗余的存储空间占用，有助于提高整体存储空间的利用。
3. 最大返回 level 2，这应该是个经验值。
 */
 // 上面的处理流程，对于等0层来说，如果该层中有sstable文件的key值范围和目标范围重叠，
 // 或者该层本身没有sstable文件的key值范围和目标范围重叠，但是下一层或者下两层的sstable
 // 满足上面的条件，都会将第0层作为目标层数。
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) { // 没有重叠
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    // 如果下一层没有重叠，就推到下一层
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) { // 下一层有重叠
        break;
      }
      if (level + 2 < config::kNumLevels) { // 下两层
        // Check that file does not overlap too many grandparent bytes.
        // 检查文件是否重叠太多的祖父字节
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) { // 如果重叠超过了level与level+2层合并时的最大重叠字节数
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// GetOverlappingInputs()用于将level层的所有和key值范围[begin,end]有
// 重叠的sstable文件对应的文件元信息对象收集起来存放到inputs数组中。
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();

 // 遍历level层的所有sstable文件元信息对象，对于每一个文件元信息对象，
 // 获取到其中存放的最大和最小key值。如果最大的key值都比begin小，那么
 // 这个sstable肯定不会和[begin,end]有重叠；如果最小的key值都比end
 // 大，那么这个sstable肯定不会和[begin,end]有重叠；否则，这个sstable
 // 文件的key值返回和[begin,end]就会有重叠，保存这个sstable文件对应的
 // 文件元信息对象。
 // 对于第0层来说比较特殊，因为第0层的sstable文件的key值范围可能会互相
 // 重叠，这个时候如果某个sstable的最小key值比begin小的话，那么就用这个
 // 最小key值作为begin，然后清除已经收集的文件元信息对象，并从该层的第一
 // 个文件开始重新判断是否和这个新key值范围[begin(new),end]有重叠；如果
 // 某个sstable文件的最大key值比end大的话，那么就用这个最大key值作为end，
 // 然后清除已经收集的文件元信息对象，并从该层的第一个文件开始重新判断是否
 // 和这个新的key值范围有重叠。这样做的结果就是可能有一些和最原始的[begin,end]
 // 没有重叠的sstable文件对应的文件元信息对象也加入到了数组中。

  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        // 因为f文件已经被添加到了有重叠的文件中，而level+0层的SSTable是有重叠的，所以当该SSTable文件被compact之后，
        // 可能其他与该SSTable有重叠的文件就会被访问到，可能会违反数据一致性的要求，故这里修改begin/end来将它们都添加到有重叠的集合中
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

// DebugString()方法用于打印出Version类实例的信息，用于调试。
std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
// 一个辅助类，这样我们就可以有效地将整个编辑序列应用于特定状态，而无需创建包含中间状态完整副本的中间版本。
/*
VersionSet::Builder的作用就是直接把所有的VersionEdit，Apply到VersionSet上面。最后形成一个version。中间过程中的各种Version就没有必要保留了。
Version与Builder的不一样的地方：Version是一个静态视角。所以只需要记住当前有的那些文件就可以了。而Builder是一个Action，或者说VersionEdit也是一个Action。动作则是会删除文件的。
假设Base有的文件为：
level0: 1
level1: 8
level2: 13, 14
builder经过多轮record(VersionEdit)累加之后，得到的一个总的操作如下：
level0: 添加文件2,3, 删除文件 1
level1: 添加文件9,
level2: 添加文件10,11,12,17 删除文件14
那么如果把总的操作作用在Base Version上，操作顺序如下：
level0:
    added_file = 2
        为了保证文件的顺序性。就是v_->files数组里面的元素都是有序的。
        这个时候，就把小于2的文件，就call MaybeAddFile(1)。
            MaybeAddFile发现1是在删除列表里面。就跳过了。
    added_file = 3
        这个时候，base->files[level0]里面已经没有数据了。
        直接尝试MaybeAddFile(3)
level1:
    Added_file = 9
        为了保证文件的顺序性，先把所有小于9的base里面的文件调用一下
        MaybeAddFile也就是call MaybeAddFile(8)

按照这个顺序合并，并且结果是保存在version里面。
参考链接：https://zhuanlan.zhihu.com/p/35275467#Saveto
*/
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  // 比较器结构体，实现辅助实现排序
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;  // internal_key的比较器

    // 实现比较两个FileMetaData类实例的大小。如果key值相同，那么比较两个的FileNumber
    // @todo 为什么比较的是两个文件的最小值？？？
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        // 在smallest key相同的情况下，FileNumber大的那个文件比
        // FileNumber小的文件要“大”
        return (f1->number < f2->number);
      }
    }
  };

  // 定义存放FileMetaData *对象的集合类型，其中比较器采用BySmallestKey。
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  // 要添加和删除的sstable文件集合，其中要删除的sstable文件集合中存放的是文件
  // 对应的FileNumber，而要添加的文件集合中存放的则是文件对应的FileMetaData
  // 类实例信息。
  struct LevelState { // level的状态
    std::set<uint64_t> deleted_files; // 要删除的文件
    FileSet* added_files; // 要添加的文件
  };

  // vset_用于存放VersionSet实例
  VersionSet* vset_;
  // base_用于存放Version实例，这里的上下文含义就是基准版本。
  Version* base_;
  // 各个level上面要更新（添加和删除）的文件集合。
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  // 将VersionEdit对象edit中的信息应用到VersionSet::Builder类实例中，换句话说
  // 就是将版本变动信息添加到VersionSet::Builder类实例中，后面构建新的Version
  // 的时候要用到。
  void Apply(const VersionEdit* edit) {
    // Update compaction pointers
    // 为了尽量均匀compact每个层级，所以会将这一次compact的end-key作为下一次
    // compact的start-key，edit->compact_pointers_就保存了每一个level下一次compact的
    // start-key，这里的操作就是将edit中保存的compact pointer信息添加到对应的
    // VersionSet实例中。
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    // 每次version edit都会有需要删除的文件。
    // 比如version0要删除level0_a,
    // version1要删除level1_a,
    // version2要删除level2_a
    // 将edit中存放的要删除的文件信息添加到levels_的deleted_files中。
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    // 将edit中存放的要新增的文件信息添加到levels_的added_files中
    // 如果这个文件还在deleted files里面。那么就需要删除之。
    // Q: 按理说序号是不断递增的。一个版本要删除的文件，不应该再次added_files．
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      // 我们设置在一定次数的查找后自动压缩文件。我们假设:
      // (1)一次寻道耗时10ms
      // (2)写入或读取1MB花费10ms (100MB/s)
      // (3) 1MB的压缩会产生25MB的IO:
      // 从这个级别读取1MB
      // 从下一层读取10-12MB(边界可能不对齐)
      // 10-12MB写入到下一级
      // 这意味着25次查找的开销与1MB数据的压缩开销相同。也就是说，一次寻道的开销与压缩40KB数据的开销大致相同。
      // 我们有点保守，在触发压缩之前，大约允许对每16KB的数据进行一次寻道。
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;
       
       // 从删除集合中剔除被添加的文件
       // @todo 为什么这里是现将VersionEdit要删除的文件添加到deleted_files中，然后再处理要新添加的文件时，
       // 如果该文件已经存在于deleted_files，先将其删除，再添加到added_files中，这里这样处理的原因是什么？
       // 答：理论上来说，先前被删除的文件是不会出现在需要新添加的文件中的，因为文件的序列号是不断递增的，不应该出现这种情况
      levels_[level].deleted_files.erase(f->number);  // 从删除集合中剔除被添加的文件
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  // 用VersionSet::Builder实例中存放的基准版本信息以及从VersionEdit实例中获取到的
  // 版本变动信息一起构造出一个新的Version实例。
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_; // 设置InternalComparator，用于对sstable文件中的key进行比较。
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      // 获取基准版本base_中的level层的所有sstable文件
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      // 获取在level层要新增的文件集合，集合中的内容是从VersionEdit类实例中获取到
      // 获取过程是在Apply()方法中实现的。
      const FileSet* added_files = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added_files->size());

      // 下面的循环处理过程所做的事情就是将基准版本中level层的sstable文件和从VersionEdit
      // 类实例中获取到的新版本中要新增的sstable文件放在一起，共同组成新版本level层的
      // sstable文件集合。两者组合的规则如下：
      // 对于added集合中的每一个文件，先将base_files数组中小于该文件的所有文件添加到
      // 新版中中，然后将该文件添加到新版本中。这一过程持续到added集合中的所有文件
      // 都处理完毕，added集合中的文件处理完毕之后，可能base_files数组中可能还有部分文件
      // 还没有添加到新版本中，所以最后要将这部分文件也一并添加到新版本中。最后添加的
      // 这部分文件(不一定有)有一个特点就是这部分文件比added集合中所有文件都要"大"，当然
      // 这个"大"是根据FileMetaData * 对象的比较器来说的，比较器实现可以参考BySmallestKey。
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp); // 返回第一个大于added_file的迭代器
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

// 非debug模式下
#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      // 确保大于0层的SSTable文件中没有重叠(当前层的SSTable是按序排列的，所以比较前一个最大值和后一个最小值即可检查出来)
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  // MaybeAddFile()用于判断是否需要往Version实例v的level层中添加文件元信息对象f
  // 如果文件元信息对象f已经在要删除的文件集合中，那就不往Version实例中添加这个
  // 文件元信息对象了。否则的话，就将其添加到Version实例v的level层sstable文件vector中
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {  // 这里检查的是要添加的文件是否存在于要删除的文件中，如果要删除则不添加到v中
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

// 将v添加到VersionSet链表中，并令current_指向当前最新的版本，即v
void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  // 将v标记成current
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

// LogAndApply()函数是将edit应用到当前的最新版本，生成新的版本添加到VersionSet中，并生成日志，记录SSTable文件的变化。
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  // 1.主要检测log_number_、next_file_number_、prev_log_number_、last_sequence_，
  // 若没有则设置
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  // 2.创建新的版本
  // 2.1.将edit version应用到当前VersionSet中
  // （1）更新每一个level的compact_pointers；
  // （2）记录下已删除的文件；
  // （3）记录下要新添加的文件；
  // （4）对新添加文件可seek次数进行设置；
  // 2.2.将1流程合并之后的文件更新到新Version中，同时对大于level-0层文件进行排序，
  // 对于大于level-0层文件进行排序，对于level-0层文件不用管
  // 2.3.对新Version，计算下每层文件的compaction score,便于后续进行的compaction操作
  // （1）level-0层根据文件个数计算score
  // （2）非level-0层，根据每层所有文件大小来计算score
  Version* v = new Version(this);
  {
    Builder builder(this, current_); // 基于当前的最新版本，生成builder
    builder.Apply(edit); // 将edit应用到builder中
    builder.SaveTo(v); // 将builder的内容保存到新版本v中
  }

  // 计算这个新version的compact score和compact level
  Finalize(v);

  // 3.将这次的版本变化信息添加到manifest文件中，便于后续的恢复
  // （1）如果是第一次写manifest文件，需要保存当前db的完整信息，通过VersionSet::WriteSnapShot()来实现，
  // 具体内容为：1）创建一个VersionEdit；2）填入comparatorName；3）填入每一层的compact pointer；4）记录每层的文件元信息。
  // （2）VersionEdit写入到manifest文件中
  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  // 如果需要，通过创建包含当前版本快照的临时文件来初始化新的日志文件描述符。
  std::string new_manifest_file; // 创建新的manifest文件
  Status s;
  if (descriptor_log_ == nullptr) {
    // 这里没有必要进行unlock操作，因为只有在第一次调用，也就是打开数据库的时候
    // 才会走到这个路径里面来
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);  // 获取新的文件描述符
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_); // 创建新的文件
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_); // 写当前的快照
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      // 保存这次的改动到manifest文件中
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  // 4.将最新的Version加入到VersionEdit所管理的Version双向循环链表中
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

// Recover()函数是将levelDB启动后恢复到上一次关闭前的状态。
Status VersionSet::Recover(bool* save_manifest) {
   // report出错信息
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  // 1.通过去读CURRENT文件内容，打开最新的manifest文件
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file); // 创建顺序读取manifest文件的句柄
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  // 2.顺序读取manifest文件，并将读取到的record解码成VersionEdit，依次应用到最新的版本中，实现对levelDB的恢复操作
  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    
    while (reader.ReadRecord(&record, &scratch) && s.ok()) { // 读取存储的一条记录
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) { // 读取的comparator和当前不一样则输出错误信息
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      // 将读取到的VersionEdit内容合并到Build类中，用于生成最新的Version
      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  // 3.将包含所有的VersionEdit变化内容的Build数据SaveTo到最新的Version中，然后将最新的Version添加到VersionSet维护的双向循环链表中
  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    // 判断是否要重新生成manifest文件
    // 由外层重新调用LogAndApply()去生成新的manifest文件，
    // 在进行Recover过程中，已经将新增文件保存在了Current Version中，
    // 当确定需要重新生成新的manifest文件时，LogAndApply会走对应的流程重新生成
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

// ReuseManifest()函数判断是否重复使用manifest文件
bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) { // 如果manifest文件超过了最大文件大小（默认为2MB），则需要生成新的manifest
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  // 如果manifest文件打开失效，则也需要重新生成manifest文件
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

// MarkFileNumberUsed()函数用于标记下一个文件的编号
void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

/*
Finalize()函数用于计算这个新version的compact score和compact level，
预计算下一个compact的最佳层（compaction_score_最大的层）
*/
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  // 遍历所有的层次计算最佳score和level
  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) { // 将当前level-0包含的文件个数除以4并赋值给compaction_score_，如果level-0的文件个数大于4，则此时compaction_score_会大于等于1
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      // 对0级进行特殊处理，使用了文件数量而不是文件大小，是出于两个考虑：
      // (1)在有更大的写缓存大小的情况下，不会做很多0级的compact操作。
      // (2)每次读操作都可能触发0级文件的合并操作，所以我们更希望能避免在0级出现
      // 很多文件，而且每个文件的大小还很小的情况（可能是因为小的写缓存设置，或者
      // 高压缩比例，也或者是很多覆盖写、删除操作，等等原因造成的0级别的小文件）
      // level 0是根据这个级别的文件数量来计算分数
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else { // 将该层文件的总大小除以该层文件允许的最大大小并赋值给compaction_score_
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    if (score > best_score) {// 找出compaction_score_最大的层
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

// WriteSnapshot()函数用于生成当前的快照
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  // 首先写compactor name
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  // 其次保存每个level的compact point
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  // 最后保存每一级别的每个文件的文件大小,最大/最小 key
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

// NumLevelFiles()函数将返回某个级别文件数量
int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

// ApproximateOffsetOf()用来估算某个key的偏移量
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) { // 遍历每一层，直接找到了ikey
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) { // largest <= ikey
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) { // smallest > ikey
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;  // ikey不在当前层中
        }
      } else { // ikey在[smallest,largest]范围内
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        // 找到对应的SSTable文件，并创建迭代器来估算ikey大致的偏移量
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

// AddLiveFiles()函数统计VersionSet中所有处于活动状态的SSTable文件
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

// NumLevelBytes()函数统计指定的level层文件的bytes
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

// MaxNextLevelOverlappingBytes()返回下一个级别中有重叠的最大数量，即level+i层的文件f与level+i+1层之间的重叠数量，找到这个i和f
// 对level-0层有特殊的处理，在函数GetOverlappingInputs中
int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) { // 1到5层
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      // 拿到下一个级别针对该该文件有覆盖的文件集合返回到overlaps参数中
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
// 找到inputs里面的最大值和最小值存储在smallest和largest中返回
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
// 计算两个集合中的最大最小值
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

/*
创建多层合并迭代器，用于Compaction后的数据进行merge。
- 第0层需要互相merge，因此可能涉及到文件有多个
- 其他层，只需要创建一个即可
*/
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  // 如果是0级文件,需要把这里面的所有0级文件存起来,所以取inputs_[0].size() + 1
  // 对于其他级别,level和level+1各一个文件就好了
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2); // 第0层每一个SSTable都需要一个
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) { // 第0层
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else { // 其他层
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

// PickCompaction()函数选择compact文件
/*
1、优先判断第一种策略（size_compaction）Version中的变量compaction_level_决定此次compaction操作的层级，
然后通过VersionSet中的compact_pointer_决定此次level n层需要参与的文件，并且将文件写入inputs_[0]变量。
每次执行完compaction操作后会在VersionSet中的compact_pointer_记录本次参与的最大键，
便于下次Compaction操作时选取该最大键之后的文件。因此compact_pointer_相当于一个游标，据此可以遍历一个
层级的键空间。如果没有该游标或者已经遍历完该层的键空间，则会选择level n中的第一个文件。
2、如果第一种策略不满足，则判断第二种策略（seek_compaction）。Version中的变量file_to_compact_level_
决定此次Compaction操作的层级，变量file_to_compact_就是level n层参与的文件。
注意，如果进行的是一个从level-0到level-1的Compaction操作，则level-0层文件的选取略有不同，由于level-0中
不同文件的键范围有可能重叠，因此需要进行特殊选取。
（1）取出level-0中参与本次Compaction操作的文件的最小键和最大键，假设其范围为[Lkey,Hkey]。
（2）根据最小键和最大键对比level-0中的所有文件，如果存在文件与[Lkey,Hkey]有重叠，则扩大最小键和最大键范围，继续查找。
因为可能会因为某个SSTable文件被Compaction操作之后，导致其他的SSTable文件也包含某个相同的值被访问到，导致不一致的结果。
*/
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1); // 判断是否通过第一种策略（size_compaction）策略触发
  const bool seek_compaction = (current_->file_to_compact_ != nullptr); // 判断是否通过第二种策略（seek_compaction）策略触发
  if (size_compaction) { // 优先选取size_compaction
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    // 选出第一个在compact_pointer之后的文件（compact_pointer记录每个层级上次Compaction操作的最大值）
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    // 如果前面的循环找不到,说明已经找了该级别的所有文件了,就把该级别的第一个文件push进去
    // 也就是回绕回去(wrap-aound)
    // @todo 来说似乎可能没办法依次取出每一个，因为它们之间会存在重叠 ？
    if (c->inputs_[0].empty()) { 
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) { // 如果是因为seek_compaction触发，则直接将allowed_seek超出限制的文件选定为本次进行Compaction操作的level n层文件
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level); 
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_; // 将Compaction实例的input_version_设置为当前版本
  c->input_version_->Ref(); // 将进行此次Compaction操作的版本加1次引用计数

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  // level-0层的特殊处理逻辑
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest); // 取出inputs_[0]所有文件中的最大值和最小值，并分别赋值到largest/smallest
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    // 用于将level层的所有和key值范围[begin,end]有
   // 重叠的sstable文件对应的文件元信息对象收集起来存放到inputs数组中。
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

   // 选取level n+1层需要参与的文件且放到inputs_[1]中
  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns true if files is not
// empty.
// FindLargestKey()函数将查找文件集合中的最大键。如果files不为空则返回true。
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
// FindSmallestBoundaryFile()函数在l2 > u1且user_key(l2) = user_key(u1)的级别文件中查找最小文件b2=(l2, u2)
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
// 从|compaction_files|中提取最大的文件b1，然后在|level_files|中搜索user_key(u1) = user_key(l2)的b2。
// 如果它找到这样的文件b2(称为边界文件)，它将其添加到|compaction_files|，然后使用这个新的上界再次搜索。
// 如果有两个块，b1=(l1, u1)和b2=(l2, u2)和user_key(u1) = user_key(l2)，
// 如果我们压缩b1但不压缩b2，那么后续的get操作将产生一个错误的结果，因为它将返回第一级b2的记录，
// 而不是b1的记录，因为它逐级搜索与提供的用户键匹配的记录。
//（注：internalkey对于user_key相同时，按照sequence降序排列，sequence越大，排在前面，更容易被找到）
// 参数:
// 在level_files中:搜索边界文件的文件列表。
// in/out compaction_files:通过添加边界文件扩展的文件列表。
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      // 相当于这个最小的边界文件下界是与要compact的上界包含相同的user_key，本应该在被compact的文件中找到，
      // 但是由于compact了之后，会在最小的边界文件中找到，导致不一致性。
      compaction_files->push_back(smallest_boundary_file); 
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

// SetupOtherInputs()函数进行level n+1层文件的选取
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]); // 扩展第level n文件的最小边界文件，添加到inputs_[0]中
  GetRange(c->inputs_[0], &smallest, &largest); // 获取所有文件的最大和最小值

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]); // 通过inputs_[0]的最大和最小值查找level n+1层，并赋值给inputs_[1]中
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]); // 扩展第level n+1文件的最小边界文件，添加到inputs_[1]中

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);  // 获取inputs_[0]和inputs_[1]所有文件的最大和最小值

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  // 看是否能在不改变前面已经取出的level n+1文件数量的条件下
  // 增加level n级别文件的数量，并且扩大之后参与的文件总大小不能超过50MB
  // 看能否将level中与取出的level+1中的range重叠的也加到inputs中，
  // 而新加的文件的range都在已经加入的level+1的文件的范围中
  /*
  具体的逻辑如下：
  1）inputs_[1]选取完毕之后，首先计算inputs_[0]和inputs_[1]所有文件的最大、最小值范围，
    然后通过该范围重新去level n层计算inputs_[0]，此时可以有可能会选取到新的文件
  2）通过新的inputs_[0]的键范围重新选取inputs_[1]中的文件，如果inpust_[1]中的文件个数不变，
    并且扩大范围后所有文件的总大小不超过50MB，则使用新的inputs_[0]进行本次的
    compaction操作，否则继续使用原来的inputs_[0]。50MB的限制是为了防止执行一次
    compaction操作导致大量的I/O操作，从而影响系统性能。
  3）如果扩大level n层的文件个数之后导致level n+1层的文件个数也进行了扩大，则不能进行此次优化。
    因为level 1到level 6的所有文件键范围是不能有重叠的，如果继续执行该优化，会导致compaction
    之后level n+1层的文件有键重叠的情况。
  @todo 为什么一定要保证level n+1层的文件个数保持不变，为什么不能使用level n+1层扩大之后的文件集合进行compact操作呢 ？？
  画图进行了理论上的测试，似乎并不影响正确性，可能是为了简化操作还是有别的原因。
  */
  if (!c->inputs_[1].empty()) { // levelDB优化逻辑，在不扩大level n+1层文件个数的情况下，将level n层的文件个数扩大
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  // 需要计算压缩之后，与grandparents_重叠的文件
  // 获取出level+2层与压缩后的level+1层(这里指的是level 与level+1合并压缩放入
  // 到level+1中的文件，不包括level+1的所有)有重叠的文件放入到grandparents_,
  // 作用就是当合并level+1与level+2时，根据grandparents_中的记录可进行提前结束，
  // 不至于合并压力太大。（不是停止合并，后文的压缩是停止当前的SSTable压缩，新生成一个新的SSTable进行压缩）
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  // 更新下一个压缩的文件范围，便于下一次查找压缩的起始key
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

// CompactRange()函数用于手动触发压缩
Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  // 获取所有与指定范围的key有重叠的所有文件
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  // 需要避免在一次操作中要参与压缩的文件太多
  // 但是针对第0层文件不做处理，因为第0层文件之间本来就会有重叠
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

// IsTrivialMove()函数返回true表示不需要合并，只要移动文件到上一层就好了
bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // 1.level n层和level n+1层没有重叠
  // 2.level n层和grandparents_重叠度小于阈值（避免后面到level n+1层时和level n+2层之间的重叠度太大）
  // 3.level n层必须只有一个sst文件
  return (num_input_files(0) == 1 && num_input_files(1) == 0 && // level 只有一个文件 且 level + 1没有文件
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_)); // 有重叠的爷爷辈文件大小之和小于阈值
}

// AddInputDeletions()函数将所有需要删除SST文件添加到*edit。因为input经过变化生成output，
// 因此input对应到deleted_file容器，output进入added_file容器。
// 需要注意在前面在add的时候，先忽略掉deleted里面的，因为IsTrivialMove是直接移动文件，到处都是细节。
void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

// IsBaseLevelForKey()函数检查在level + 2及更高的等级上,有没有找到user_key
// 如果都没有找到，说明这个级别就是针对这个key的base level。
// 主要是用于key的type=deletion时可不可以将该key删除掉。
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++; // 每次检查完毕，都将这个计数器递增，这样下一次查找就从上一次停止查找的位置开始继续查找
    }
  }
  return true;
}

// ShouldStopBefore()函数判断这个key的加入会不会使得当前output的sstable和grantparents有太多的overlap
// 为了避免合并到level+1层之后和level+2层重叠太多，导致下次合并level+1时候时间太久，因此需要及时停止输出，并生成新的SST
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  // 寻找爷爷辈级别的文件中含有这个key的最小文件
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,  // 当传入的internal_key一直大于该文件的最大key时这个循环一直下去
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {  // 如果之前已经看到这个key了,那要累加overlap范围的大小
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  // 第一次进来前为false，进来之后该函数就会置为true，第二次以后进来都为true了
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    // 如果overlap大小超过了一定范围,返回true
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

// ReleaseInputs()函数用来解引用compact过程中使用的version
void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
