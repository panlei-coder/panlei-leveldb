## Files

The implementation of leveldb is similar in spirit to the representation of a
single [Bigtable tablet (section 5.3)](https://research.google/pubs/pub27898/).
However the organization of the files that make up the representation is
somewhat different and is explained below.

Each database is represented by a set of files stored in a directory. There are
several different types of files as documented below:

### Log files

A log file (*.log) stores a sequence of recent updates. Each update is appended
to the current log file. When the log file reaches a pre-determined size
(approximately 4MB by default), it is converted to a sorted table (see below)
and a new log file is created for future updates.

A copy of the current log file is kept in an in-memory structure (the
`memtable`). This copy is consulted on every read so that read operations
reflect all logged updates.

## Sorted tables

A sorted table (*.ldb) stores a sequence of entries sorted by key. Each entry is
either a value for the key, or a deletion marker for the key. (Deletion markers
are kept around to hide obsolete values present in older sorted tables).

The set of sorted tables are organized into a sequence of levels. The sorted
table generated from a log file is placed in a special **young** level (also
called level-0). When the number of young files exceeds a certain threshold
(currently four), all of the young files are merged together with all of the
overlapping level-1 files to produce a sequence of new level-1 files (we create
a new level-1 file for every 2MB of data.)

Files in the young level may contain overlapping keys. However files in other
levels have distinct non-overlapping key ranges. Consider level number L where
L >= 1. When the combined size of files in level-L exceeds (10^L) MB (i.e., 10MB
for level-1, 100MB for level-2, ...), one file in level-L, and all of the
overlapping files in level-(L+1) are merged to form a set of new files for
level-(L+1). These merges have the effect of gradually migrating new updates
from the young level to the largest level using only bulk reads and writes
(i.e., minimizing expensive seeks).

### Manifest

A MANIFEST file lists the set of sorted tables that make up each level, the
corresponding key ranges, and other important metadata. A new MANIFEST file
(with a new number embedded in the file name) is created whenever the database
is reopened. The MANIFEST file is formatted as a log, and changes made to the
serving state (as files are added or removed) are appended to this log.

### Current

CURRENT is a simple text file that contains the name of the latest MANIFEST
file.

### Info logs

Informational messages are printed to files named LOG and LOG.old.

### Others

Other files used for miscellaneous purposes may also be present (LOCK, *.dbtmp).

## Level 0

When the log file grows above a certain size (4MB by default):
Create a brand new memtable and log file and direct future updates here.

In the background:

1. Write the contents of the previous memtable to an sstable.
2. Discard the memtable.
3. Delete the old log file and the old memtable.
4. Add the new sstable to the young (level-0) level.

## Compactions

When the size of level L exceeds its limit, we compact it in a background
thread. The compaction picks a file from level L and all overlapping files from
the next level L+1. Note that if a level-L file overlaps only part of a
level-(L+1) file, the entire file at level-(L+1) is used as an input to the
compaction and will be discarded after the compaction.  Aside: because level-0
is special (files in it may overlap each other), we treat compactions from
level-0 to level-1 specially: a level-0 compaction may pick more than one
level-0 file in case some of these files overlap each other.

A compaction merges the contents of the picked files to produce a sequence of
level-(L+1) files. We switch to producing a new level-(L+1) file after the
current output file has reached the target file size (2MB). We also switch to a
new output file when the key range of the current output file has grown enough
to overlap more than ten level-(L+2) files.  This last rule ensures that a later
compaction of a level-(L+1) file will not pick up too much data from
level-(L+2).

The old files are discarded and the new files are added to the serving state.

Compactions for a particular level rotate through the key space. In more detail,
for each level L, we remember the ending key of the last compaction at level L.
The next compaction for level L will pick the first file that starts after this
key (wrapping around to the beginning of the key space if there is no such
file).

Compactions drop overwritten values. They also drop deletion markers if there
are no higher numbered levels that contain a file whose range overlaps the
current key.

### Timing

Level-0 compactions will read up to four 1MB files from level-0, and at worst
all the level-1 files (10MB). I.e., we will read 14MB and write 14MB.

Other than the special level-0 compactions, we will pick one 2MB file from level
L. In the worst case, this will overlap ~ 12 files from level L+1 (10 because
level-(L+1) is ten times the size of level-L, and another two at the boundaries
since the file ranges at level-L will usually not be aligned with the file
ranges at level-L+1). The compaction will therefore read 26MB and write 26MB.
Assuming a disk IO rate of 100MB/s (ballpark range for modern drives), the worst
compaction cost will be approximately 0.5 second.

If we throttle the background writing to something small, say 10% of the full
100MB/s speed, a compaction may take up to 5 seconds. If the user is writing at
10MB/s, we might build up lots of level-0 files (~50 to hold the 5*10MB). This
may significantly increase the cost of reads due to the overhead of merging more
files together on every read.

Solution 1: To reduce this problem, we might want to increase the log switching
threshold when the number of level-0 files is large. Though the downside is that
the larger this threshold, the more memory we will need to hold the
corresponding memtable.

Solution 2: We might want to decrease write rate artificially when the number of
level-0 files goes up.

Solution 3: We work on reducing the cost of very wide merges. Perhaps most of
the level-0 files will have their blocks sitting uncompressed in the cache and
we will only need to worry about the O(N) complexity in the merging iterator.

### Number of files

Instead of always making 2MB files, we could make larger files for larger levels
to reduce the total file count, though at the expense of more bursty
compactions.  Alternatively, we could shard the set of files into multiple
directories.

An experiment on an ext3 filesystem on Feb 04, 2011 shows the following timings
to do 100K file opens in directories with varying number of files:


| Files in directory | Microseconds to open a file |
|-------------------:|----------------------------:|
|               1000 |                           9 |
|              10000 |                          10 |
|             100000 |                          16 |

So maybe even the sharding is not necessary on modern filesystems?

## Recovery

* Read CURRENT to find name of the latest committed MANIFEST
* Read the named MANIFEST file
* Clean up stale files
* We could open all sstables here, but it is probably better to be lazy...
* Convert log chunk to a new level-0 sstable
* Start directing new writes to a new log file with recovered sequence#

## Garbage collection of files

`RemoveObsoleteFiles()` is called at the end of every compaction and at the end
of recovery. It finds the names of all files in the database. It deletes all log
files that are not the current log file. It deletes all table files that are not
referenced from some level and are not the output of an active compaction.

一.文件

leveldb 的实现在本质上与单个Bigtabletablet的表示类似（第 5.3 节） 。然而，组成表示的文件的组织有些不同，如下所述。

每个数据库都由存储在目录中的一组文件表示。有几种不同类型的文件，如下所述：

日志文件
日志文件 (*.log) 存储一系列最近的更新。每个更新都会附加到当前日志文件中。当日志文件达到预定大小（默认情况下约为 4MB）时，它会转换为排序表（见下文），并创建一个新的日志文件以供将来更新。

当前日志文件的副本保存在内存结构（ ）中 memtable。每次读取时都会查阅此副本，以便读取操作反映所有记录的更新。

二.Sorted tables

排序表 (*.ldb) 存储按键排序的条目序列。每个条目要么是键的值，要么是键的删除标记。（保留删除标记是为了隐藏旧排序表中存在的过时值）。

这组已排序的表被组织成一系列级别。从日志文件生成的排序表被放置在一个特殊的年轻级别（也称为 level-0）中。当年轻文件的数量超过一定阈值（当前为四个）时，所有年轻文件都会与所有重叠的 1 级文件合并在一起，以生成一系列新的 1 级文件（我们创建一个新的 1 级文件）每 2MB 数据归档一次。）

年轻级别中的文件可能包含重叠的键。然而，其他级别的文件具有不同的不重叠的键范围。考虑级别编号 L，其中 L >= 1。当级别 L 中的文件组合大小超过 (10^L) MB（即，级别 1 为 10MB，级别 2 为 100MB，...）时，其中的一个文件level-L，并且level-(L+1)中的所有重叠文件被合并以形成level-(L+1)的一组新文件。这些合并具有仅使用批量读取和写入（即，最小化昂贵的查找）将新更新从年轻级别逐渐迁移到最大级别的效果。

1.Manifest
MANIFEST 文件列出了组成每个级别的一组排序表、相应的键范围以及其他重要的元数据。每当重新打开数据库时，都会创建一个新的 MANIFEST 文件（文件名中嵌入新编号）。MANIFEST 文件被格式化为日志，并且对服务状态所做的更改（添加或删除文件时）将附加到此日志中。

SSTable 中的某个文件属于特定层级，而且其存储的记录是 key 有序的，那么必然有文件中的最小 key 和最大 key，这是非常重要的信息，LevelDB 应该记下这些信息。manifest 就是干这个的，它记载了 SSTable 各个文件的管理信息，比如属于哪个 level，文件名称叫啥，最小 key 和最大 key 各自是多少。下图是 manifest 所存储内容的示意：

图中只显示了两个文件（manifest 会记载所有 SSTable 文件的这些信息），即 level 0 的 Test1.sst 和 Test2.sst 文件，同时记载了这些文件各自对应的 key 范围，比如 Test1.sst 的 key 范围是 “abc” 到 “hello”，而文件 Test2.sst 的 key 范围是 “bbc” 到 “world”，可以看出两者的 key 范围是有重叠的。

2.Current
CURRENT 是一个简单的文本文件，其中包含最新的 MANIFEST 文件的名称。

3.Info logs
信息性消息被打印到名为 LOG 和 LOG.old 的文件中。

4.其他的
还可能存在用于其他目的的其他文件（LOCK、*.dbtmp）。

三.Level 0

当日志文件增长到超过一定大小（默认为 4MB）时：创建一个全新的内存表和日志文件，并在此处指导将来的更新。

在后台：

将之前memtable的内容写入sstable。
丢弃内存表。
删除旧的日志文件和旧的内存表。
将新的 sstable 添加到 young (level-0) 级别。

四.Compactions

当级别 L 的大小超过其限制时，我们将其压缩到后台线程中。压缩从 L 级选取一个文件，并从下一个 L+1 级选取所有重叠文件。请注意，如果 L 级文件仅与 (L+1) 级文件的一部分重叠，则 (L+1) 级的整个文件将用作压缩的输入，并在压缩后将被丢弃。另外：因为 level-0 是特殊的（其中的文件可能会相互重叠），所以我们特别对待从 level-0 到 level-1 的压缩：一个 level-0 压缩可能会选择多个 level-0 文件，以防其中一些文件文件相互重叠。

压缩合并所选文件的内容以生成一系列级别 (L+1) 文件。当前输出文件达到目标文件大小 (2MB) 后，我们切换到生成新级别 (L+1) 文件。当当前输出文件的键范围增长到足以重叠十多个级别（L+2）文件时，我们还会切换到新的输出文件。最后一条规则确保稍后压缩级别 (L+1) 文件不会从级别 (L+2) 中获取太多数据。

旧文件将被丢弃，新文件将添加到服务状态。

特定级别的压缩在键空间中轮流进行。更详细地说，对于每个级别 L，我们记住级别 L 的最后一次压缩的结束键。级别 L 的下一个压缩将选择在此键之后开始的第一个文件（如果存在，则环绕到键空间的开头）没有这样的文件）。

压缩会丢弃被覆盖的值。如果不存在包含范围与当前键重叠的文件的更高编号级别，它们也会删除删除标记。

1.Timing
Level-0 压缩将从 level-0 读取最多四个 1MB 文件，最坏的情况下读取所有 level-1 文件 (10MB)。即，我们将读取 14MB 并写入 14MB。

除了特殊的 level-0 压缩之外，我们将从 level L 中选择一个 2MB 文件。在最坏的情况下，这将与 level L+1 中的约 12 个文件重叠（10 个，因为 level-(L+1) 是大小的十倍） L 级的文件范围，以及边界上的另外两个，因为 L 级的文件范围通常不会与 L 级+1 的文件范围对齐。因此，压缩将读取 26MB 并写入 26MB。假设磁盘 IO 速率为 100MB/s（现代驱动器的大致范围），最差的压缩成本将约为 0.5 秒。

如果我们将后台写入限制为较小的值，例如 100MB/s 速度的 10%，则压缩可能需要长达 5 秒的时间。如果用户以 10MB/s 的速度写入，我们可能会构建大量 0 级文件（约 50 个以容纳 5*10MB）。由于每次读取时将更多文件合并在一起的开销，这可能会显着增加读取成本。

方案一：为了减少这个问题，当0级文件数量较多时，我们可能需要提高日志切换阈值。虽然缺点是这个阈值越大，我们需要更多的内存来保存相应的内存表。

解决方案2：当0级文件数量增加时，我们可能希望人为地降低写入速率。

解决方案 3：我们致力于降低非常广泛的合并的成本。也许大多数 0 级文件的块都未压缩地保存在缓存中，我们只需要担心合并迭代器中的 O(N) 复杂度。

2.Number of files
我们可以为更大的级别创建更大的文件，而不是总是创建 2MB 文件，以减少文件总数，尽管代价是更多的突发压缩。或者，我们可以将文件集分成多个目录。

2011 年 2 月 4 日在 ext3 文件系统上进行的一项实验显示，在具有不同文件数量的目录中打开 100K 文件的时间如下：

目录中的文件	打开文件所需的微秒数
1000	9
10000	10
100000	16
那么现代文件系统上甚至不需要分片吗？

五.Recovery

读取 CURRENT 以查找最新提交的 MANIFEST 的名称
读取指定的 MANIFEST 文件
清理陈旧文件
我们可以在这里打开所有 sstables，但最好是偷懒......
将日志块转换为新的 0 级 sstable
开始将新写入定向到具有恢复序列的新日志文件#

六.Garbage collection of files

RemoveObsoleteFiles()在每次压缩结束和恢复结束时调用。它查找数据库中所有文件的名称。它删除所有不是当前日志文件的日志文件。它删除所有未从某个级别引用的表文件，并且不是主动压缩的输出。