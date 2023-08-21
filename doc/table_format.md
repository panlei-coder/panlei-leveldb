leveldb File format
===================

    <beginning_of_file>
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block 1]
    ...
    [meta block K]
    [metaindex block]
    [index block]
    [Footer]        (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>

The file contains internal pointers.  Each such pointer is called
a BlockHandle and contains the following information:

    offset:   varint64
    size:     varint64

See [varints](https://developers.google.com/protocol-buffers/docs/encoding#varints)
for an explanation of varint64 format.

1.  The sequence of key/value pairs in the file are stored in sorted
order and partitioned into a sequence of data blocks.  These blocks
come one after another at the beginning of the file.  Each data block
is formatted according to the code in `block_builder.cc`, and then
optionally compressed.

2. After the data blocks we store a bunch of meta blocks.  The
supported meta block types are described below.  More meta block types
may be added in the future.  Each meta block is again formatted using
`block_builder.cc` and then optionally compressed.

3. A "metaindex" block.  It contains one entry for every other meta
block where the key is the name of the meta block and the value is a
BlockHandle pointing to that meta block.

4. An "index" block.  This block contains one entry per data block,
where the key is a string >= last key in that data block and before
the first key in the successive data block.  The value is the
BlockHandle for the data block.

5. At the very end of the file is a fixed length footer that contains
the BlockHandle of the metaindex and index blocks as well as a magic number.

        metaindex_handle: char[p];     // Block handle for metaindex
        index_handle:     char[q];     // Block handle for index
        padding:          char[40-p-q];// zeroed bytes to make fixed length
                                       // (40==2*BlockHandle::kMaxEncodedLength)
        magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)

## "filter" Meta Block

If a `FilterPolicy` was specified when the database was opened, a
filter block is stored in each table.  The "metaindex" block contains
an entry that maps from `filter.<N>` to the BlockHandle for the filter
block where `<N>` is the string returned by the filter policy's
`Name()` method.

The filter block stores a sequence of filters, where filter i contains
the output of `FilterPolicy::CreateFilter()` on all keys that are stored
in a block whose file offset falls within the range

    [ i*base ... (i+1)*base-1 ]

Currently, "base" is 2KB.  So for example, if blocks X and Y start in
the range `[ 0KB .. 2KB-1 ]`, all of the keys in X and Y will be
converted to a filter by calling `FilterPolicy::CreateFilter()`, and the
resulting filter will be stored as the first filter in the filter
block.

The filter block is formatted as follows:

    [filter 0]
    [filter 1]
    [filter 2]
    ...
    [filter N-1]

    [offset of filter 0]                  : 4 bytes
    [offset of filter 1]                  : 4 bytes
    [offset of filter 2]                  : 4 bytes
    ...
    [offset of filter N-1]                : 4 bytes

    [offset of beginning of offset array] : 4 bytes
    lg(base)                              : 1 byte

The offset array at the end of the filter block allows efficient
mapping from a data block offset to the corresponding filter.

## "stats" Meta Block

This meta block contains a bunch of stats.  The key is the name
of the statistic.  The value contains the statistic.

TODO(postrelease): record following stats.

    data size
    index size
    key size (uncompressed)
    value size (uncompressed)
    number of entries
    number of data blocks

一.leveldb 文件格式

<beginning_of_file>
[data block 1]
[data block 2]
...
[data block N]
[meta block 1]
...
[meta block K]
[metaindex block]
[index block]
[Footer]        (fixed size; starts at file_size - sizeof(Footer))
<end_of_file>

该文件包含内部指针。每个这样的指针称为 BlockHandle 并包含以下信息：

offset:   varint64
size:     varint64
有关 varint64 格式的说明，请参阅varints 。

文件中的键/值对序列按排序顺序存储，并划分为一系列数据块。这些块一个接一个地出现在文件的开头。每个数据块根据 中的代码进行格式化block_builder.cc，然后选择性地进行压缩。

在数据块之后，我们存储一堆元块。支持的元块类型如下所述。将来可能会添加更多元块类型。每个元块再次使用格式化 block_builder.cc，然后可选地进行压缩。

一个“元索引”块。它包含每个其他元块的一个条目，其中键是元块的名称，值是指向该元块的 BlockHandle。

“索引”块。该块每个数据块包含一个条目，其中键是一个字符串 >= 该数据块中的最后一个键并且在连续数据块中的第一个键之前。该值是数据块的BlockHandle。

文件的最后是一个固定长度的页脚，其中包含元索引和索引块的块句柄以及一个幻数。

 metaindex_handle: char[p];     // Block handle for metaindex
 index_handle:     char[q];     // Block handle for index
 padding:          char[40-p-q];// zeroed bytes to make fixed length
                                // (40==2*BlockHandle::kMaxEncodedLength)
 magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)

二.“过滤”元块

FilterPolicy如果在打开数据库时指定了a ，则每个表中都会存储一个过滤块。“metaindex”块包含一个条目，该条目映射到filter.<N>过滤器块的 BlockHandle，其中<N>是过滤器策略方法返回的字符串 Name()。

过滤器块存储一系列过滤器，其中过滤器 i 包含存储FilterPolicy::CreateFilter()在文件偏移量落在范围内的块中的所有键的输出

[ i*base ... (i+1)*base-1 ]
目前，“base”为 2KB。例如，如果块 X 和 Y 开始于 range [ 0KB .. 2KB-1 ]，则 X 和 Y 中的所有键都将通过调用 转换为过滤器FilterPolicy::CreateFilter()，并且生成的过滤器将存储为过滤器块中的第一个过滤器。

过滤器块的格式如下：

[filter 0]
[filter 1]
[filter 2]
...
[filter N-1]

[offset of filter 0]                  : 4 bytes
[offset of filter 1]                  : 4 bytes
[offset of filter 2]                  : 4 bytes
...
[offset of filter N-1]                : 4 bytes

[offset of beginning of offset array] : 4 bytes
lg(base)                              : 1 byte
过滤器块末尾的偏移数组允许从数据块偏移到相应过滤器的有效映射。

三.“统计”元块

这个元块包含一堆统计数据。关键是统计数据的名称。该值包含统计信息。

TODO（发布后）：记录以下统计数据。

data size
index size
key size (uncompressed)
value size (uncompressed)
number of entries
number of data blocks