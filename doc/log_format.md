leveldb Log format
==================
The log file contents are a sequence of 32KB blocks.  The only exception is that
the tail of the file may contain a partial block.

Each block consists of a sequence of records:

    block := record* trailer?
    record :=
      checksum: uint32     // crc32c of type and data[] ; little-endian
      length: uint16       // little-endian
      type: uint8          // One of FULL, FIRST, MIDDLE, LAST
      data: uint8[length]

A record never starts within the last six bytes of a block (since it won't fit).
Any leftover bytes here form the trailer, which must consist entirely of zero
bytes and must be skipped by readers.

Aside: if exactly seven bytes are left in the current block, and a new non-zero
length record is added, the writer must emit a FIRST record (which contains zero
bytes of user data) to fill up the trailing seven bytes of the block and then
emit all of the user data in subsequent blocks.

More types may be added in the future.  Some Readers may skip record types they
do not understand, others may report that some data was skipped.

    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4

The FULL record contains the contents of an entire user record.

FIRST, MIDDLE, LAST are types used for user records that have been split into
multiple fragments (typically because of block boundaries).  FIRST is the type
of the first fragment of a user record, LAST is the type of the last fragment of
a user record, and MIDDLE is the type of all interior fragments of a user
record.

Example: consider a sequence of user records:

    A: length 1000
    B: length 97270
    C: length 8000

**A** will be stored as a FULL record in the first block.

**B** will be split into three fragments: first fragment occupies the rest of
the first block, second fragment occupies the entirety of the second block, and
the third fragment occupies a prefix of the third block.  This will leave six
bytes free in the third block, which will be left empty as the trailer.

**C** will be stored as a FULL record in the fourth block.

----

## Some benefits over the recordio format:

1. We do not need any heuristics for resyncing - just go to next block boundary
   and scan.  If there is a corruption, skip to the next block.  As a
   side-benefit, we do not get confused when part of the contents of one log
   file are embedded as a record inside another log file.

2. Splitting at approximate boundaries (e.g., for mapreduce) is simple: find the
   next block boundary and skip records until we hit a FULL or FIRST record.

3. We do not need extra buffering for large records.

## Some downsides compared to recordio format:

1. No packing of tiny records.  This could be fixed by adding a new record type,
   so it is a shortcoming of the current implementation, not necessarily the
   format.

2. No compression.  Again, this could be fixed by adding new record types.

一.levelDB日志格式

日志文件内容是一系列 32KB 块。唯一的例外是文件的尾部可能包含部分块。

每个块由一系列记录组成：

block := record* trailer?
record :=
  checksum: uint32     // crc32c of type and data[] ; little-endian
  length: uint16       // little-endian
  type: uint8          // One of FULL, FIRST, MIDDLE, LAST
  data: uint8[length]

记录永远不会在块的最后六个字节内开始（因为它放不下）。这里的任何剩余字节形成预告片，它必须完全由零字节组成，并且必须被读者跳过。
另外：如果当前块中只剩下七个字节，并且添加了新的非零长度记录，则编写器必须发出一条 FIRST 记录（其中包含零字节的用户数据）来填充该块的尾随七个字节然后在后续块中发出所有用户数据。
将来可能会添加更多类型。一些读者可能会跳过他们不理解的记录类型，其他读者可能会报告某些数据被跳过。

FULL == 1
FIRST == 2
MIDDLE == 3
LAST == 4

FULL 记录包含整个用户记录的内容。

FIRST、MIDDLE、LAST 是用于已分成多个片段（通常是由于块边界）的用户记录的类型。FIRST 是用户记录的第一个片段的类型，LAST 是用户记录的最后一个片段的类型，MIDDLE 是用户记录的所有内部片段的类型。

示例：考虑一系列用户记录：

A: length 1000
B: length 97270
C: length 8000

A将作为完整记录存储在第一个块中。

B将被分割成三个片段：第一个片段占据第一个块的剩余部分，第二个片段占据第二个块的全部，第三个片段占据第三个块的前缀。这将在第三个块中留下六个空闲字节，该块将作为预告片留空。

C将作为完整记录存储在第四个块中。

二.相对于 recordio 格式的一些优点：

(1)我们不需要任何启发式的重新同步 - 只需转到下一个块边界并扫描即可。如果存在损坏，请跳到下一个块。作为一个附带好处，当一个日志文件的部分内容作为记录嵌入到另一个日志文件中时，我们不会感到困惑。
(2)在近似边界处分割（例如，对于mapreduce）很简单：找到下一个块边界并跳过记录，直到我们到达完整或第一条记录。
(3)对于大记录，我们不需要额外的缓冲。

三.与 recordio 格式相比的一些缺点：

(1)不包装微小的记录。这可以通过添加新的记录类型来解决，因此这是当前实现的缺点，而不一定是格式的缺点。
(2)无压缩。同样，这个问题可以通过添加新的记录类型来解决。