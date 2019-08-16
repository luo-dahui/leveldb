// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.
/*
  1.db中有一个 compact 后台进程，负责将 memtable 持久化成 sstable ，以及均衡整个 db 中各 level 的
    sstable 。

  2.Comapct 进程会优先将已经写满的 memtable dump 成 level-0 的 sstable （不会合并相同
    key 或者清理已经删除的 key ）。

  3.然后，根据设计的策略选取 level-n 以及 level-n+1 中有 key-range 
    overlap 的几个 sstable 进行 merge( 期间会合并相同的 key 以及清理删除的 key），最后生成若干个
    level-(n+1) 的 ssatble 。

  4.随着数据不断的写入和 compact 的进行，低 level 的 sstable 不断向高
    level 迁移。 

  5.level-0 中的 sstable 因为是由 memtable 直接 dump得到，所以 key-range 可能 overlap ，
    而 level-1 以及更高 level 中的 sstable 都是做 merge 产生，保证了位于同 level 的 sstable 之间，
    key-range 不会 overlap ，这个特性有利于读的处理。
*/
#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

            
/*
1.将每次 compact 后的最新数据状态定义为 Version ，也就是当前 db 元信息以及每个 level 上具有最新
数据状态的 sstable 集合。 

2.compact 会在某个 level 上新加入或者删除一些 sstable ，但可能这个时候，
那些要删除的 sstable 正在被读，为了处理这样的读写竞争情况，基于 sstable 文件一旦生成就不会
改动的特点，每个 Version 加入引用计数，读以及解除读操作会将引用计数相应加减一。这样， db 中
可能有多个 Version 同时存在（提供服务），它们通过链表链接起来。当 Version 的引用计数为 0 并
且不是当前最新的 Version 时，它会从链表中移除，对应的，该 Version 内的 sstable 就可以删除了
（这些废弃的 sstable 会在下一次 compact 完成时被清理掉）。
*/
class Version {
 public:
  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  // 此版本开始
  VersionSet* vset_;  // VersionSet to which this Version belongs
  // 链表指针：下一个版本
  Version* next_;     // Next version in linked list
  // 链表指针：上一个版本
  Version* prev_;     // Previous version in linked list
  // 引用计数（读以及解除读操作会将引用计数相应加减一）
  int refs_;          // Number of live refs to this version

  // List of files per level
  /*
    1.每个 level 的所有 sstable 元信息。
    2.files_[i] 中的 FileMetaData 按照 FileMetaData::smallest 排序，
      这是在每次更新都保证的。（参见 VersionSet::Builder::Save() ）
  */
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // 下一个文件将根据查找统计信息进行压缩。
  // 需要 compact 的文件（ allowed_seeks 用光）
  FileMetaData* file_to_compact_;
  // file_to_compact_的level
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().

  // 当前最大的compact权重以及对应的level
  double compaction_score_;
  int compaction_level_;
};


/*
1.整个 db 的当前状态被 VersionSet 管理着，其中有:
  (1)当前最新的 Version 以及其他正在服务的 Version链表；
  (2)全局的 SequnceNumber，FileNumber ；
  (3)当前的 manifest_file_number; 
  (4)封装 sstable 的TableCache 。
  (5)每个 level 中下一次 compact 要选取的 start_key 等等。
*/
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);
  // 实际的Env
  Env* const env_;
  // db 的数据路径
  const std::string dbname_;
   // 传入的 option
  const Options* const options_;
   // 操作 sstable 的 TableCache
  TableCache* const table_cache_;
  // 内部key的comparator
  const InternalKeyComparator icmp_;
  // 下一个可用的 FileNumber
  uint64_t next_file_number_;
  // manifest 文件的 FileNumber
  uint64_t manifest_file_number_;
  // 最后用过的 SequnceNumber
  uint64_t last_sequence_;
  // log 文件的 FileNumber
  uint64_t log_number_;
  // 辅助 log 文件的 FileNumber ，在 compact memtable 时，置为 0.
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // manifest 文件的封装
  WritableFile* descriptor_file_;
  // manifest 文件的 writer
  log::Writer* descriptor_log_;
  // 正在服务的 Version 链表
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  // 当前最新的的 Version
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.

  /*
    1.为了尽量均匀 compact 每个 level ，所以会将这一次 compact 的 end-key 作为下一次 compact 的 start-key 。
    2.compactor_pointer_ 就保存着每个 level下一次 compact 的 start-key. 
    3.除了 current_ 外的 Version ，并不会做 compact ，所以这个值并不保存在 Version 中。
  */
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
// 封装关于压缩的信息
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  // 要compact的level
  int level_;
  // 生成sstable文件的最大size(options->max_file_size)
  uint64_t max_output_file_size_;
  // compact时当前的version
  Version* input_version_;
  // 记录compact过程中的操作
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  /*
    inputs_[0]:为level-n的sstable文件信息
    inputs_[1]:为level-n+1的sstable文件信息
  */
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  /*
    1.位于leveln+2,并且与compact的key-range有overlap的sstable文件。
    2.保持grandparents_是因为compact最终会生成一系列level-n+1的sstable文件，
      而如果生成的sstable与level-n+2中有过多的overlap的话，当compact level-n+1时，
      会产生过多的merge,为了尽量避免这种情况，compact过程中需要检查与level-n+2中产生overlap的
      size,并与阀值kMaxGrandParentOverlapBytes做比较，以便提前终止compact。
  */
  std::vector<FileMetaData*> grandparents_;
  // 记录compact时grandparents_中已经overlap的index
  size_t grandparent_index_;
  // 记录是否已经有key检查overlap，如果是第一次检查，发现有overlap ，也不会增加 overlapped_bytes_.
  bool seen_key_; 
  // 记录已经 overlap的累计size         
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey
  /*
  1.compact时，当key的 ValueType 是 kTypeDeletion 时，
    要检查其在 level-n+1 以上是否存在（ IsBaseLevelForKey() ）
    来决定是否丢弃掉该key。因为 compact 时， key的遍历是顺序的，
    所以每次检查从上一次检查结束的地方开始即可，
    level_ptrs_[i] 中就记录了 input_version_->levels_[i] 中，
    上一次比较结束的sstable的容器下标。
  */
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
