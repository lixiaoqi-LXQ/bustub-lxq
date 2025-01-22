//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

extern const uint32_t DIRECTORY_MASK[];

// TODO: fine-grained lock

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : index_name_(name),
      bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  auto page_guard = bpm->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto header = page_guard.template AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth);
  // std::cout << header_max_depth << "\t" << directory_max_depth << "\t" << bucket_max_size << std::endl;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = Hash(key);
  // std::cout << "LookUp key=" << key << " with hash=" << hash << std::endl;

  // header
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.As<ExtendibleHTableHeaderPage>();
  auto headr_index = header->HashToDirectoryIndex(hash);
  auto dir_pid = header->GetDirectoryPageId(headr_index);
  if (dir_pid == INVALID_PAGE_ID) {
    return false;
  }

  // directory
  ReadPageGuard dir_guard = bpm_->FetchPageRead(dir_pid);
  header_guard.Drop();
  auto dir = dir_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = dir->HashToBucketIndex(hash);
  auto bucket_pid = dir->GetBucketPageId(bucket_idx);
  BUSTUB_ASSERT(bucket_pid != INVALID_PAGE_ID, "No entry with pid=-1 should be in dir-page which is initialized!");

  // bucket
  V tmp_result;
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_pid);
  dir_guard.Drop();
  auto bucket = bucket_guard.As<BucketPageType>();
  auto is_exist = bucket->Lookup(key, tmp_result, cmp_);
  if (is_exist) {
    result->emplace_back(std::move(tmp_result));
  }
  return is_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // std::cout << "Insert key=" << key << " with hash=" << hash << std::endl;

  // header
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto headr_index = header->HashToDirectoryIndex(hash);
  auto dir_pid = header->GetDirectoryPageId(headr_index);

  // completely new directory
  if (dir_pid == INVALID_PAGE_ID) {
    page_id_t new_bkt_pid, new_dir_pid;
    auto dir_guard = bpm_->NewPageGuarded(&new_dir_pid).UpgradeWrite();
    auto bkt_guard = bpm_->NewPageGuarded(&new_bkt_pid).UpgradeWrite();

    // init bucket
    auto bucket = bkt_guard.AsMut<BucketPageType>();
    bucket->Init(bucket_max_size_);
    auto insert_result = bucket->Insert(key, value, cmp_);
    BUSTUB_ASSERT(insert_result == true, "new bucket can insert");

    // init directory: depth is 0, only 1 bucket in the page table
    auto dir = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
    dir->Init(directory_max_depth_);
    dir->SetBucketPageId(0, new_bkt_pid);

    // fill back to header
    header->SetDirectoryPageId(headr_index, new_dir_pid);

    return true;
  }

  // existing directory
  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_pid);
  header_guard.Drop();
  auto dir = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // bucket
  auto bkt_idx = dir->HashToBucketIndex(hash);
  auto bkt_pid = dir->GetBucketPageId(bkt_idx);
  BUSTUB_ASSERT(bkt_pid != INVALID_PAGE_ID, "No entry with pid=-1 should be in dir-page which is initialized!");
  WritePageGuard bkt_guard = bpm_->FetchPageWrite(bkt_pid);
  BucketPageType *bucket = bkt_guard.AsMut<BucketPageType>();
  while (bucket->IsFull()) {
    // 1. if local-depth < global-depth, alocate new bucket page and redistribute kv pairs
    // 2. if local-depth = global-depth, increase global depth then rehash
    // 3. loop until done.

    if (dir->GetLocalDepth(bkt_idx) < dir->GetGlobalDepth()) {
      page_id_t new_bkt_pid;
      WritePageGuard new_bkt_guard = bpm_->NewPageGuarded(&new_bkt_pid).UpgradeWrite();
      BucketPageType *new_bucket = new_bkt_guard.AsMut<BucketPageType>();

      // 1. redistribute kv-pairs
      // new bucket is responsible for 1 (new depth)
      auto local_depth = dir->GetLocalDepth(bkt_idx);
      new_bucket->Init(bucket_max_size_);
      for (auto i = 0u; i < bucket->Size();) {
        auto hash = Hash(bucket->KeyAt(i));
        if (((hash >> local_depth) & 0x1) == 1) {
          new_bucket->Insert(bucket->KeyAt(i), bucket->ValueAt(i), cmp_);
          bucket->RemoveAt(i);
        } else {
          i++;
        }
      }

      // 2. adjust directory entries
      // gloabl depth = 4, current local depth = 2, and dealing with bucket index = 1100
      // 1100:  [1](len=global-local-1) [1](0 or 1, 2 new bucket indicator) [00](fixed length)
      // #entry-to-fill: 2^(global-local)
      auto n_to_fill = 1u << (dir->GetGlobalDepth() - local_depth);
      auto fixed_bits = bkt_idx & DIRECTORY_MASK[local_depth];
      for (auto i = 0u; i < n_to_fill; i++) {
        uint32_t bkt_idx_to_fill = (i << local_depth) | fixed_bits;
        BUSTUB_ASSERT(
            dir->GetLocalDepth(bkt_idx_to_fill) == local_depth and dir->GetBucketPageId(bkt_idx_to_fill) == bkt_pid,
            "sibling should direct into same one bucket");
        dir->IncrLocalDepth(bkt_idx_to_fill);
        if ((i & 0x1) == 1) {
          dir->SetBucketPageId(bkt_idx_to_fill, new_bkt_pid);
        }
      }

      // 3. update bucket infomation
      if (((bkt_idx >> local_depth) & 0x1) == 1) {
        bkt_pid = new_bkt_pid;
        bucket = new_bucket;
        bkt_guard = std::move(new_bkt_guard);
      }
    } else {
      if (not dir->CanGrow()) {
        return false;
      }
      dir->IncrGlobalDepth();
      bkt_idx = dir->HashToBucketIndex(hash);
      BUSTUB_ASSERT(bkt_pid == dir->GetBucketPageId(bkt_idx), "rehash donot affect pid");
    }
  }

  BUSTUB_ASSERT(not bucket->IsFull(), "implementation hypothesis");
  //
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // std::cout << "Remove key=" << key << " with hash=" << hash << std::endl;

  // header
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto headr_index = header->HashToDirectoryIndex(hash);
  auto dir_pid = header->GetDirectoryPageId(headr_index);
  if (dir_pid == INVALID_PAGE_ID) {
    return false;
  }

  // directory
  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_pid);
  header_guard.Drop();
  auto dir = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bkt_idx = dir->HashToBucketIndex(hash);
  auto bkt_pid = dir->GetBucketPageId(bkt_idx);
  BUSTUB_ASSERT(bkt_pid != INVALID_PAGE_ID, "No entry with pid=-1 should be in dir-page which is initialized!");

  // bucket
  WritePageGuard bkt_guard = bpm_->FetchPageWrite(bkt_pid);
  bool ret = bkt_guard.AsMut<BucketPageType>()->Remove(key, cmp_);
  auto bucket = bkt_guard.As<BucketPageType>();

  // merge
  // NOTE: take care of initial case
  bool cont = true;
  while (cont and dir->GetLocalDepth(bkt_idx) != 0) {
    cont = false;
    auto split_idx = dir->GetSplitImageIndex(bkt_idx);
    auto split_pid = dir->GetBucketPageId(split_idx);
    WritePageGuard split_guard = bpm_->FetchPageWrite(split_pid);
    auto split_bkt = split_guard.As<BucketPageType>();
    if ((bucket->IsEmpty() or split_bkt->IsEmpty()) and dir->GetLocalDepth(split_idx) == dir->GetLocalDepth(bkt_idx)) {
      // update pid and local depth
      auto depth = dir->GetLocalDepth(bkt_idx);
      auto fixed = bkt_idx & ((1 << (depth - 1)) - 1);
      auto count = 1u << (dir->GetGlobalDepth() - depth + 1);
      for (auto i = 0u; i < count; i++) {
        auto idx = (i << (depth - 1)) | fixed;
        BUSTUB_ASSERT(dir->GetLocalDepth(idx) == depth, "bulkly same");
        BUSTUB_ASSERT(dir->GetBucketPageId(idx) == bkt_pid or dir->GetBucketPageId(idx) == split_pid,
                      "entry to be merged should behave like this");
        dir->DecrLocalDepth(idx);
        dir->SetBucketPageId(idx, (not bucket->IsEmpty()) ? bkt_pid : split_pid);
      }
      // update variable for loop
      if (bucket->IsEmpty()) {
        bkt_idx = split_idx;
        bkt_pid = split_pid;
        bkt_guard = std::move(split_guard);
        bucket = split_bkt;
      }
      // split_idx = dir->GetSplitImageIndex(bkt_idx);
      // split_pid = dir->GetBucketPageId(split_idx);
      // split_guard = bpm_->FetchPageWrite(split_pid);
      // split_bkt = split_guard.As<BucketPageType>();
      while (dir->CanShrink()) {
        dir->DecrGlobalDepth();
        bkt_idx &= dir->GetGlobalDepthMask();
      }
      cont = true;
    }
  }
  return ret;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
