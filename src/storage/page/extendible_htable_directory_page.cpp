//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::fill(std::begin(local_depths_), std::end(local_depths_), 0);
  std::fill(std::begin(bucket_page_ids_), std::end(bucket_page_ids_), INVALID_PAGE_ID);
}

static const uint32_t DIRECTORY_MASK[] = {0x0, 0x1, 0x3, 0x7, 0xf, 0x1f, 0x3f, 0x7f, 0xff, 0x1ff};

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return DIRECTORY_MASK[global_depth_]; }
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return DIRECTORY_MASK[GetLocalDepth(bucket_idx)];
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "EHTable directory idx out of range");
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE, "EHTable directory idx out of range");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << (global_depth_ - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  BUSTUB_ASSERT(++global_depth_ <= max_depth_, "gloabl depth out of range");
  uint32_t n_entry_old = Size() >> 1;
  memcpy(&bucket_page_ids_[n_entry_old], bucket_page_ids_, n_entry_old * sizeof(page_id_t));
  memcpy(&local_depths_[n_entry_old], local_depths_, n_entry_old * sizeof(uint8_t));

  // hash pid local-depth
  // 0    2   1
  // 1    3   1
  //
  // hash pid local-depth
  // 00   2   1
  // 01   3   1
  // 10   2   1
  // 11   3   1
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // in our case, table can be shrunk only when all local-depth < global-depth
  // ** that is first-half is enough
  global_depth_--;
  for (auto bidx = Size(); bidx < Size() >> 1; bidx++) {
    local_depths_[bidx] = 0;
    bucket_page_ids_[bidx] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (auto bidx = 0u; bidx < Size(); bidx++) {
    if (GetLocalDepth(bidx) >= GetGlobalDepth()) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  // BUSTUB_ASSERT(local_depth <= global_depth_, "local depth <= global depth");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "bucket index out of range");
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
