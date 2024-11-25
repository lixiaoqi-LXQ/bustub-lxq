//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include <algorithm>
#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  std::fill(std::begin(directory_page_ids_), std::end(directory_page_ids_), INVALID_PAGE_ID);
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  return hash >> (32 - max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> page_id_t {
  BUSTUB_ASSERT(directory_idx < HTABLE_HEADER_ARRAY_SIZE, "EHTable header idx overflow");
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  BUSTUB_ASSERT(directory_idx < HTABLE_HEADER_ARRAY_SIZE, "header idx overflow");
  auto &target_pageid = directory_page_ids_[directory_idx];
  BUSTUB_ASSERT(target_pageid == -1, "EHTable header trying to overwrite existing page");
  target_pageid = directory_page_id;
}

}  // namespace bustub
