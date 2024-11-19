//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstdio>

#include <sstream>
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "fmt/std.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PageContentFormat {
  uint64_t seed_;
  uint64_t page_id_;
  char data_[0];
};

auto PageContent2Str(Page *page_ptr) -> std::string {
  if (page_ptr == nullptr) {
    return "<invalid>";
  }
  auto data = page_ptr->GetData();
  return std::string("\"") + std::string(data) + std::string("\"");

  // std::stringstream ss;
  // const auto *content = reinterpret_cast<const PageContentFormat *>(data);
  // ss << "{seed=" << content->seed_ << ", page-id=" << &content->page_id_ << "->" << content->page_id_
  //    << ", data=" << static_cast<uint8_t>(*content->data_) << "}";

  // return ss.str();
};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

void BufferPoolManager::SyncPageIfDirty(Page *page_ptr) {
  if (not page_ptr->IsDirty()) {
    return;
  }
  // printf("> SyncPageIfDirty(pid=%d), page content: %s\n", page_ptr->page_id_, PageContent2Str(page_ptr).c_str());
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest r{/*is_write=*/true, page_ptr->data_, page_ptr->page_id_, std::move(promise)};
  disk_scheduler_->Schedule(std::move(r));
  // TODO(lxq): flush dirty page in background thread
  future.get();
  page_ptr->is_dirty_ = false;
}

auto BufferPoolManager::NewPage(page_id_t pid) -> Page * {
  Page *page_ptr{nullptr};
  frame_id_t fid;

  if (not free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    page_ptr = &pages_[fid];
  } else if (replacer_->Evict(&fid)) {
    page_ptr = &pages_[fid];
    auto victim_pid = page_ptr->GetPageId();
    page_table_.erase(victim_pid);
    // printf("> NewPage evict page with pid=%d\n", victim_pid);
  }

  // initiate for the new page
  if (page_ptr != nullptr) {
    if (pid == INVALID_PAGE_ID) {
      pid = AllocatePage();
    }
    // LRU-K
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    // <pid, fid>
    page_table_.emplace(pid, fid);
    // page metadata
    SyncPageIfDirty(page_ptr);
    page_ptr->ResetMemory();
    page_ptr->page_id_ = pid;
    // printf("> NewPage set pid to %d\n", page_ptr->page_id_);
    BUSTUB_ASSERT(page_ptr->pin_count_ == 0, "the new page should have no pin");
    page_ptr->pin_count_ = 1;
  }
  return page_ptr;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard l(latch_);
  Page *page_ptr = NewPage();
  if (page_ptr != nullptr) {
    *page_id = page_ptr->GetPageId();
  }
  // printf("Newpage() return page-id=%d and page*=%p\n", *page_id, page_ptr);
  return page_ptr;
}

auto BufferPoolManager::FetchPage(page_id_t pid, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard l(latch_);
  Page *page_ptr{nullptr};
  auto iter = page_table_.find(pid);
  if (iter != page_table_.end()) {
    frame_id_t fid = iter->second;
    page_ptr = &pages_[fid];
    page_ptr->pin_count_++;
    replacer_->SetEvictable(fid, false);
    replacer_->RecordAccess(fid);
  } else if ((page_ptr = NewPage(pid)) != nullptr) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    DiskRequest r{/*is_write=*/false, page_ptr->data_, pid, std::move(promise)};
    disk_scheduler_->Schedule(std::move(r));
    future.get();
  }
  // printf("FetchPage(pid=%d), page content: %s\n", pid, PageContent2Str(page_ptr).c_str());
  return page_ptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard l(latch_);
  bool ret = false;
  auto iter = page_table_.find(page_id);
  Page *page_ptr{nullptr};
  if (iter != page_table_.end()) {
    auto fid = iter->second;
    page_ptr = &pages_[fid];
    if (is_dirty) {
      page_ptr->is_dirty_ = is_dirty;
    }
    if (page_ptr->pin_count_ > 0) {
      if (--page_ptr->pin_count_ == 0) {
        replacer_->SetEvictable(fid, true);
      }
      ret = true;
    }
  }
  // printf("UnPinPage(pid=%d, dirty=%s), page content: %s\n", page_id, is_dirty ? "true" : "false",
  //        PageContent2Str(page_ptr).c_str());
  return ret;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard l(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto fid = iter->second;
  Page *page_ptr = &pages_[fid];
  SyncPageIfDirty(page_ptr);
  // printf("FlushPage(pid=%d)\n", page_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard l(latch_);
  for (auto [pid, fid] : page_table_) {
    Page *page_ptr = &pages_[fid];
    SyncPageIfDirty(page_ptr);
  }
  // printf("FlushAllPages()\n");
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard l(latch_);
  auto iter = page_table_.find(page_id);
  bool ret = false;
  if (iter == page_table_.end()) {
    ret = true;
  } else {
    frame_id_t fid = iter->second;
    Page *page_ptr = &pages_[fid];
    if (page_ptr->pin_count_ != 0) {
      ret = false;
    } else {
      // FIXME: if sync for dirty page is needed?
      // SyncPageIfDirty(page_ptr);
      ResetPage(page_ptr);
      replacer_->Remove(fid);
      free_list_.push_front(fid);
      page_table_.erase(iter);
      ret = true;
      DeallocatePage(page_id);
    }
  }
  // printf("DeletePage(%d)\n", page_id);
  return ret;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto ptr = FetchPage(page_id);
  return {this, ptr};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  BasicPageGuard bg{this, FetchPage(page_id)};
  return bg.UpgradeRead();
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  BasicPageGuard bg{this, FetchPage(page_id)};
  return bg.UpgradeWrite();
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto ptr = NewPage(page_id);
  return {this, ptr};
}
}  // namespace bustub
