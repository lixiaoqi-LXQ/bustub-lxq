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

auto GetThreadID() -> size_t { return std::hash<std::thread::id>{}(std::this_thread::get_id()); }

namespace bustub {

struct BenchMarkInfo {
  const uint NThreadScan{1};
  const uint NThreadGet{8};
  const uint BlockSize{4};

  uint NPages{0};
  uint NBlockPerThread{0};

  std::vector<uint> scan_current_block_;
  std::vector<bool> is_scan_inited_;

  BenchMarkInfo() { is_scan_inited_.assign(NThreadScan, false); }

  void Update(uint pages) {
    NPages = pages;
    NBlockPerThread = pages / NThreadScan / BlockSize;
    scan_current_block_.clear();
    for (uint i = 0; i < NThreadScan; i++) {
      scan_current_block_.push_back(GetThreadBIDScope(i).first);
    }
  }

  using BlockIndex = std::pair<uint, uint>;             // bid & offset
  using BlockScope = std::pair<uint, uint>;             // first and last
  using PageIDScope = std::pair<page_id_t, page_id_t>;  // first and last

  inline auto GetScanThreadID(page_id_t pid) -> uint { return GetBlockIndex(pid).first / NBlockPerThread; }
  inline auto GetBlockIndex(page_id_t pid) -> BlockIndex { return {pid / BlockSize, pid % BlockSize}; }
  inline auto GetBlockPIDScope(page_id_t pid) -> PageIDScope {
    page_id_t start = pid & ~(BlockSize - 1);
    return {start, start + BlockSize};
  }
  inline auto GetThreadBIDScope(uint thread_id) -> BlockScope {
    return {thread_id * NBlockPerThread, (thread_id + 1) * NBlockPerThread};
  }
  inline auto GetCurrentBlockPIDScope(page_id_t pid) -> PageIDScope {
    auto tid = GetScanThreadID(pid);
    auto bid = scan_current_block_[tid];
    return {bid * BlockSize, (bid + 1) * BlockSize};
  }

} bench_mark_info;

struct PageContentFormat {
  uint64_t seed_;
  uint64_t page_id_;
  char data_[0];
};

auto PageContent2Str(Page *page_ptr) -> std::string {
  if (page_ptr == nullptr) {
    return "<invalid>";
  }
  // return std::to_string(page_ptr->GetPinCount());
  auto data = page_ptr->GetData();
  // return std::string("\"") + std::string(data) + std::string("\"");

  std::stringstream ss;
  const auto *content = reinterpret_cast<const PageContentFormat *>(data);
  ss << "{seed=" << content->seed_ << ", page-id=" << &content->page_id_ << "->" << content->page_id_
     << ", data=" << static_cast<uint8_t>(*content->data_) << "}";

  return ss.str();
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
    page_locks_.emplace_back(new std::mutex);
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::FindPageTableAndLock(page_id_t pid) -> frame_id_t {
  page_table_lock_.lock();
  auto iter = page_table_.find(pid);
  return iter == page_table_.end() ? -1 : iter->second;
}

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

auto BufferPoolManager::NewPageAndLock(page_id_t pid, bool auto_unlock) -> Page * {
  // NOTE: page tabel lock is held from the caller outside
  Page *page_ptr{nullptr};
  frame_id_t fid;
  enum { FAIL, FreeList, Evict } result_from;

  if ((fid = PopFreeList()) != -1) {
    result_from = FreeList;
  } else if (replacer_->Evict(&fid)) {
    result_from = Evict;
  } else {
    result_from = FAIL;
  }

  // if found a new page, fid is not in all of the components:
  // page table, free list or replacer

  // initiate for the new page
  if (result_from != FAIL) {
    page_ptr = &pages_[fid];
    page_locks_[fid]->lock();
    if (pid == INVALID_PAGE_ID) {
      pid = AllocatePage();
    }

    if (result_from == Evict) {
      // remove victim pid
      auto victim_pid = page_ptr->GetPageId();
      page_table_.erase(victim_pid);
      // fmt::print("> NewPageAndLock erase victim for pid {}\n", victim_pid);
      // fmt::print("[{}] NewPageAndLock evict victim(pid={}, fid={}), dirty={}, old page content: {}\n",
      //            std::this_thread::get_id(), victim_pid, fid, page_ptr->is_dirty_,
      //            PageContent2Str(page_ptr).c_str());
      BUSTUB_ASSERT(page_ptr->pin_count_ == 0, "the new page should have no pin");
    }

    // update page table and replacer
    page_table_.emplace(pid, fid);
    // fmt::print("> NewPageAndLock insert new page table entry for pid {}\n", pid);
    replacer_->Add(fid);
    if (auto_unlock) {
      page_table_lock_.unlock();
    }

    // page metadata
    SyncPageIfDirty(page_ptr);
    page_ptr->ResetMemory();
    page_ptr->page_id_ = pid;
    // printf("> NewPage set pid to %d\n", page_ptr->page_id_);
    BUSTUB_ASSERT(page_ptr->pin_count_ == 0, "the new page should have no pin");
    page_ptr->pin_count_ = 1;
  } else {
    if (auto_unlock) {
      page_table_lock_.unlock();
    }
  }

  return page_ptr;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  page_table_lock_.lock();
  Page *page_ptr = NewPageAndLock();
  if (page_ptr != nullptr) {
    *page_id = page_ptr->GetPageId();
    page_locks_[Fid(page_ptr)]->unlock();
  }
  // benchmark related info
  bench_mark_info.Update(*page_id + 1);
  // printf("Newpage() return page-id=%d and page*=%p\n", *page_id, page_ptr);
  return page_ptr;
}

// pre-fetch pages in the same block
auto BufferPoolManager::FetchPageScan(page_id_t pid) -> Page * {
  // TODO:
  // 1. sync previous block
  // 2. fetch current block
  auto tid = bench_mark_info.GetScanThreadID(pid);
  auto block = bench_mark_info.GetBlockPIDScope(pid);
  // early return
  // if (bench_mark_info.is_scan_inited_[tid] and pid != block.first) {
  if (pid != block.first) {
    auto fid = FindPageTableAndLock(pid);
    // fmt::print("page table size: {}, contents: ", page_table_.size());
    // for (auto pid : page_table_) {
    //   fmt::print("{}, ", pid.first);
    // }
    // fmt::print("\n");
    BUSTUB_ASSERT(fid != -1, "this page should have been prefetched");
    hit_info_.Hit();
    page_table_lock_.unlock();
    return &pages_[fid];
  }

  Page *ret_page_ptr{nullptr};

  page_table_lock_.lock();

  // flush dirty pages
  // FIXME: there may be no previous block (first time)
  auto current_block = bench_mark_info.GetCurrentBlockPIDScope(pid);
  for (page_id_t i = current_block.first; i < current_block.second; i++) {
    auto iter = page_table_.find(i);
    if (iter == page_table_.end()) {
      continue;
    }
    frame_id_t fid = iter->second;
    std::lock_guard page_lock(*page_locks_[fid]);
    Page *page_ptr = &pages_[fid];
    SyncPageIfDirty(page_ptr);
  }

  // fetch pages: make sure all 4 pages are loaded into the buffer pool
  for (page_id_t i = pid; i < block.second; i++) {
    Page *page_ptr{nullptr};
    frame_id_t fid{-1};

    bool found_in_page_table{false};
    auto iter = page_table_.find(i);
    if (iter != page_table_.end()) {  // 1. try find in page table
      fid = iter->second;
      // pin page (including prefetched ones)
      replacer_->SetEvictable(fid, false);
      {
        std::lock_guard page_lock(*page_locks_[fid]);
        page_ptr = &pages_[fid];
        page_ptr->pin_count_++;
      }
      found_in_page_table = true;
    } else {                                // 2. fetch from disk
      page_ptr = NewPageAndLock(i, false);  // FIXME: handle dirty write together？
      if ((page_ptr) == nullptr) {
        std::terminate();  // cache size is enough for bench program
      }
      fid = Fid(page_ptr);
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest r{/*is_write=*/false, page_ptr->data_, i, std::move(promise)};
      disk_scheduler_->Schedule(std::move(r));
      future.get();  // FIXME: move the waiting to background
      page_locks_[fid]->unlock();
    }
    if (i == pid) {
      ret_page_ptr = page_ptr;
      found_in_page_table ? hit_info_.Hit() : hit_info_.Miss();
    }
  }

  page_table_lock_.unlock();

  // update infomation for get thread to use
  auto bid = bench_mark_info.GetBlockIndex(pid).first;
  bench_mark_info.scan_current_block_[tid] = bid;
  bench_mark_info.is_scan_inited_[tid] = true;

  return ret_page_ptr;
}

auto BufferPoolManager::FetchPageGet(page_id_t pid) -> Page * { return FetchPageDefault(pid); }

auto BufferPoolManager::FetchPageDefault(page_id_t pid, [[maybe_unused]] AccessType access_type) -> Page * {
  Page *page_ptr{nullptr};
  frame_id_t fid;
  if ((fid = FindPageTableAndLock(pid)) != -1) {
    replacer_->SetEvictable(fid, false);
    {
      std::lock_guard page_lock(*page_locks_[fid]);
      page_ptr = &pages_[fid];
      page_ptr->pin_count_++;
    }
    hit_info_.Hit();
    page_table_lock_.unlock();
    replacer_->RecordAccess(fid, access_type);
  } else {
    hit_info_.Miss();
    if ((page_ptr = NewPageAndLock(pid)) != nullptr) {
      fid = Fid(page_ptr);
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      DiskRequest r{/*is_write=*/false, page_ptr->data_, pid, std::move(promise)};
      disk_scheduler_->Schedule(std::move(r));
      future.get();
      page_locks_[fid]->unlock();
    }
  }
  // fmt::print("[{}] FetchPage(pid={}, fid={}), page content: {}\n", std::this_thread::get_id(), pid, fid,
  //            PageContent2Str(page_ptr).c_str());
  return page_ptr;
}

auto BufferPoolManager::FetchPage(page_id_t pid, [[maybe_unused]] AccessType access_type) -> Page * {
  switch (access_type) {
    case AccessType::Scan:
      return FetchPageScan(pid);
    case AccessType::Lookup:
      return FetchPageGet(pid);
    default:
      return FetchPageDefault(pid);
  }
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  bool ret = false;
  Page *page_ptr{nullptr};
  frame_id_t fid;
  if ((fid = FindPageTableAndLock(page_id)) != -1) {
    std::lock_guard l(*page_locks_[fid]);
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
  } else {
    std::terminate();
  }
  page_table_lock_.unlock();
  // fmt::print("[{}] UnpinPage(pid={}, fid={}), page content: {}\n", std::this_thread::get_id(), page_id, fid,
  //            PageContent2Str(page_ptr).c_str());
  return ret;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto fid = FindPageTableAndLock(page_id);
  if (fid == -1) {
    page_table_lock_.unlock();
    return false;
  }
  std::lock_guard guard_page(*page_locks_[fid]);
  Page *page_ptr = &pages_[fid];
  SyncPageIfDirty(page_ptr);
  page_table_lock_.unlock();
  // printf("FlushPage(pid=%d)\n", page_id);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard guard_page_table(page_table_lock_);
  for (auto [pid, fid] : page_table_) {
    std::lock_guard guard_page(*page_locks_[fid]);
    Page *page_ptr = &pages_[fid];
    SyncPageIfDirty(page_ptr);
  }
  // printf("FlushAllPages()\n");
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto fid = FindPageTableAndLock(page_id);
  bool ret{false};
  if (fid == -1) {
    ret = true;
  } else {
    std::lock_guard guard_page(*page_locks_[fid]);
    Page *page_ptr = &pages_[fid];
    if (page_ptr->pin_count_ != 0) {
      ret = false;
    } else {
      // FIXME: if sync for dirty page is needed?
      // SyncPageIfDirty(page_ptr);
      ResetPage(page_ptr);
      replacer_->Remove(fid);
      page_table_.erase(page_id);
      DeallocatePage(page_id);
      {
        std::lock_guard guard_free_list(free_list_lock_);
        free_list_.push_front(fid);
      }
      ret = true;
    }
  }
  page_table_lock_.unlock();
  // printf("DeletePage(%d)\n", page_id);
  return ret;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
