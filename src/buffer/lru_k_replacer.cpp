//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

auto operator<(const LRUKNode &n1, const LRUKNode &n2) -> bool {
  BUSTUB_ASSERT(n1.IsEvictable() and n2.IsEvictable(), "Node to be compared must be evictable");
  auto kts_n1 = n1.GetKts();
  auto kts_n2 = n2.GetKts();
  if (kts_n1 == kts_n2) {
    BUSTUB_ASSERT(kts_n1 == 0, "Timestamp must be unique");
    return n1.Getoldestts() < n2.Getoldestts();
    // return n1.GetLatestts() < n2.GetLatestts();
  }
  return kts_n1 < kts_n2;
}

void LRUKNode::UpdateHistory(size_t ts) {
  BUSTUB_ASSERT(ts != 0, "0 should be used no k-th access");
  history_.push_front(ts);
  if (history_.size() > k_) {
    history_.pop_back();
  }
  BUSTUB_ASSERT(history_.size() <= k_, "The history size should be no more than k");
  if (not is_evictable_) {
    return;
  }
  BUSTUB_ASSERT(prev_ != nullptr and next_ != nullptr, "Evictable node must be in evict-list");
}

LRUKEvictList::LRUKEvictList() : guard_(std::make_shared<LRUKNode>(0, -1)) { guard_->prev_ = guard_->next_ = guard_; }

LRUKEvictList::~LRUKEvictList() {
  auto node_ptr = guard_;
  do {
    auto old = node_ptr;
    node_ptr = node_ptr->next_;
    old->prev_ = guard_->next_ = nullptr;
  } while (node_ptr != nullptr);
}

void LRUKEvictList::Add(const LRUKNodePtr &node) {
  BUSTUB_ASSERT(not node->IsEvictable(), "Node first added should be non-evictable");
  node->is_evictable_ = true;
  auto next = guard_->next_;
  while (not IsGuard(next) and *next < *node) {
    next = next->next_;
  }
  auto prev = next->prev_;
  node->prev_ = prev;
  prev->next_ = node;
  node->next_ = next;
  next->prev_ = node;
  size_++;
}

void LRUKEvictList::Remove(const LRUKNodePtr &node) {
  BUSTUB_ASSERT(node->IsEvictable(), "Node to remove ,must be in the evict-list");
  auto prev = node->prev_;
  auto next = node->next_;
  prev->next_ = next;
  next->prev_ = prev;
  node->is_evictable_ = false;
  node->prev_ = node->next_ = nullptr;
  size_--;
}

auto LRUKEvictList::Evict() -> LRUKNodePtr {
  BUSTUB_ASSERT(size_ != 0, "Evict on empty list");
  auto victim = guard_->next_;
  guard_->next_ = victim->next_;
  victim->next_->prev_ = guard_;
  victim->is_evictable_ = false;
  victim->prev_ = victim->next_ = nullptr;
  size_--;
  return victim;
}

void LRUKEvictList::UpdatePosition(const LRUKNodePtr &node) {
  BUSTUB_ASSERT(node->IsEvictable(), "Node must be in the evict-list");
  BUSTUB_ASSERT(IsGuard(node->prev_) or *node->prev_ < *node, "Assertion on update");
  auto old_prev = node->prev_;
  auto old_next = node->next_;
  auto new_next = old_next;
  while (not IsGuard(new_next) and *new_next < *node) {
    new_next = new_next->next_;
  }
  if (new_next == old_next) {
    return;
  }
  old_prev->next_ = old_next;
  old_next->prev_ = old_prev;
  auto new_prev = new_next->prev_;
  node->prev_ = new_prev;
  new_prev->next_ = node;
  node->next_ = new_next;
  new_next->prev_ = node;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (evict_list_.GetSize() == 0) {
    latch_.unlock();
    return false;
  }
  auto victim = evict_list_.Evict();
  auto fid = victim->GetFrameID();
  *frame_id = fid;
  node_store_.erase(fid);
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id should not be larger than replacer_size_");
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    iter = node_store_.emplace(frame_id, std::make_shared<LRUKNode>(frame_id, k_)).first;
    evict_list_.Add(iter->second);  // default evictable
  }
  auto node_ptr = iter->second;
  node_ptr->UpdateHistory(TimestampInc());
  if (node_ptr->IsEvictable()) {
    evict_list_.UpdatePosition(node_ptr);
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  auto node = node_store_.at(frame_id);
  if (node->IsEvictable() == set_evictable) {
    latch_.unlock();
    return;
  }
  if (set_evictable) {
    evict_list_.Add(node);
  } else {
    evict_list_.Remove(node);
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_, "frame_id should not be larger than replacer_size_");
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    latch_.unlock();
    return;
  }
  auto node_ptr = iter->second;
  BUSTUB_ASSERT(node_ptr->IsEvictable(), "Remove should not be called on a non-evictable frame");
  node_store_.erase(iter);
  evict_list_.Remove(node_ptr);
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  auto size = evict_list_.GetSize();
  latch_.unlock();
  return size;
}

}  // namespace bustub
