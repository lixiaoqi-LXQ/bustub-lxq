#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  MoveGuard(std::forward<BasicPageGuard>(that), std::forward<BasicPageGuard>(*this));
}

void BasicPageGuard::Drop() {
  //   BUSTUB_ASSERT(page_ != nullptr, "why use in-valid page guard?");
  if (page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(PageId(), is_dirty_);
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  MoveGuard(std::forward<BasicPageGuard>(that), std::forward<BasicPageGuard>(*this));
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  page_->RLatch();
  ReadPageGuard rg;
  rg.guard_ = std::move(*this);
  return rg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  page_->WLatch();
  WritePageGuard wg;
  wg.guard_ = std::move(*this);
  return wg;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  // BasicPageGuard::MoveGuard(std::forward<BasicPageGuard>(that.guard_), std::forward<BasicPageGuard>(this->guard_));
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  // BasicPageGuard::MoveGuard(std::forward<BasicPageGuard>(that.guard_), std::forward<BasicPageGuard>(this->guard_));
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
