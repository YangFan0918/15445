#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr || bpm_ == nullptr) {
    bpm_ = nullptr;
    page_ = nullptr;
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this->page_ == that.page_ && this->bpm_ == that.bpm_) {
    that.Drop();
    return *this;
  }
  if (this->page_ != nullptr) {
    Drop();
  }
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  this->page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}
BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  assert(that.guard_.page_ == nullptr);
  assert(that.guard_.bpm_ == nullptr);
  // that.guard_.page_ = nullptr;
  // that.guard_.bpm_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (guard_.page_ == that.guard_.page_ && guard_.bpm_ == that.guard_.bpm_) {
    that.Drop();
    return *this;
  }
  if (guard_.page_ != nullptr) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  assert(that.guard_.page_ == nullptr);
  assert(that.guard_.bpm_ == nullptr);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_ = std::move(that.guard_);
  assert(that.guard_.page_ == nullptr);
  assert(that.guard_.bpm_ == nullptr);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (guard_.page_ == that.guard_.page_ && guard_.bpm_ == that.guard_.bpm_) {
    that.Drop();
    return *this;
  }
  if (guard_.page_ != nullptr) {
    Drop();
  }
  guard_ = std::move(that.guard_);
  assert(that.guard_.page_ == nullptr);
  assert(that.guard_.bpm_ == nullptr);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  auto res = bpm_->FetchPageWrite(page_->GetPageId());
  Drop();
  return res;
}
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  auto res = bpm_->FetchPageRead(page_->GetPageId());
  Drop();
  return res;
}

}  // namespace bustub
