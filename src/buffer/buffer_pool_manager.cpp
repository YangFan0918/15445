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
#include <cstddef>
#include <future>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  // std::cout << "pool的大小为:" << pool_size << "\n";
  // std::cout << "k的大小为:" << replacer_k << "\n";
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock lk(latch_);
  // std::cout << ++idx_ << ":建立一个新的page\n";
  frame_id_t p;  //页框的编号
  if (!free_list_.empty()) {
    auto res = free_list_.begin();
    p = (*res);  //取出来的内存中的页框
    free_list_.erase(res);
  } else {
    if (replacer_->Evict(&p)) {
      if (pages_[p].IsDirty()) {
        // std::cout << "在第" << p << "号frame里的第" << pages_[p].page_id_ << "号page要被移除\n";
        std::promise<bool> p1;
        std::future<bool> f1;
        f1 = p1.get_future();
        disk_scheduler_->Schedule({true, pages_[p].data_, pages_[p].page_id_, std::move(p1)});
        while (!f1.get()) {
        }
        pages_[p].ResetMemory();
      }
      page_table_.erase(pages_[p].page_id_);
    } else {
      return nullptr;
    }
  }
  auto id = AllocatePage();  // page的编号
  // std::cout << "第" << p << "号frame里储存第" << id << "号page\n";
  *(page_id) = id;
  //已经获得frame->p和page->id
  //首先更新frame里面的东西
  pages_[p].page_id_ = id;
  page_table_[id] = p;
  pages_[p].pin_count_ = 1;
  pages_[p].is_dirty_ = false;
  //更新replacer
  replacer_->RecordAccess(p);
  replacer_->SetEvictable(p, false);
  // std::cout << pages_ << " " << (pages_ + p) << " " << p << "\n";
  // assert(pages_[p].page_id_ == id);
  // std::cout << pages_[p].page_id_ << "??\n";
  return (pages_ + p);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock lk(latch_);
  // std::cout << ++idx_ << ":寻找第" << page_id << "号page\n";
  if (page_table_.count(page_id) == 1U) {
    // std::cout << "找到了\n";
    auto p = page_table_[page_id];  //对应的页框
    // assert(pages_[p].page_id_ == page_id);
    pages_[p].pin_count_++;
    replacer_->RecordAccess(p);
    replacer_->SetEvictable(p, false);
    return (pages_ + p);
  }
  frame_id_t p;
  if (!free_list_.empty()) {
    auto res = free_list_.begin();
    p = (*res);  //取出来的内存中的页框
    free_list_.erase(res);
  } else {
    if (replacer_->Evict(&p)) {
      if (pages_[p].IsDirty()) {
        // std::cout << "在第" << p << "号frame里的第" << pages_[p].page_id_ << "号page要被移除\n";
        // callback先随便填一个
        std::promise<bool> p1;
        std::future<bool> f1;
        f1 = p1.get_future();
        disk_scheduler_->Schedule({true, pages_[p].data_, pages_[p].page_id_, std::move(p1)});
        while (!f1.get()) {
        }
        pages_[p].ResetMemory();
      }
      page_table_.erase(pages_[p].page_id_);
    } else {
      return nullptr;
    }
  }
  // std::cout << "第" << p << "号frame里储存第" << page_id << "号page\n";
  pages_[p].page_id_ = page_id;
  page_table_[page_id] = p;
  pages_[p].pin_count_ = 1;
  pages_[p].is_dirty_ = false;
  std::promise<bool> p1;
  std::future<bool> f1;
  f1 = p1.get_future();
  disk_scheduler_->Schedule({false, pages_[p].data_, pages_[p].page_id_, std::move(p1)});
  while (!f1.get()) {
  }
  replacer_->RecordAccess(p);
  replacer_->SetEvictable(p, false);
  // assert(pages_[p].page_id_ == page_id);
  // std::cout << pages_[p].page_id_ << "??\n";
  return (pages_ + p);
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock lk(latch_);
  // std::cout << ++idx_ << ":减少第" << page_id << "号page的引用,并将它的is_dirty设置为" << is_dirty << "\n";
  if (page_table_.count(page_id) == 1U) {
    auto p = page_table_[page_id];
    // std::cerr << pages_[p].page_id_ << "?!!?" << page_id << " " << p << "\n";
    assert(pages_[p].page_id_ == page_id);
    // assert(pages_[p].pin_count_ >= 0);
    if (!pages_[p].IsDirty()) {
      pages_[p].is_dirty_ = is_dirty;
    }
    if (pages_[p].pin_count_ != 0) {
      pages_[p].pin_count_--;
      if (pages_[p].pin_count_ == 0) {
        replacer_->SetEvictable(p, true);
      }
      return true;
    }
    return false;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // std::cout << ++idx_ << ":刷新第" << page_id << "号page\n";
  std::scoped_lock lk(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto p = page_table_[page_id];
  // assert(pages_[p].page_id_ == page_id);
  std::promise<bool> p1;
  std::future<bool> f1;
  f1 = p1.get_future();
  disk_scheduler_->Schedule({true, pages_[p].data_, pages_[p].page_id_, std::move(p1)});
  while (!f1.get()) {
  }
  pages_[p].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // std::cout << ++idx_ << ":刷新全部page\n";
  std::scoped_lock lk(latch_);
  for (auto x : page_table_) {
    auto p = x.second;
    FlushPage(pages_[p].page_id_);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // std::cout << ++idx_ << ":删除第" << page_id << "号page\n";
  std::scoped_lock lk(latch_);
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  auto p = page_table_[page_id];
  if (pages_[p].GetPinCount() != 0) {
    return false;
  }
  auto id = pages_[p].page_id_;
  // assert(id == page_id);
  page_table_.erase(id);
  free_list_.push_back(p);
  replacer_->Remove(p);
  pages_[p].page_id_ = INVALID_PAGE_ID;
  pages_[p].ResetMemory();
  pages_[p].pin_count_ = 0;
  pages_[p].is_dirty_ = false;
  DeallocatePage(id);
  // std::cout << "第" << p << "号frame释放\n";
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::scoped_lock lk(latch_);
  auto res = FetchPage(page_id);
  return {this, res};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::scoped_lock lk(latch_);
  auto res = FetchPage(page_id);
  if (res != nullptr) {
    res->RLatch();
  }
  return {this, res};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::scoped_lock lk(latch_);
  auto res = FetchPage(page_id);
  if (res != nullptr) {
    res->WLatch();
  }
  return {this, res};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  // std::scoped_lock lk(latch_);
  return {this, NewPage(page_id)};
}

}  // namespace bustub
