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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : k_(k), replacer_size_(num_frames) {
  history_list_size_ = 0;
  buffer_list_size_ = 0;
  evictable_size_ = 0;

  history_list_head_ = new LRUKNode;
  history_list_tail_ = new LRUKNode;
  history_list_head_->pre_ = history_list_tail_;
  history_list_head_->next_ = history_list_tail_;
  history_list_tail_->pre_ = history_list_head_;
  history_list_tail_->next_ = history_list_head_;

  buffer_list_head_ = new LRUKNode;
  buffer_list_tail_ = new LRUKNode;
  buffer_list_head_->pre_ = buffer_list_tail_;
  buffer_list_head_->next_ = buffer_list_tail_;
  buffer_list_tail_->pre_ = buffer_list_head_;
  buffer_list_tail_->next_ = buffer_list_head_;

  history_list_.clear();
  buffer_list_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lk(latch_);
  bool flag = true;
  if (evictable_size_ == 0U) {
    flag = false;
  } else {
    evictable_size_--;
    if (history_list_size_ != 0U) {
      auto res = DeleteFromHistory();
      (*frame_id) = res->fid_;
      need_del_.insert(res);
    } else {
      auto res = DeleteFromBuffer();
      (*frame_id) = res->fid_;
      need_del_.insert(res);
    }
  }
  return flag;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lk(latch_);
  if (static_cast<int>(frame_id) > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("invalid");
  }
  if (history_list_.find(frame_id) != history_list_.end()) {
    auto res = history_list_.at(frame_id);
    res->times_++;
    if (res->times_ == k_) {
      RemoveFromList(res);
      if (res->is_evictable_) {
        history_list_size_--;
      }
      history_list_.erase(res->fid_);
      Inserttobuffer(res);
      if (res->is_evictable_) {
        buffer_list_size_++;
      }
      buffer_list_.insert({res->fid_, res});
    }
  } else if (buffer_list_.find(frame_id) != buffer_list_.end()) {
    auto res = buffer_list_.at(frame_id);
    RemoveFromList(res);
    Inserttobuffer(res);
  } else {
    auto *res = new LRUKNode(frame_id);
    Inserttohistoy(res);
    history_list_.insert({res->fid_, res});
    need_del_.insert(res);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lk(latch_);
  if (static_cast<int>(frame_id) > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument("invalid");
  }
  if (history_list_.count(frame_id) == 1U) {
    auto res = history_list_.at(frame_id);
    if (res->is_evictable_) {
      history_list_size_--;
      evictable_size_--;
    }
    res->is_evictable_ = set_evictable;
    if (res->is_evictable_) {
      history_list_size_++;
      evictable_size_++;
    }
  } else if (buffer_list_.count(frame_id) == 1U) {
    auto res = buffer_list_.at(frame_id);
    if (res->is_evictable_) {
      buffer_list_size_--;
      evictable_size_--;
    }
    res->is_evictable_ = set_evictable;
    if (res->is_evictable_) {
      buffer_list_size_++;
      evictable_size_++;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lk(latch_);
  if (history_list_.count(frame_id) == 1U) {
    auto res = history_list_.at(frame_id);
    if (!res->is_evictable_) {
      throw std::invalid_argument("invalid");
    }
    //合法的
    evictable_size_--;
    history_list_size_--;
    RemoveFromList(res);
    history_list_.erase(res->fid_);
    need_del_.insert(res);
  } else if (buffer_list_.count(frame_id) == 1U) {
    auto res = buffer_list_.at(frame_id);
    if (!res->is_evictable_) {
      throw std::invalid_argument("invalid");
    }
    //合法的
    evictable_size_--;
    buffer_list_size_--;
    RemoveFromList(res);
    buffer_list_.erase(res->fid_);
    need_del_.insert(res);
  } else {
    return;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lk(latch_);
  auto res = evictable_size_;
  return res;
}
}  // namespace bustub
