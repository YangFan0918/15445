//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <malloc.h>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
  //  private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  // [[maybe_unused]] std::list<size_t> history_;
  // [[maybe_unused]] size_t k_;
  // [[maybe_unused]] frame_id_t fid_;
  // [[maybe_unused]] bool is_evictable_{false};
 public:
  LRUKNode *next_;
  LRUKNode *pre_;
  size_t times_;  //被访问的次数
  frame_id_t fid_;
  bool is_evictable_{false};
  LRUKNode() = default;
  explicit LRUKNode(int fid) : next_(), pre_() {
    times_ = 1;
    fid_ = fid;
    is_evictable_ = false;
  }
  ~LRUKNode() = default;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth pre_vious access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() {
    history_list_.clear();
    buffer_list_.clear();

    delete history_list_head_;
    delete history_list_tail_;
    delete buffer_list_head_;
    delete buffer_list_tail_;
    for (auto x : need_del_) {
      // delete x->next_;
      // delete x->pre_;
      delete x;
    }
    need_del_.clear();
  }
  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was pre_viously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was pre_viously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;
  auto DeleteFromHistory() -> LRUKNode * {
    history_list_size_--;
    auto node = history_list_tail_;
    while (!node->is_evictable_) {
      node = node->pre_;
    }
    node->pre_->next_ = node->next_;
    node->next_->pre_ = node->pre_;
    history_list_.erase(node->fid_);
    return node;
  };
  auto DeleteFromBuffer() -> LRUKNode * {
    buffer_list_size_--;
    auto node = buffer_list_tail_;
    while (!node->is_evictable_) {
      node = node->pre_;
    }
    node->pre_->next_ = node->next_;
    node->next_->pre_ = node->pre_;
    buffer_list_.erase(node->fid_);
    return node;
  };

  void RemoveFromList(LRUKNode *node) {
    node->pre_->next_ = node->next_;
    node->next_->pre_ = node->pre_;
  };
  void Inserttohistoy(LRUKNode *node) {
    history_list_head_->next_->pre_ = node;
    node->next_ = history_list_head_->next_;
    node->pre_ = history_list_head_;
    history_list_head_->next_ = node;
  }
  void Inserttobuffer(LRUKNode *node) {
    buffer_list_head_->next_->pre_ = node;
    node->next_ = buffer_list_head_->next_;
    node->pre_ = buffer_list_head_;
    buffer_list_head_->next_ = node;
  }

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode *> history_list_;
  std::unordered_map<frame_id_t, LRUKNode *> buffer_list_;
  LRUKNode *history_list_head_;
  LRUKNode *history_list_tail_;
  LRUKNode *buffer_list_head_;
  LRUKNode *buffer_list_tail_;
  size_t history_list_size_;
  size_t buffer_list_size_;
  std::mutex latch_;      //互斥锁
  size_t k_;              // LRU——K的K
  size_t replacer_size_;  //总的大小 history_list_size+buffer_list_size<=replace_size_
  size_t evictable_size_;
  std::set<LRUKNode *> need_del_;
  // size_t current_timestamp_{0};
  // size_t curr_size_{0};//
};

}  // namespace bustub