//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  //初始化header_page_id_
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto p = header_guard.UpgradeWrite();
  auto header_page = p.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
  header_guard.Drop();
  p.Drop();
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // return false;
  auto header_guard = bpm_->FetchPageRead(header_page_id_);          //找到header的page并且加上读的锁
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();  //转化成指向这个page的指针
  uint32_t hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);  //得到在header中的下标
  if (header_page->GetDirectoryPageId(directory_idx) == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);  //
  header_guard.Drop();
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();  //
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  if (directory_page->GetBucketPageId(bucket_idx) == (INVALID_PAGE_ID)) {
    return false;
  }
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  directory_guard.Drop();
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V res;
  if (bucket_page->Lookup(key, res, cmp_)) {
    bucket_guard.Drop();
    (*result).push_back(res);
    return true;
  }
  bucket_guard.Drop();
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();  //转化成指向这个page的指针
  uint32_t hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  if (header_page->GetDirectoryPageId(directory_idx) == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);  //
  header_guard.Drop();
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();  //
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  if (directory_page->GetBucketPageId(bucket_idx) == (INVALID_PAGE_ID)) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->Insert(key, value, cmp_)) {
    return true;
  }
  V res;
  if (bucket_page->Lookup(key, res, cmp_)) {
    return false;
  }
  // header_guard.Drop();
  // bucket已经满了
  while (!bucket_page->Insert(key, value, cmp_)) {
    // directory需要扩展但是没办法扩展了
    if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_idx) &&
        directory_page->GetGlobalDepth() == directory_page->GetMaxDepth()) {
      return false;
    }
    //先生成一个新的bucket
    page_id_t new_bucket_page_id;
    if (bpm_->NewPage(&new_bucket_page_id) == nullptr) {
      return false;
    }
    bpm_->UnpinPage(new_bucket_page_id, false);
    //有了new_bucket_page_id
    if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_idx)) {
      directory_page->IncrGlobalDepth();
    }
    directory_page->IncrLocalDepth(bucket_idx);
    int new_local_depth = directory_page->GetLocalDepth(bucket_idx);  //新的local_depth
    auto new_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
    UpdateDirectoryMapping(directory_page, new_bucket_idx, new_bucket_page_id, new_local_depth,
                           (1U << new_local_depth) - 1);
    UpdateDirectoryMapping(directory_page, bucket_idx, bucket_page_id, new_local_depth, (1U << new_local_depth) - 1);
    auto new_bucket_gurad = bpm_->FetchPageWrite(new_bucket_page_id);
    auto new_bucket_page = new_bucket_gurad.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    MigrateEntries(bucket_page, new_bucket_page, new_bucket_idx, (1U << new_local_depth) - 1);
    if (directory_page->HashToBucketIndex(hash) == new_bucket_idx) {
      std::swap(new_bucket_idx, bucket_idx);
      std::swap(new_bucket_gurad, bucket_guard);
      std::swap(new_bucket_page, bucket_page);
    }
    // bucket_guard.Drop();
    // bucket_idx = directory_page->HashToBucketIndex(hash);
    // bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    // bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    // bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  //创建一个directory的page
  page_id_t directory_page_id;
  if (bpm_->NewPage(&directory_page_id) == nullptr) {
    return false;  //创建page失败直接false
  }
  bpm_->UnpinPage(directory_page_id, false);
  //更新header的信息
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  //更新directory
  auto directory_gurad = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  return InsertToNewBucket(directory_page, directory_page->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  //先创建一个bucket的page
  page_id_t bucket_page_id;
  if (bpm_->NewPage(&bucket_page_id) == nullptr) {
    return false;  //创建page失败直接false
  }
  bpm_->UnpinPage(bucket_page_id, false);
  //更新directory的信息
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  directory->SetLocalDepth(bucket_idx, 0);
  //现在已经有page了让page变成pagewriteguard
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  bucket_page->Insert(key, value, cmp_);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  uint32_t pre = (1U << new_local_depth);
  for (uint32_t i = new_bucket_idx; i >= pre; i -= pre) {
    directory->SetBucketPageId(i, new_bucket_page_id);
    directory->SetLocalDepth(i, new_local_depth);
  }
  for (uint32_t i = new_bucket_idx; i < directory->Size(); i += pre) {
    directory->SetBucketPageId(i, new_bucket_page_id);
    directory->SetLocalDepth(i, new_local_depth);
  }
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  uint32_t old_bucket_size = old_bucket->Size();
  std::vector<std::pair<K, V>> res;
  for (uint32_t i = 0; i < old_bucket_size; i++) {
    res.push_back(std::make_pair(old_bucket->KeyAt(i), old_bucket->ValueAt(i)));
  }
  old_bucket->Init(bucket_max_size_);
  new_bucket->Init(bucket_max_size_);
  for (uint32_t i = 0; i < old_bucket_size; i++) {
    auto [Key, Value] = res[i];
    auto hash = Hash(Key);
    auto idx = hash & local_depth_mask;
    if (idx == (new_bucket_idx & local_depth_mask)) {
      new_bucket->Insert(Key, Value, cmp_);
    } else {
      old_bucket->Insert(Key, Value, cmp_);
    }
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();  //转化成指向这个page的指针
  uint32_t hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  if (header_page->GetDirectoryPageId(directory_idx) == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);  //
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();  //
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  if (directory_page->GetBucketPageId(bucket_idx) == (INVALID_PAGE_ID)) {
    return false;
  }
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  while (bucket_page->IsEmpty()) {
    if (directory_page->GetLocalDepth(bucket_idx) == 0) {
      break;
    }
    if (directory_page->GetLocalDepth(bucket_idx) !=
        directory_page->GetLocalDepth(directory_page->GetSplitImageIndex(bucket_idx))) {
      break;
    }
    auto new_bucket_page_id = directory_page->GetBucketPageId(directory_page->GetSplitImageIndex(bucket_idx));
    directory_page->DecrLocalDepth(bucket_idx);
    int now_local_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t pre = (1U << now_local_depth);
    for (uint32_t i = bucket_idx & (pre - 1); i >= pre; i -= pre) {
      directory_page->SetLocalDepth(i, now_local_depth);
      directory_page->SetBucketPageId(i, new_bucket_page_id);
    }
    for (uint32_t i = bucket_idx & (pre - 1); i < directory_page->Size(); i += pre) {
      directory_page->SetLocalDepth(i, now_local_depth);
      directory_page->SetBucketPageId(i, new_bucket_page_id);
    }
    while (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
    bucket_idx = directory_page->HashToBucketIndex(hash);
    bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    bucket_guard.Drop();
    bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    //看看这时候的splitidx可不可以合并
    if (directory_page->GetLocalDepth(bucket_idx) != 0U) {
      auto split_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
      auto split_bucket_page_id = directory_page->GetBucketPageId(split_bucket_idx);
      auto split_bucket_guard = bpm_->FetchPageWrite(split_bucket_page_id);
      auto split_bucket_page = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      if (split_bucket_page->Size() == 0) {
        bucket_idx = split_bucket_idx;
        bucket_page_id = split_bucket_page_id;
        bucket_guard.Drop();
        bucket_guard = std::move(split_bucket_guard);
        bucket_page = split_bucket_page;
      } else {
        split_bucket_guard.Drop();
      }
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub