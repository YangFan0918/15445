//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  // TODO(fall2023): set the timestamps here. Watermark updated below.
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  txn_ref->read_ts_.store(last_commit_ts_.load());
  commit_lck.unlock();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  timestamp_t commit_ts = last_commit_ts_ + 1;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  for (auto [table_id, rid_set] : txn->GetWriteSets()) {
    auto table_heap = catalog_->GetTable(table_id)->table_.get();
    for (auto x : rid_set) {
      TupleMeta tuplemate = table_heap->GetTupleMeta(x);
      table_heap->UpdateTupleMeta({commit_ts, tuplemate.is_deleted_}, x);

      auto versionundolink = GetVersionLink(x);
      versionundolink->in_progress_ = false;
      UpdateVersionLink(x, versionundolink);
    }
  }
  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_++;
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto watermark = GetWatermark();
  std::unordered_map<txn_id_t, bool> should_be_save;
  auto all_table_name = catalog_->GetTableNames();
  for (const auto &table_name : all_table_name) {
    auto table_heap = catalog_->GetTable(table_name)->table_.get();
    auto iterator = table_heap->MakeIterator();
    while (!iterator.IsEnd()) {
      auto [tuplemate, tuple] = iterator.GetTuple();
      auto rid = iterator.GetRID();
      ++iterator;
      if (tuplemate.ts_ <= watermark) {
        continue;
      }
      auto version_undo_link = GetVersionLink(rid);
      if (version_undo_link == std::nullopt) {
        continue;
      }
      auto undo_link = (*version_undo_link).prev_;
      UndoLog undo_log;
      if (undo_link.IsValid() && txn_map_.count(undo_link.prev_txn_) != 0) {
        undo_log = GetUndoLog(undo_link);
      }
      bool flag = false;
      while (undo_link.IsValid() && txn_map_.count(undo_link.prev_txn_) != 0 &&
             (undo_log.ts_ > watermark || (undo_log.ts_ <= watermark && !flag))) {
        should_be_save[undo_link.prev_txn_] = true;
        if (undo_log.ts_ <= watermark) {
          flag = true;
        }
        undo_link = undo_log.prev_version_;
        if (undo_link.IsValid() && txn_map_.count(undo_link.prev_txn_) != 0) {
          undo_log = GetUndoLog(undo_link);
        }
      }
    }
  }
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    if (should_be_save.find(it->first) != should_be_save.end() ||
        it->second->GetTransactionState() != TransactionState::COMMITTED) {
      ++it;
    } else {
      it = txn_map_.erase(it);
    }
  }
}

}  // namespace bustub
