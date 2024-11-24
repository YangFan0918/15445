//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <optional>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
  index_info_ = exec_ctx->GetCatalog()->GetIndex(plan->index_oid_);
}

void IndexScanExecutor::Init() {
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  auto table_schema = index_info_->key_schema_;
  auto key = plan_->pred_key_;
  auto value = key->val_;
  std::vector<Value> values{value};
  Tuple need_search_tuple = Tuple(values, &table_schema);
  rid_.clear();
  htable_->ScanKey(need_search_tuple, &rid_, nullptr);
  isdone_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (isdone_) {
    return false;
  }
  isdone_ = true;
  if (rid_.empty()) {
    return false;
  }
  assert(rid_.size() == 1);
  auto [find_tuplemate, find_tuple] = table_info_->table_->GetTuple(rid_[0]);
  auto txn_manager = exec_ctx_->GetTransactionManager();
  bool flag = false;
  if (find_tuplemate.ts_ == exec_ctx_->GetTransaction()->GetTransactionId() ||
      find_tuplemate.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
    flag = true;
  }
  bool is_dead = !flag ? true : find_tuplemate.is_deleted_;
  if (!flag) {
    std::vector<UndoLog> undolog;
    if (txn_manager->GetUndoLink(rid_[0]) == std::nullopt) {
      return false;
    }
    auto undolink = *txn_manager->GetUndoLink(rid_[0]);
    if (!undolink.IsValid()) {
      return false;
    }
    auto undo_log_optional = exec_ctx_->GetTransactionManager()->GetUndoLogOptional(undolink);
    while (true) {
      if (undo_log_optional == std::nullopt) {
        break;
      }
      undolog.emplace_back(*undo_log_optional);
      if ((*undo_log_optional).ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
        break;
      }
      undolink = (*undo_log_optional).prev_version_;
      if (!undolink.IsValid()) {
        break;
      }
      undo_log_optional = exec_ctx_->GetTransactionManager()->GetUndoLogOptional(undolink);
    }
    if (!undolink.IsValid()) {
      return false;
    }
    if (undolog.empty() || undolog.back().ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
      return false;
    }
    auto answer = ReconstructTuple(&GetOutputSchema(), find_tuple, find_tuplemate, undolog);
    if (answer == std::nullopt) {
      return false;
    }
    find_tuple = *answer;
    is_dead = false;
  }
  if (!is_dead) {
    *tuple = find_tuple;
    *rid = rid_[0];
    return true;
  }
  return false;
}

}  // namespace bustub
