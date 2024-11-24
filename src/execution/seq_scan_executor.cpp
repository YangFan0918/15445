//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include <optional>
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "include/concurrency/transaction_manager.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_iterator_ =
      std::make_unique<TableIterator>(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->MakeIterator());
}

void SeqScanExecutor::Init() {
  table_iterator_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple tuple_now;
  TupleMeta tuplemate_now;
  RID rid_now;
  AbstractExpressionRef filter_predicate = plan_->filter_predicate_;
  Value value_now;
  bool found = true;
  bool is_dead;
  while (!((*table_iterator_)).IsEnd()) {
    tuple_now = (*table_iterator_).GetTuple().second;
    tuplemate_now = (*table_iterator_).GetTuple().first;
    rid_now = (*table_iterator_).GetRID();
    ++(*table_iterator_);
    found = true;

    bool flag = false;
    if (tuplemate_now.ts_ == exec_ctx_->GetTransaction()->GetTransactionId() ||
        tuplemate_now.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
      flag = true;
    }
    is_dead = !flag ? true : tuplemate_now.is_deleted_;

    if (!flag) {
      //需要去找前面的版本
      std::vector<UndoLog> undolog;
      if (exec_ctx_->GetTransactionManager()->GetUndoLink(rid_now) == std::nullopt) {
        continue;
      }
      auto undolink = *(exec_ctx_->GetTransactionManager()->GetUndoLink(rid_now));
      if (!undolink.IsValid()) {
        continue;
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
        continue;
      }
      if (undolog.empty() || undolog.back().ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
        continue;
      }
      auto answer = ReconstructTuple(&GetOutputSchema(), tuple_now, tuplemate_now, undolog);
      if (answer == std::nullopt) {
        continue;
      }
      tuple_now = *answer;
      is_dead = false;
    }

    if (filter_predicate) {
      value_now = filter_predicate->Evaluate(&tuple_now, GetOutputSchema());
      found = (!value_now.IsNull()) && (value_now.GetAs<bool>());
    }
    while ((is_dead || !found) && !(*table_iterator_).IsEnd()) {
      tuple_now = (*table_iterator_).GetTuple().second;
      tuplemate_now = (*table_iterator_).GetTuple().first;
      rid_now = (*table_iterator_).GetRID();
      found = true;
      ++(*table_iterator_);
      flag = false;
      if (tuplemate_now.ts_ == exec_ctx_->GetTransaction()->GetTransactionId() ||
          tuplemate_now.ts_ <= exec_ctx_->GetTransaction()->GetReadTs()) {
        flag = true;
      }
      is_dead = !flag ? true : tuplemate_now.is_deleted_;
      if (!flag) {
        //需要去找前面的版本
        std::vector<UndoLog> undolog;
        if (exec_ctx_->GetTransactionManager()->GetUndoLink(rid_now) == std::nullopt) {
          continue;
        }
        auto undolink = *(exec_ctx_->GetTransactionManager()->GetUndoLink(rid_now));
        if (!undolink.IsValid()) {
          continue;
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
          continue;
        }
        if (undolog.empty() || undolog.back().ts_ > exec_ctx_->GetTransaction()->GetReadTs()) {
          continue;
        }
        auto answer = ReconstructTuple(&GetOutputSchema(), tuple_now, tuplemate_now, undolog);
        if (answer == std::nullopt) {
          continue;
        }
        tuple_now = *answer;
        is_dead = false;
      }

      if (filter_predicate) {
        value_now = filter_predicate->Evaluate(&tuple_now, GetOutputSchema());
        found = (!value_now.IsNull()) && (value_now.GetAs<bool>());
      }
    }
    if (!is_dead && found) {
      *tuple = tuple_now;
      *rid = rid_now;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
