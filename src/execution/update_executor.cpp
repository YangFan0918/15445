//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->GetTableOid());
  index_info_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  Tuple tuple_now;
  RID rid_now;
  auto table_heap = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_.get();
  while (child_executor_->Next(&tuple_now, &rid_now)) {
    buffer_tuple_.emplace_back(std::make_pair(tuple_now, rid_now));
    auto tuplemate_now = table_heap->GetTupleMeta(rid_now);
    if (((tuplemate_now.ts_ & TXN_START_ID) != 0 &&
         tuplemate_now.ts_ != exec_ctx_->GetTransaction()->GetTransactionId()) ||
        ((tuplemate_now.ts_ & TXN_START_ID) == 0 && tuplemate_now.ts_ > exec_ctx_->GetTransaction()->GetReadTs())) {
      // write-write conflict occur
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("write-write conflict\n");
    }
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool res = false;
  if (!isdone_) {
    auto txn_manager = exec_ctx_->GetTransactionManager();
    auto twn = exec_ctx_->GetTransaction();
    int count = 0;
    for (auto [tuple_old, rid_] : buffer_tuple_) {
      count++;
      auto tuplemate_old = table_info_->table_->GetTupleMeta(rid_);
      tuple_old = table_info_->table_->GetTuple(rid_).second;
      std::vector<Value> values;
      for (const auto &target_expression : plan_->target_expressions_) {
        values.emplace_back(target_expression->Evaluate(&tuple_old, child_executor_->GetOutputSchema()));
      }
      auto tuple_new = Tuple(values, &child_executor_->GetOutputSchema());
      if (tuplemate_old.ts_ == twn->GetTransactionId()) {
        // self-modification
        if (txn_manager->GetVersionLink(rid_) != std::nullopt && txn_manager->GetVersionLink(rid_)->prev_.IsValid()) {
          // produce new undolog
          auto undolink = txn_manager->GetVersionLink(rid_);
          auto prev = (*undolink).prev_;
          assert(prev.IsValid());
          auto prev_log_idx = prev.prev_log_idx_;
          auto prev_undo_log = twn->GetUndoLog(prev_log_idx);
          // modify pre_undo_log
          // prev_undo_log.is_deleted_ = tuplemate_old.is_deleted_;
          std::vector<bool> is_modified_this_time(child_executor_->GetOutputSchema().GetColumnCount());
          std::vector<bool> is_modified_last_time = prev_undo_log.modified_fields_;
          std::vector<uint> cols;
          for (uint i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            if (tuple_new.GetValue(&child_executor_->GetOutputSchema(), i)
                        .CompareEquals(tuple_old.GetValue(&child_executor_->GetOutputSchema(), i)) !=
                    CmpBool::CmpTrue &&
                !is_modified_last_time[i]) {
              is_modified_this_time[i] = true;
            }
            if (is_modified_last_time[i]) {
              cols.emplace_back(i);
            }
          }
          auto now_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), cols);
          cols.clear();
          int idx = 0;
          std::vector<Value> vv;
          for (uint i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            if (is_modified_this_time[i]) {
              vv.emplace_back(tuple_old.GetValue(&child_executor_->GetOutputSchema(), i));
              cols.emplace_back(i);
            } else if (is_modified_last_time[i]) {
              vv.emplace_back(prev_undo_log.tuple_.GetValue(&now_schema, idx++));
              cols.emplace_back(i);
            }
          }
          now_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), cols);
          prev_undo_log.tuple_ = Tuple(vv, &now_schema);
          for (uint i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            if (is_modified_this_time[i] || is_modified_last_time[i]) {
              is_modified_this_time[i] = true;
            }
          }
          prev_undo_log.modified_fields_ = is_modified_this_time;
          twn->ModifyUndoLog(prev_log_idx, prev_undo_log);

          table_info_->table_->UpdateTupleInPlace({tuplemate_old.ts_, false}, tuple_new, rid_);
        } else {
          table_info_->table_->UpdateTupleInPlace({tuplemate_old.ts_, false}, tuple_new, rid_);
        }
      } else {
        auto versionundolink_pre = txn_manager->GetVersionLink(rid_);
        VersionUndoLink versionundolink_now;
        versionundolink_now.in_progress_ = true;
        if (versionundolink_pre != std::nullopt) {
          versionundolink_now.prev_ = (*versionundolink_pre).prev_;
        }
        auto check = [versionundolink_pre](std::optional<VersionUndoLink> res) -> bool {
          if (res == std::nullopt) {
            return true;
          }
          if (!res->in_progress_ && res->prev_ == versionundolink_pre->prev_) {
            return true;
          }
          return false;
        };

        if (tuplemate_old.ts_ > twn->GetReadTs() || !txn_manager->UpdateVersionLink(rid_, versionundolink_now, check)) {
          twn->SetTainted();
          throw ExecutionException("Primary Key Index exist\n");
        }

        UndoLog undo_log_now;
        undo_log_now.is_deleted_ = tuplemate_old.is_deleted_;
        undo_log_now.ts_ = tuplemate_old.ts_;
        std::vector<bool> is_modified_this_time(child_executor_->GetOutputSchema().GetColumnCount());
        std::vector<Value> vv;
        std::vector<uint> cols;
        for (uint i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
          if (tuple_new.GetValue(&child_executor_->GetOutputSchema(), i)
                  .CompareEquals(tuple_old.GetValue(&child_executor_->GetOutputSchema(), i)) != CmpBool::CmpTrue) {
            is_modified_this_time[i] = true;
            cols.emplace_back(i);
            vv.emplace_back(tuple_old.GetValue(&child_executor_->GetOutputSchema(), i));
          }
        }
        auto now_schema = Schema::CopySchema(&child_executor_->GetOutputSchema(), cols);
        undo_log_now.tuple_ = Tuple(vv, &now_schema);
        undo_log_now.modified_fields_ = is_modified_this_time;
        auto versionundolink = txn_manager->GetVersionLink(rid_);
        if (versionundolink != std::nullopt) {
          undo_log_now.prev_version_ = (*versionundolink).prev_;
        }
        auto answer = twn->AppendUndoLog(undo_log_now);
        VersionUndoLink now_versionundolink = {answer, true};
        txn_manager->UpdateVersionLink(rid_, now_versionundolink);
        table_info_->table_->UpdateTupleInPlace({twn->GetTransactionId(), false}, tuple_new, rid_);
      }
      twn->AppendWriteSet(table_info_->oid_, rid_);
    }
    std::vector<Value> values{{TypeId::INTEGER, count}};
    *tuple = Tuple(values, &GetOutputSchema());
    isdone_ = true;
    res = true;
  }
  return res;
}

}  // namespace bustub