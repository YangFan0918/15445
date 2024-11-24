//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "include/concurrency/transaction_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())) {
  index_info_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  bool res = false;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  if (!isdone_) {
    Tuple tuple_now;
    RID rid_now;
    int count = 0;
    while (child_executor_->Next(&tuple_now, &rid_now)) {
      bool flag = false;
      for (const auto &x : index_info_) {
        std::vector<RID> results;
        x->index_->ScanKey(
            tuple_now.KeyFromTuple(child_executor_->GetOutputSchema(), x->key_schema_, x->index_->GetKeyAttrs()),
            &results, txn);
        if (!results.empty()) {
          //存在主键冲突
          auto tuple_mate = table_info_->table_->GetTupleMeta(results[0]);
          if (!tuple_mate.is_deleted_) {
            txn->SetTainted();
            throw ExecutionException("Primary Key Index exist\n");
          }
          //这条tuple在table_heap被删除了
          if (tuple_mate.ts_ == txn->GetTransactionId()) {
            //自我修改
            table_info_->table_->UpdateTupleInPlace({tuple_mate.ts_, false}, tuple_now, results[0]);
            flag = true;
            txn->AppendWriteSet(table_info_->oid_, results[0]);
            break;
          }
          //别人上一次修改的
          //看一下是否能修改
          auto versionundolink_pre = txn_manager->GetVersionLink(results[0]);
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
          if (tuple_mate.ts_ > txn->GetReadTs() ||
              !txn_manager->UpdateVersionLink(results[0], versionundolink_now, check)) {
            txn->SetTainted();
            throw ExecutionException("Primary Key Index exist\n");
          }
          // lock
          UndoLog undolog;
          undolog.is_deleted_ = true;
          undolog.modified_fields_ = std::vector<bool>(child_executor_->GetOutputSchema().GetColumnCount(), false);
          undolog.prev_version_ = versionundolink_now.prev_;
          undolog.ts_ = tuple_mate.ts_;

          auto undolink = txn->AppendUndoLog(undolog);
          versionundolink_now.prev_ = undolink;
          assert(txn_manager->UpdateVersionLink(results[0], versionundolink_now, nullptr));
          table_info_->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, tuple_now, results[0]);
          flag = true;
          txn->AppendWriteSet(table_info_->oid_, results[0]);
        }
      }
      if (flag) {
        continue;
      }
      //不存在主键冲突
      auto rid_return =
          table_info_->table_->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionId(), false}, tuple_now);
      VersionUndoLink now;
      now.in_progress_ = true;
      txn_manager->UpdateVersionLink(*rid_return, now, nullptr);
      for (auto &x : index_info_) {
        if (!x->index_->InsertEntry(
                tuple_now.KeyFromTuple(child_executor_->GetOutputSchema(), x->key_schema_, x->index_->GetKeyAttrs()),
                rid_return.value(), txn)) {
          txn->SetTainted();
          throw ExecutionException("Primary Key Index exist\n");
        }
      }
      txn->AppendWriteSet(table_info_->oid_, *rid_return);
      count++;
    }
    std::vector<Value> values{{TypeId::INTEGER, count}};
    *tuple = Tuple(values, &GetOutputSchema());
    isdone_ = true;
    res = true;
  }
  return res;
}

}  // namespace bustub
