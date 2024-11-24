//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  get_left_tuple_ = left_child_->Next(&left_tuple_, &left_rid_);
  Tuple right_tuple = {};
  RID right_rid = {};
  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto right_hash_join_key = GetHashJoinRightKey(&right_tuple);
    auto right_hash_join_value = GetRightTupleToHashJoinValue(&right_tuple);
    Insert(&right_hash_join_key, &right_hash_join_value);
  }

  left_key_ = GetHashJoinLeftKey(&left_tuple_);
  left_value_ = GetLeftTupleToHashJoinValue(&left_tuple_);
  if (aht_.count(left_key_) != 0U) {
    next_idx_ = 0;
    end_idx_ = aht_[left_key_].size();
  } else {
    next_idx_ = end_idx_ = 0;
  }
  isdone_ = false;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (get_left_tuple_) {
    if (next_idx_ != end_idx_) {
      std::vector<Value> values;
      for (const auto &x : left_value_.columns_values_) {
        values.emplace_back(x);
      }
      for (const auto &x : aht_[left_key_][next_idx_].columns_values_) {
        values.emplace_back(x);
      }
      *tuple = Tuple(values, &GetOutputSchema());
      isdone_ = true;
      next_idx_++;
      return true;
    }
    bool flag = false;
    if (!isdone_ && plan_->join_type_ == JoinType::LEFT) {
      std::vector<Value> values;
      for (const auto &x : left_value_.columns_values_) {
        values.emplace_back(x);
      }
      for (uint i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(this->right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      flag = true;
    }
    get_left_tuple_ = left_child_->Next(&left_tuple_, &left_rid_);
    left_key_ = GetHashJoinLeftKey(&left_tuple_);
    left_value_ = GetLeftTupleToHashJoinValue(&left_tuple_);
    if (aht_.count(left_key_) != 0U) {
      next_idx_ = 0;
      end_idx_ = aht_[left_key_].size();
    } else {
      next_idx_ = end_idx_ = 0;
    }
    isdone_ = false;
    if (flag) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
