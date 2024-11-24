//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdarg>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  get_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_);
  isdone_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  auto predicate = plan_->predicate_;
  if (plan_->join_type_ == JoinType::LEFT) {
    while (get_left_tuple_) {
      while (right_executor_->Next(&right_tuple, &right_rid)) {
        //左右都能取出来tuple
        //判断一下是否符合join条件
        auto result_predicate = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                        right_executor_->GetOutputSchema());
        if (result_predicate.GetAs<bool>()) {
          std::vector<Value> values;
          for (uint i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (uint i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
            values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
          }
          *tuple = Tuple{values, &GetOutputSchema()};
          isdone_ = true;
          return true;
        }
      }
      //遍历完了b表，如果这个时候没有匹配到要添加NULL
      if (!isdone_) {
        std::vector<Value> values;
        for (uint i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(this->right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        isdone_ = true;
        return true;
      }
      get_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_);
      right_executor_->Init();
      isdone_ = false;
    }
    return false;
  }
  // INNER
  while (get_left_tuple_) {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      //左右都能取出来tuple
      //判断一下是否符合join条件
      auto result_predicate = predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                      right_executor_->GetOutputSchema());
      if (result_predicate.GetAs<bool>()) {
        std::vector<Value> values;
        for (uint i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple{values, &GetOutputSchema()};
        isdone_ = true;
        return true;
      }
    }
    get_left_tuple_ = left_executor_->Next(&left_tuple_, &left_rid_);
    right_executor_->Init();
    isdone_ = false;
  }
  return false;
}

}  // namespace bustub
