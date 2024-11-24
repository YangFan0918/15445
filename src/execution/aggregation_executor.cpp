//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    auto aggkey = MakeAggregateKey(&tuple);
    auto aggvalue = MakeAggregateValue(&tuple);
    aht_->InsertCombine(aggkey, aggvalue);
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
  isdone_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_->Begin() != aht_->End()) {
    if (*aht_iterator_ == aht_->End()) {
      return false;
    }
    auto aggrekey = aht_iterator_->Key();
    auto aggrevalue = aht_iterator_->Val();
    std::vector<Value> values;
    values.reserve(aggrekey.group_bys_.size() + aggrevalue.aggregates_.size());
    for (const auto &x : aggrekey.group_bys_) {
      values.emplace_back(x);
    }
    for (const auto &x : aggrevalue.aggregates_) {
      values.emplace_back(x);
    }
    *tuple = {values, &GetOutputSchema()};
    ++*aht_iterator_;
    return true;
  }
  if (isdone_) {
    return false;
  }
  isdone_ = true;
  if (plan_->group_bys_.empty()) {
    std::vector<Value> values{};
    Tuple tuple_buffer{};
    for (const auto &agg_value : aht_->GenerateInitialAggregateValue().aggregates_) {
      values.emplace_back(agg_value);
    }
    *tuple = {values, &GetOutputSchema()};
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
