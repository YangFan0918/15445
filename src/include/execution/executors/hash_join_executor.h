//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> columns_values_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.columns_values_.size(); i++) {
      if (columns_values_[i].CompareEquals(other.columns_values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  std::vector<Value> columns_values_;
};

}  // namespace bustub

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.columns_values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto GetHashJoinLeftKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &x : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(x->Evaluate(tuple, plan_->GetLeftPlan()->OutputSchema()));
    }
    return {values};
  };

  auto GetHashJoinRightKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &x : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(x->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema()));
    }
    return {values};
  };

  auto GetLeftTupleToHashJoinValue(const Tuple *tuple) -> HashJoinValue {
    std::vector<Value> values;
    for (uint i = 0; i < plan_->GetLeftPlan()->OutputSchema().GetColumnCount(); i++) {
      values.emplace_back(tuple->GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
    }
    return {values};
  };

  auto GetRightTupleToHashJoinValue(const Tuple *tuple) -> HashJoinValue {
    std::vector<Value> values;
    for (uint i = 0; i < plan_->GetRightPlan()->OutputSchema().GetColumnCount(); i++) {
      values.emplace_back(tuple->GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
    }
    return {values};
  };

  auto Insert(const HashJoinKey *key, const HashJoinValue *value) -> void {
    std::vector<HashJoinValue> res;
    if (aht_.count(*key) == 0) {
      res.push_back(*value);
      aht_[*key] = res;
    } else {
      aht_[*key].emplace_back(*value);
    }
  };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<HashJoinKey, std::vector<HashJoinValue>> aht_;
  HashJoinKey left_key_;
  HashJoinValue left_value_;
  Tuple left_tuple_;
  RID left_rid_;
  bool get_left_tuple_;
  bool isdone_;
  int next_idx_;
  int end_idx_;
};

}  // namespace bustub
