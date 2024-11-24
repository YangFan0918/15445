#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ParseAndExpression(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> *left,
                        std::vector<AbstractExpressionRef> *right) -> bool {
  auto *logic_expr = dynamic_cast<LogicExpression *>(predicate.get());
  if (logic_expr != nullptr) {
    return ParseAndExpression(logic_expr->GetChildAt(0), left, right) &&
           ParseAndExpression(logic_expr->GetChildAt(1), left, right);
  }
  auto *comparison_ptr = dynamic_cast<ComparisonExpression *>(predicate.get());
  if (comparison_ptr != nullptr) {
    auto column_value_1 = dynamic_cast<const ColumnValueExpression &>(*comparison_ptr->GetChildAt(0));
    if (column_value_1.GetTupleIdx() == 0) {
      left->emplace_back(comparison_ptr->GetChildAt(0));
      right->emplace_back(comparison_ptr->GetChildAt(1));
    } else {
      left->emplace_back(comparison_ptr->GetChildAt(1));
      right->emplace_back(comparison_ptr->GetChildAt(0));
    }
    return true;
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = OptimizeMergeFilterNLJ(plan->CloneWithChildren(std::move(children)));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    ;
    std::vector<AbstractExpressionRef> left;
    std::vector<AbstractExpressionRef> right;
    auto predicate = join_plan.predicate_;
    if (ParseAndExpression(predicate, &left, &right)) {
      return std::make_shared<HashJoinPlanNode>(join_plan.output_schema_, join_plan.GetLeftPlan(),
                                                join_plan.GetRightPlan(), left, right, join_plan.GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
