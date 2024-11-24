#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = OptimizeMergeFilterScan(plan->CloneWithChildren(std::move(children)));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    auto predicate = seq_plan.filter_predicate_;
    if (predicate) {
      auto table_oid = seq_plan.table_oid_;
      auto table_name = seq_plan.table_name_;
      auto table_idx = catalog_.GetTableIndexes(table_name);
      auto log_expr = dynamic_cast<LogicExpression *>(predicate.get());
      if ((log_expr == nullptr) && !table_idx.empty()) {
        auto equal_expr = dynamic_cast<ComparisonExpression *>(predicate.get());
        if (equal_expr->comp_type_ == ComparisonType::Equal) {
          auto column_expr = dynamic_cast<ColumnValueExpression *>(equal_expr->GetChildAt(0).get());
          auto column_idx = column_expr->GetColIdx();
          for (auto x : table_idx) {
            const auto &columns = x->index_->GetKeyAttrs();
            assert(columns.size() == 1);
            if (columns[0] == column_idx) {
              auto key_prey = dynamic_cast<ConstantValueExpression *>(equal_expr->GetChildAt(1).get());
              return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, table_oid, x->index_oid_, predicate,
                                                         key_prey);
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
