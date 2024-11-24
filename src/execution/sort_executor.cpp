#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sort_tuple_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sort_tuple_.emplace_back(tuple);
  }
  std::sort(sort_tuple_.begin(), sort_tuple_.end(), [this](const Tuple &a, const Tuple &b) {
    for (const auto &order_by : this->plan_->order_bys_) {
      const Value aa = order_by.second->Evaluate(&a, this->GetOutputSchema());
      const Value bb = order_by.second->Evaluate(&b, this->GetOutputSchema());
      if (aa.CompareEquals(bb) != CmpBool::CmpTrue) {
        if ((order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC)) {
          return aa.CompareLessThan(bb) == CmpBool::CmpTrue;
        }
        return aa.CompareGreaterThan(bb) == CmpBool::CmpTrue;
      }
    }

    return true;
  });
  now_idx_ = 0;
  end_idx_ = sort_tuple_.size();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (now_idx_ != end_idx_) {
    *tuple = sort_tuple_[now_idx_];
    now_idx_++;
    return true;
  }
  return false;
}

}  // namespace bustub
