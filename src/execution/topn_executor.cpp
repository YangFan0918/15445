#include "execution/executors/topn_executor.h"
#include <cstddef>
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  std::size_t k = plan_->n_;
  heap_size_ = 0;
  auto compare = [this](const Tuple &a, const Tuple &b) {
    for (const auto &order_by : this->plan_->order_bys_) {
      const Value aa = order_by.second->Evaluate(&a, this->GetOutputSchema());
      const Value bb = order_by.second->Evaluate(&b, this->GetOutputSchema());
      if (aa.CompareEquals(bb) != CmpBool::CmpTrue) {
        if ((order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC)) {
          return aa.CompareGreaterThan(bb) == CmpBool::CmpFalse;
        }
        return aa.CompareLessThan(bb) == CmpBool::CmpFalse;
      }
    }
    return true;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(compare)> heap{compare};
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    heap_size_++;
    if (heap.size() > k) {
      heap.pop();
      heap_size_--;
    }
  }
  while (!heap.empty()) {
    answer_.emplace_back(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (answer_.empty()) {
    return false;
  }
  *tuple = answer_.back();
  answer_.pop_back();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_size_; }

}  // namespace bustub
