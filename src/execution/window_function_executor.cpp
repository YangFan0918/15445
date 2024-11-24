#include "execution/executors/window_function_executor.h"
#include <sys/types.h>
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  std::vector<Tuple> sort_tuple;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sort_tuple.emplace_back(tuple);
  }
  //先排序
  uint column_num = plan_->columns_.size();
  for (uint i = 0; i < column_num; i++) {
    if (plan_->window_functions_.count(i) > 0) {
      auto res = plan_->window_functions_.find(i)->second;
      if (!res.order_by_.empty()) {
        std::sort(sort_tuple.begin(), sort_tuple.end(), [res, this](const Tuple &a, const Tuple &b) {
          for (const auto &order_by : res.order_by_) {
            const Value aa = order_by.second->Evaluate(&a, this->child_executor_->GetOutputSchema());
            const Value bb = order_by.second->Evaluate(&b, this->child_executor_->GetOutputSchema());
            if (aa.CompareEquals(bb) != CmpBool::CmpTrue) {
              if ((order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC)) {
                return aa.CompareLessThan(bb) == CmpBool::CmpTrue;
              }
              return aa.CompareGreaterThan(bb) == CmpBool::CmpTrue;
            }
          }

          return true;
        });
        break;
      }
    }
  }
  std::vector<bool> is_window_function(column_num);
  std::vector<bool> has_order_by(column_num);
  for (uint i = 0; i < column_num; i++) {
    if (plan_->window_functions_.count(i) > 0) {
      is_window_function[i] = true;
      aht_.emplace_back(SimpleWindowFunctionHashtable(plan_->window_functions_.find(i)->second.type_));
      if (!plan_->window_functions_.find(i)->second.order_by_.empty()) {
        has_order_by[i] = true;
      }
    } else {
      aht_.emplace_back(SimpleWindowFunctionHashtable(WindowFunctionType::CountStarAggregate));
    }
  }
  for (const auto &tuple : sort_tuple) {
    std::vector<Value> value(column_num);
    for (uint i = 0; i < column_num; i++) {
      if (is_window_function[i]) {
        //先查入
        auto now_windowfunction = plan_->window_functions_.find(i)->second;
        std::vector<Value> key;
        for (const auto &x : now_windowfunction.partition_by_) {
          key.push_back(x->Evaluate(&tuple, child_executor_->GetOutputSchema()));
        }
        if (now_windowfunction.type_ == WindowFunctionType::Rank) {
          assert(now_windowfunction.order_by_.size() == 1);
          auto vv = now_windowfunction.order_by_[0].second->Evaluate(&tuple, child_executor_->GetOutputSchema());
          value[i] = aht_[i].InsertCombine({key}, vv);
          continue;
        }
        Value v = now_windowfunction.function_->Evaluate(&tuple, child_executor_->GetOutputSchema());
        aht_[i].InsertCombine({key}, v);
        if (!now_windowfunction.order_by_.empty()) {
          value[i] = aht_[i].Find({key});
        }
      } else {
        value[i] = (tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
    }
    answer_.emplace_back(value);
  }
  int idx = -1;
  for (const auto &tuple : sort_tuple) {
    ++idx;
    for (uint i = 0; i < column_num; i++) {
      if (is_window_function[i]) {
        auto now_windowfunction = plan_->window_functions_.find(i)->second;
        std::vector<Value> key;
        for (const auto &x : now_windowfunction.partition_by_) {
          key.push_back(x->Evaluate(&tuple, child_executor_->GetOutputSchema()));
        }
        if (now_windowfunction.order_by_.empty() && now_windowfunction.type_ != WindowFunctionType::Rank) {
          answer_[idx][i] = aht_[i].Find({key});
        }
      }
    }
  }
  now_idx_ = 0;
  end_idx_ = sort_tuple.size();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (now_idx_ == end_idx_) {
    return false;
  }
  *tuple = Tuple(answer_[now_idx_], &plan_->OutputSchema());
  now_idx_++;
  return true;
}
}  // namespace bustub
