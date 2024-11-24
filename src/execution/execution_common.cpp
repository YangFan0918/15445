#include "execution/execution_common.h"
#include <optional>
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values;
  uint columns_num = schema->GetColumnCount();
  for (uint i = 0; i < columns_num; i++) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }
  bool is_deaded = base_meta.is_deleted_;
  for (const auto &undolog : undo_logs) {
    std::vector<uint> cols;
    for (uint i = 0; i < columns_num; i++) {
      if (undolog.modified_fields_[i]) {
        cols.emplace_back(i);
      }
    }
    is_deaded = undolog.is_deleted_;
    auto now_schema = Schema::CopySchema(schema, cols);
    for (uint i = 0; i < cols.size(); i++) {
      values[cols[i]] = undolog.tuple_.GetValue(&now_schema, i);
    }
  }
  if (is_deaded) {
    return std::nullopt;
  }
  return Tuple(values, schema);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  auto iterator = table_info->table_->MakeIterator();
  while (!iterator.IsEnd()) {
    auto rid = iterator.GetRID();
    auto tuple = iterator.GetTuple().second;
    auto tuplemate_now = table_info->table_->GetTupleMeta(rid);
    fmt::println(stderr, "RID {}/{} ts={} tuple={} waterMark={}", rid.GetPageId(), rid.GetSlotNum(), tuplemate_now.ts_,
                 tuple.ToString(&table_info->schema_), txn_mgr->GetWatermark());
    ++iterator;
  }
  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
