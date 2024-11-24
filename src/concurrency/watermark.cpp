#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  current_reads_[read_ts]++;
  if (current_reads_[read_ts] == 1) {
    current_reads_set_.insert(read_ts);
  }
  if (!current_reads_set_.empty()) {
    watermark_ = *current_reads_set_.begin();
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_set_.erase(current_reads_set_.find(read_ts));
    current_reads_.erase(current_reads_.find(read_ts));
  }
  if (!current_reads_set_.empty()) {
    watermark_ = *current_reads_set_.begin();
  }
}

}  // namespace bustub
