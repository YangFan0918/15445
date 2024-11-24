//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>

#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  auto res = std::make_optional<DiskRequest>(r.is_write_, r.data_, r.page_id_, std::move(r.callback_));
  request_queue_.Put(std::move(res));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto res = request_queue_.Get();
    if (res == std::nullopt) {
      break;
    }
    if (res->is_write_) {
      disk_manager_->WritePage(res->page_id_, res->data_);
      res->callback_.set_value(true);
    } else {
      disk_manager_->ReadPage(res->page_id_, res->data_);
      res->callback_.set_value(true);
    }
  }
}

}  // namespace bustub
