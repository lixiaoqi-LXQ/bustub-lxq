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
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  StartWorkerThread();
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (uint i = 0; i < threads_num_; i++) {
    multi_queue_[i]->Put(std::nullopt);
  }
  for (auto &thread : multi_threads_) {
    thread.join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  auto tid = r.page_id_ % threads_num_;
  multi_queue_[tid]->Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  for (uint i = 0; i < threads_num_; i++) {
    auto queue = std::make_shared<Queue>();
    multi_queue_.push_back(queue);
    printf("main %dth loop create a queue %p\n", i, queue.get());
    multi_threads_.emplace_back([i, queue, this] {
      printf("thread-%d get a queue %p\n", i, queue.get());
      auto r = queue->Get();

      while (r.has_value()) {
        BUSTUB_ASSERT(r->page_id_ % threads_num_ == i, "thread takes charge of its own partition only");
        if (r->is_write_) {
          disk_manager_->WritePage(r->page_id_, r->data_);
        } else {
          disk_manager_->ReadPage(r->page_id_, r->data_);
        }
        r->callback_.set_value(true);
        r = queue->Get();
      }
    });
  }
}

}  // namespace bustub
