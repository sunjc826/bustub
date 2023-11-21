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
#include <thread>
#include <type_traits>
#include <utility>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
    // Spawn the background thread
    background_thread_.emplace([&] { StartWorkerThread(); });
    for (size_t i{}; i < NUM_WORKERS; ++i)
    {
        workers_[i].emplace(disk_manager);
    }
}

WorkerShard::WorkerShard(DiskManager *disk_manager) 
    : 
    disk_manager_(disk_manager), 
    worker_thread_([&]{ StartWorkerThread(); })
{
}

DiskScheduler::~DiskScheduler() {
    // Put a `std::nullopt` in the queue to signal to exit the loop
    request_queue_.Put(std::nullopt);
    if (background_thread_.has_value()) {
        background_thread_->join();
    }
    for (auto &worker : workers_)
    {
        worker->request_queue_.Put(std::nullopt);
        worker->worker_thread_.join(); // since we are using c++17, we don't have access to c++20 jthread
    }
}

void DiskScheduler::Schedule(DiskRequest r) {
    request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
    while (true)
    {
        auto opt_disk_request = request_queue_.Get();
        if (not opt_disk_request) {
            break;
        }
        auto &disk_request = *opt_disk_request;
        workers_[ShardHash(disk_request.page_id_)]->request_queue_.Put(std::move(opt_disk_request));
    }
}

auto DiskScheduler::ShardHash(size_t page_id) -> size_t
{
    return page_id % NUM_WORKERS;
}

void WorkerShard::StartWorkerThread()
{
    while (true)
    {
        auto opt_disk_request = request_queue_.Get();
        if (not opt_disk_request) {
            break;
        }
        auto &disk_request = *opt_disk_request;
        if (disk_request.is_write_)
        {
            disk_manager_->WritePage(disk_request.page_id_, disk_request.data_);
        }
        else 
        {
            disk_manager_->ReadPage(disk_request.page_id_, disk_request.data_);
        }
        disk_request.callback_.set_value(true);
    }
}

}  // namespace bustub
