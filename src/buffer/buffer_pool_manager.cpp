//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <algorithm>
#include <cstddef>
#include <mutex>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager), pages_latch_(pool_size) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// Before calling this, need to acquire exclusive lock on global_latch_
auto BufferPoolManager::FindFreeFrame() -> std::optional<frame_id_t>
{
    std::optional<frame_id_t> frame_id;
    if (not free_list_.empty())
    {
        frame_id.emplace(free_list_.front());
        free_list_.pop_front();
    }
    else
    {
        if (frame_id_t evicted_frame_id; replacer_->Evict(&evicted_frame_id))
        {
            auto &page = pages_[evicted_frame_id];
            if (page.is_dirty_)
            {
                FlushPage(page);
            }
            page_table_.erase(page.page_id_);
            frame_id.emplace(evicted_frame_id);
        }
    }

    return frame_id;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * 
{ 
    std::unique_lock guard{global_latch_};
    auto frame_id = FindFreeFrame();
    if (not frame_id)
    {
        return nullptr;
    }
    *page_id = AllocatePage();
    page_table_.emplace(*page_id, *frame_id);

    std::lock_guard guard2{pages_latch_[*frame_id]};
    replacer_->RecordAccess(*frame_id);
    replacer_->SetEvictable(*frame_id, false);
    guard.unlock();
    
    auto &page = pages_[*frame_id];
    page.page_id_ = *page_id;
    page.is_dirty_ = false;
    ++page.pin_count_;
    return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
    std::shared_lock shared_guard{global_latch_};
    auto it = page_table_.find(page_id);
    frame_id_t frame_id;
    if (it == page_table_.end())
    {
        shared_guard.unlock();
        {
            std::unique_lock guard{global_latch_}; // double checked locking
            it = page_table_.find(page_id);
            if (it == page_table_.end())
            {
                auto opt_frame_id = FindFreeFrame();
                if (not opt_frame_id)
                {
                    return nullptr;
                }
                frame_id = *opt_frame_id;
                std::promise<bool> p;
                std::future<bool> f = p.get_future();
                page_table_.emplace(page_id, frame_id);
                disk_scheduler_->Schedule(DiskRequest{
                    .is_write_ = false,
                    .data_ = pages_[frame_id].data_,
                    .page_id_ = page_id,
                    .callback_ = std::move(p)
                });
                BUSTUB_ASSERT(f.get(), "Assume successful page fetch from disk");

                std::lock_guard guard2{pages_latch_[frame_id]};
                replacer_->RecordAccess(frame_id, access_type);
                replacer_->SetEvictable(frame_id, false);
                guard.unlock();

                auto &page = pages_[frame_id];
                ++page.pin_count_;
                page.page_id_ = page_id;
                page.is_dirty_ = false;
            }
            else 
            {
                frame_id = it->second;
                std::lock_guard guard2{pages_latch_[frame_id]};
                replacer_->RecordAccess(frame_id, access_type);
                replacer_->SetEvictable(frame_id, false);
                guard.unlock();
                
                auto &page = pages_[frame_id];
                ++page.pin_count_;

            }
        }
    }
    else 
    {
        frame_id = it->second;

        std::lock_guard guard{pages_latch_[frame_id]};
        replacer_->RecordAccess(frame_id, access_type);
        replacer_->SetEvictable(frame_id, false);
        shared_guard.unlock();
        auto &page = pages_[frame_id];
        ++page.pin_count_;
    }
    
    return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
    std::shared_lock shared_guard{global_latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        return false;
    }
    auto frame_id = it->second;

    std::lock_guard guard{pages_latch_[frame_id]};
    // we can't unlock shared_guard here because we might want to call SetEvictable later

    auto &page = pages_[frame_id];
    if (page.pin_count_ <= 0)
    {
        return false;
    }
    page.is_dirty_ = page.is_dirty_ or is_dirty;
    if (--page.pin_count_ == 0)
    {
        replacer_->SetEvictable(frame_id, true);
    }
    return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool 
{
    std::shared_lock shared_guard{global_latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        return false;
    }
    auto frame_id = it->second;
    std::lock_guard guard2{pages_latch_[frame_id]};
    shared_guard.unlock();
    FlushPage(pages_[frame_id]);
    return true;
}

// Precondition: The pages_latch_ entry corresponding to page must have been locked
void BufferPoolManager::FlushPage(Page &page)
{
    std::promise<bool> p;
    std::future f = p.get_future();
    disk_scheduler_->Schedule(DiskRequest{
        .is_write_ = true,
        .data_ = page.data_,
        .page_id_ = page.page_id_,
        .callback_ = std::move(p),
    });
    BUSTUB_ASSERT(f.get(), "Assume always successful disk write");
    page.is_dirty_ = false;
}

void BufferPoolManager::FlushAllPages() {
    std::vector<std::future<bool>> futures;
    futures.reserve(pool_size_);
    std::vector<std::lock_guard<std::mutex>> guards(pages_latch_.begin(), pages_latch_.end()); 
    for (size_t i{}; i < pool_size_; ++i)
    {
        std::promise<bool> p;
        futures.push_back(p.get_future());
        auto &page = pages_[i];
        disk_scheduler_->Schedule(DiskRequest{
            .is_write_ = true,
            .data_ = page.data_,
            .page_id_ = page.page_id_,
            .callback_ = std::move(p),
        });
    }

    for (size_t i{}; i < pool_size_; ++i)
    {
        BUSTUB_ASSERT(futures[i].get(), "Assume always successful disk write");
        pages_[i].is_dirty_ = false;
    }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool 
{
    std::shared_lock shared_guard{global_latch_};
    auto it = page_table_.find(page_id);
    if (it == page_table_.end())
    {
        return true;
    }
    auto frame_id = it->second;
    shared_guard.unlock();
    std::lock_guard guard{global_latch_};
    std::lock_guard guard2{pages_latch_[frame_id]};
    auto &page = pages_[frame_id];
    if (page.page_id_ != page_id) // double checked locking, we could also do another page_table_.find() but that is slower
    {
        return true;
    }

    if (page.pin_count_ > 0)
    {
        return false;
    }

    page_table_.erase(it);
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    page.ResetMemory();
    DeallocatePage(page_id);
    return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
