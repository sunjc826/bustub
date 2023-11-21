//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <mutex>
#include <optional>
#include <unordered_map>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) 
    : 
    replacer_size_(num_frames),
    node_store_(num_frames), // due to 1-indexing 
    k_(k),
    node_latches_(num_frames)
{
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool 
{
    std::lock_guard guard{global_latch_};
    while (not pq_.empty())
    {
        auto const top = pq_.top();
        pq_.pop();
        std::lock_guard guard2{node_latches_[top.frame_id_]};
        auto &opt_node = node_store_[top.frame_id_];
        if (not opt_node)
        {
            continue;
        }
        auto &node = *opt_node;
        if (node.timestamp_added_ > top.earliest_timestamp_) // top is from a removed node
        {
            continue;
        }

        if (node.history_.size() == k_)
        {
            if (top.kth_last_timestamp_ != node.history_.front()) // top is from an outdated node
            {
                pq_.emplace(top.frame_id_, k_, node);
                continue;
            }
        }
        else 
        {
            if (top.kth_last_timestamp_ != TIMESTAMP_NEG_INF or top.earliest_timestamp_ != node.history_.front()) // top is from an outdated node
            {
                pq_.emplace(top.frame_id_, k_, node);
                continue;
            }
        }

        if (not node.is_evictable_) // top is from an update to date but non-evictable node
        {
            node.present_in_pq_ = false;
            continue;
        }
        
        auto frame_id_to_evict = top.frame_id_;
        *frame_id = frame_id_to_evict;
        node_store_[frame_id_to_evict] = std::nullopt;
        std::lock_guard guard3{num_evictable_latch_};
        --num_evictable_;
        return true;
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) 
{
    if (static_cast<size_t>(frame_id) >= replacer_size_)
    {
        throw Exception("Invalid frame_id");
    }
    std::shared_lock shared_guard{global_latch_};
    std::lock_guard guard{node_latches_[frame_id]};
    auto &opt_node = node_store_[frame_id];
    if (not opt_node)
    {        
        node_store_[frame_id] = LRUKNode(current_timestamp_);
        pq_.emplace(frame_id, k_, *node_store_[frame_id]);
    }
    else 
    {
        auto &node = *opt_node;
        if (node.history_.size() == k_)
        {
            node.history_.pop();
        }
        node.history_.push(current_timestamp_);
    }
    ++current_timestamp_;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) 
{
    if (static_cast<size_t>(frame_id) >= replacer_size_)
    {
        throw Exception("Invalid frame_id");
    }
    std::shared_lock shared_guard{global_latch_};
    std::lock_guard guard{node_latches_[frame_id]};
    auto &opt_node = node_store_[frame_id];
    if (not opt_node)
    {
        throw Exception("Invalid frame_id");
    }
    auto &node = *opt_node;
    if (node.is_evictable_ == set_evictable)
    {
        return;
    }
    if ((node.is_evictable_ = set_evictable))
    {
        if (not node.present_in_pq_)
        {
            pq_.emplace(frame_id, k_, node);
            node.present_in_pq_ = true;
        }

        std::lock_guard guard2{num_evictable_latch_};
        ++num_evictable_;
    }
    else 
    {
        std::lock_guard guard2{num_evictable_latch_};
        --num_evictable_;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) 
{
    if (static_cast<size_t>(frame_id) >= replacer_size_)
    {
        throw Exception("Invalid frame_id");
    }
    std::shared_lock shared_guard{global_latch_};
    std::lock_guard guard{node_latches_[frame_id]};
    auto &opt_node = node_store_[frame_id];
    if (not opt_node)
    {
        throw Exception("Invalid frame_id");
    }
    if (not opt_node->is_evictable_)
    {
        throw Exception("Frame not evictable");
    }
    opt_node = std::nullopt;
}

auto LRUKReplacer::Size() -> size_t {
    std::shared_lock shared_guard{global_latch_};
    std::lock_guard guard{num_evictable_latch_};
    return num_evictable_;
}

}  // namespace bustub
