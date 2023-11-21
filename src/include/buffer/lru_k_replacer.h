//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <set>
#include <vector>
#include <queue>
#include <optional>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

struct LRUKNode {
    explicit LRUKNode(size_t timestamp_added) : timestamp_added_(timestamp_added) {
        history_.push(timestamp_added);
    }
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

    std::queue<size_t> history_;
    size_t timestamp_added_;
    bool is_evictable_{false};
    bool present_in_pq_{true};
};



/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:

    static size_t constexpr TIMESTAMP_NEG_INF = 0;
    struct PQNode
    {
        frame_id_t frame_id_;
        // if fewer than k timestamps, this will be TIMESTAMP_NEG_INF
        size_t kth_last_timestamp_;
        size_t earliest_timestamp_;
        
        explicit PQNode(frame_id_t frame_id, size_t k, LRUKNode const &lruk_node)
            : 
            frame_id_(frame_id), 
            kth_last_timestamp_(lruk_node.history_.size() == k ? lruk_node.history_.front() : TIMESTAMP_NEG_INF),
            earliest_timestamp_(lruk_node.history_.front())
        {
        }

        // We want to prioritize the node with the earliest `kth_last_timestamp_`,
        // then break ties by earliest `earliest_timestamp_`.
        // Need to break ties since kth_last_timestamp_ can be TIMESTAMP_NEG_INF for multiple nodes.
        auto operator<(PQNode const &other) const noexcept -> bool
        {
            return std::tie(kth_last_timestamp_, earliest_timestamp_) > std::tie(other.kth_last_timestamp_, other.earliest_timestamp_);
        }
    };
    
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
    size_t const replacer_size_;
    std::vector<std::optional<LRUKNode>> node_store_;
    std::priority_queue<PQNode> pq_;
    size_t current_timestamp_{1};
    size_t k_;
    size_t num_evictable_{};
    std::shared_mutex global_latch_;
    std::mutex num_evictable_latch_;
    std::mutex current_timestamp_latch_;
    std::vector<std::mutex> node_latches_;
};

}  // namespace bustub
