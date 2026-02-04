#pragma once

/**
 * @file lru_policy.h
 * @brief Least Recently Used (LRU) page replacement policy.
 *
 * LRU evicts the page that hasn't been accessed for the longest time.
 * Good for workloads with temporal locality (recently used pages are
 * likely to be used again).
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. SCAN RESISTANCE
 *    LRU is vulnerable to sequential scans. A single scan of N pages will
 *    evict all cached pages if N > buffer size, even if those pages were
 *    frequently accessed. This is why 2Q exists.
 *
 * 2. O(1) OPERATIONS
 *    Using list + unordered_map gives O(1) touch and evict, which is good.
 *    The trade-off is memory overhead for the map entries.
 *
 * 3. NOT THREAD-SAFE
 *    This implementation is not thread-safe. The BufferManager must provide
 *    external synchronization. This is reasonable for a policy class.
 *
 * 4. NO PINNED PAGE AWARENESS
 *    Like the base Policy, this doesn't know about pinned pages.
 *    The BufferManager must filter eviction candidates.
 *
 * ============================================================================
 */

#include <list>
#include <unordered_map>
#include <stdexcept>

#include "buffer/policy.h"
#include "common/types.h"

namespace buzzdb {

/**
 * @brief LRU page replacement policy implementation.
 *
 * Maintains a doubly-linked list where the front is least-recently-used
 * and the back is most-recently-used. A hash map provides O(1) lookup.
 */
class LRUPolicy : public Policy {
private:
    /// List of page IDs, front = LRU (evict first), back = MRU (keep)
    std::list<PageID> lru_list_;

    /// Map from PageID to iterator in lru_list_ for O(1) access
    std::unordered_map<PageID, std::list<PageID>::iterator> page_map_;

public:
    LRUPolicy() = default;
    ~LRUPolicy() override = default;

    /**
     * @brief Record an access to a page.
     * @param page_id The accessed page.
     * @return true if page was already in the LRU list, false if newly added.
     *
     * If the page exists, moves it to the back (most recently used).
     * If new, adds it to the back.
     */
    bool touch(PageID page_id) override {
        auto it = page_map_.find(page_id);
        if (it != page_map_.end()) {
            // Move existing page to back (MRU position)
            lru_list_.splice(lru_list_.end(), lru_list_, it->second);
            // Iterator remains valid after splice, no map update needed
            return true;
        }

        // New page - add to back
        lru_list_.push_back(page_id);
        page_map_[page_id] = --lru_list_.end();
        return false;
    }

    /**
     * @brief Evict the least recently used page.
     * @return PageID of the evicted page.
     * @throws std::runtime_error if no pages to evict.
     */
    PageID evict() override {
        if (lru_list_.empty()) {
            throw std::runtime_error("LRUPolicy::evict() called on empty list");
        }

        PageID victim = lru_list_.front();
        page_map_.erase(victim);
        lru_list_.pop_front();
        return victim;
    }

    // -------------------------------------------------------------------------
    // ADDITIONAL METHODS (not in base Policy interface)
    // -------------------------------------------------------------------------

    /// Check if a page is tracked by this policy.
    bool contains(PageID page_id) const {
        return page_map_.find(page_id) != page_map_.end();
    }

    /// Get number of pages tracked.
    size_t size() const {
        return lru_list_.size();
    }

    /// Check if empty.
    bool empty() const {
        return lru_list_.empty();
    }

    /// Get list of pages in LRU order (front = least recent).
    std::vector<PageID> get_list() const {
        return std::vector<PageID>(lru_list_.begin(), lru_list_.end());
    }

    /// Remove a specific page from tracking (e.g., when evicted externally).
    void remove(PageID page_id) {
        auto it = page_map_.find(page_id);
        if (it != page_map_.end()) {
            lru_list_.erase(it->second);
            page_map_.erase(it);
        }
    }
};

}  // namespace buzzdb
