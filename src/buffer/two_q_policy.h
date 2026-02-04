#pragma once

/**
 * @file two_q_policy.h
 * @brief 2Q (Two-Queue) page replacement policy.
 *
 * 2Q improves on LRU by being resistant to sequential scans:
 * - First access: page goes into FIFO queue
 * - Second access: page is promoted to LRU queue
 * - Eviction: prefer FIFO queue first, then LRU
 *
 * This prevents a sequential scan from evicting frequently-used pages,
 * since scan pages only get one access and stay in FIFO.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. SIMPLIFIED 2Q
 *    This is a simplified version of 2Q. The original paper by Johnson & Shasha
 *    (1994) uses three queues: A1in, A1out, and Am. A1out is a "ghost" queue
 *    that remembers recently evicted pages. This implementation skips A1out.
 *
 * 2. NO QUEUE SIZE LIMITS
 *    The original 2Q specifies that A1in (FIFO) should be limited to a fraction
 *    of the total buffer (e.g., 25%). This implementation doesn't enforce that,
 *    leaving it to the BufferManager's total capacity limit.
 *
 * 3. EVICT WITH STATE MAP
 *    The evict(state_map) method couples the policy to BufferManager's internal
 *    PageState representation. This was constrained by the lab scaffolding.
 *
 *    ALTERNATIVE DESIGNS (for future refactoring):
 *
 *    (a) Predicate callback - policy doesn't know WHY, just IF:
 *        PageID evict(std::function<bool(PageID)> can_evict) {
 *            for (auto it = fifo_queue_.begin(); it != fifo_queue_.end(); ++it)
 *                if (can_evict(*it)) { ...evict... }
 *        }
 *        Caller: policy_.evict([&](PageID p) { return state_[p] == UNFIXED; });
 *
 *    (b) Policy tracks pin state itself:
 *        void pin(PageID);   -- called by BufferManager on fix
 *        void unpin(PageID); -- called by BufferManager on unfix
 *        PageID evict();     -- internally skips pinned pages
 *
 *    (c) Candidate iterator - return candidates, let caller filter:
 *        std::optional<PageID> next_eviction_candidate();
 *        void confirm_evicted(PageID);  -- actually removed
 *        void skip(PageID);             -- couldn't evict, try next
 *
 * 4. PROMOTION ON TOUCH
 *    A page in FIFO is promoted to LRU on *any* subsequent touch. Some variants
 *    require multiple touches or a time delay before promotion.
 *
 * 5. NOT THREAD-SAFE
 *    Same as LRUPolicy - external synchronization required.
 *
 * ============================================================================
 */

#include <list>
#include <unordered_map>
#include <vector>
#include <stdexcept>

#include "buffer/policy.h"
#include "common/types.h"

namespace buzzdb {

/// Page state constants (matching original lab code)
/// CRITIQUE: These should perhaps be in a separate header or enum class.
using PageState = int;
constexpr PageState PAGE_UNFIXED = 0;
constexpr PageState PAGE_EXCLUSIVE = -1;
// Positive values indicate shared lock count

/**
 * @brief Custom exception for buffer full condition.
 *
 * CRITIQUE: This is a policy-level exception but really belongs at the
 * BufferManager level. However, the evict(state_map) method needs to
 * signal "no unfixed pages available", which is effectively "buffer full".
 */
class buffer_full_error : public std::exception {
public:
    const char* what() const noexcept override {
        return "Buffer is full: all pages are pinned";
    }
};

/**
 * @brief 2Q page replacement policy implementation.
 *
 * Maintains two queues:
 * - FIFO: pages accessed exactly once (candidates for eviction)
 * - LRU: pages accessed multiple times (protected from scans)
 */
class TwoQPolicy : public Policy {
private:
    /// FIFO queue for first-access pages
    std::list<PageID> fifo_queue_;
    std::unordered_map<PageID, std::list<PageID>::iterator> fifo_map_;

    /// LRU queue for re-accessed pages
    std::list<PageID> lru_queue_;
    std::unordered_map<PageID, std::list<PageID>::iterator> lru_map_;

public:
    TwoQPolicy() = default;
    ~TwoQPolicy() override = default;

    /**
     * @brief Record an access to a page.
     * @param page_id The accessed page.
     * @return true if page was already tracked, false if newly added.
     *
     * Behavior:
     * - If in FIFO: promote to LRU (end), return true
     * - If in LRU: move to end (MRU), return true
     * - If new: add to FIFO, return false
     */
    bool touch(PageID page_id) override {
        // Check if in FIFO
        auto fifo_it = fifo_map_.find(page_id);
        if (fifo_it != fifo_map_.end()) {
            // Promote from FIFO to LRU
            // Use splice to move the node directly (efficient, no copy)
            lru_queue_.splice(lru_queue_.end(), fifo_queue_, fifo_it->second);
            fifo_map_.erase(page_id);
            lru_map_[page_id] = --lru_queue_.end();
            return true;
        }

        // Check if in LRU
        auto lru_it = lru_map_.find(page_id);
        if (lru_it != lru_map_.end()) {
            // Move to end (MRU position)
            lru_queue_.splice(lru_queue_.end(), lru_queue_, lru_it->second);
            // Iterator remains valid after splice within same list
            return true;
        }

        // New page - add to FIFO
        fifo_queue_.push_back(page_id);
        fifo_map_[page_id] = --fifo_queue_.end();
        return false;
    }

    /**
     * @brief Evict a page (simple version, ignores pin state).
     * @return PageID of the evicted page.
     * @throws std::runtime_error if both queues are empty.
     *
     * CRITIQUE: This simple evict() is rarely useful in practice because
     * it doesn't account for pinned pages.
     */
    PageID evict() override {
        if (!fifo_queue_.empty()) {
            PageID victim = fifo_queue_.front();
            fifo_map_.erase(victim);
            fifo_queue_.pop_front();
            return victim;
        }

        if (!lru_queue_.empty()) {
            PageID victim = lru_queue_.front();
            lru_map_.erase(victim);
            lru_queue_.pop_front();
            return victim;
        }

        throw std::runtime_error("TwoQPolicy::evict() called on empty queues");
    }

    /**
     * @brief Evict a page, respecting pin states.
     * @param state_of_page Map from PageID to its current pin state.
     * @return PageID of the evicted page.
     * @throws buffer_full_error if all pages are pinned.
     *
     * CRITIQUE: Couples policy to PageState internals. This design was
     * constrained by the lab scaffolding. See file header comment #3 for
     * alternative designs (predicate callback, policy-tracked pins, or
     * candidate iterator pattern).
     */
    PageID evict(const std::unordered_map<PageID, PageState>& state_of_page) {
        // Try FIFO first
        for (auto it = fifo_queue_.begin(); it != fifo_queue_.end(); ++it) {
            auto state_it = state_of_page.find(*it);
            if (state_it != state_of_page.end() && state_it->second == PAGE_UNFIXED) {
                PageID victim = *it;
                fifo_map_.erase(victim);
                fifo_queue_.erase(it);
                return victim;
            }
        }

        // Then try LRU
        for (auto it = lru_queue_.begin(); it != lru_queue_.end(); ++it) {
            auto state_it = state_of_page.find(*it);
            if (state_it != state_of_page.end() && state_it->second == PAGE_UNFIXED) {
                PageID victim = *it;
                lru_map_.erase(victim);
                lru_queue_.erase(it);
                return victim;
            }
        }

        throw buffer_full_error();
    }

    // -------------------------------------------------------------------------
    // ADDITIONAL METHODS (for testing and debugging)
    // -------------------------------------------------------------------------

    /// Get FIFO queue contents in order.
    std::vector<PageID> get_fifo_list() const {
        return std::vector<PageID>(fifo_queue_.begin(), fifo_queue_.end());
    }

    /// Get LRU queue contents in order (front = least recent).
    std::vector<PageID> get_lru_list() const {
        return std::vector<PageID>(lru_queue_.begin(), lru_queue_.end());
    }

    /// Check if a page is tracked.
    bool contains(PageID page_id) const {
        return fifo_map_.count(page_id) > 0 || lru_map_.count(page_id) > 0;
    }

    /// Total pages tracked.
    size_t size() const {
        return fifo_queue_.size() + lru_queue_.size();
    }

    /// Remove a page from tracking.
    void remove(PageID page_id) {
        auto fifo_it = fifo_map_.find(page_id);
        if (fifo_it != fifo_map_.end()) {
            fifo_queue_.erase(fifo_it->second);
            fifo_map_.erase(fifo_it);
            return;
        }

        auto lru_it = lru_map_.find(page_id);
        if (lru_it != lru_map_.end()) {
            lru_queue_.erase(lru_it->second);
            lru_map_.erase(lru_it);
        }
    }

    // Debug printing
    void print_fifo() const {
        std::cout << "FIFO: ";
        for (const auto& pid : fifo_queue_) {
            std::cout << pid << " ";
        }
        std::cout << "\n";
    }

    void print_lru() const {
        std::cout << "LRU: ";
        for (const auto& pid : lru_queue_) {
            std::cout << pid << " ";
        }
        std::cout << "\n";
    }
};

}  // namespace buzzdb
