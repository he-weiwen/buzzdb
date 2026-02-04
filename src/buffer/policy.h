#pragma once

/**
 * @file policy.h
 * @brief Abstract interface for page replacement policies.
 *
 * Page replacement policies decide which page to evict when the buffer pool
 * is full and a new page needs to be loaded.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. MINIMAL INTERFACE
 *    The interface is clean and focused: touch() and evict(). This is good.
 *    However, it doesn't account for:
 *    - Pinned pages (pages that cannot be evicted)
 *    - Page priorities or hints from upper layers
 *
 * 2. NO THREAD SAFETY REQUIREMENT
 *    The interface doesn't specify thread safety requirements. Implementations
 *    may or may not be thread-safe, leading to confusion. Should be documented.
 *
 * 3. RETURN VALUE SEMANTICS
 *    touch() returns bool indicating "was already present". This is useful
 *    but the semantics could be clearer. Some policies might want to return
 *    more information (e.g., was it promoted from FIFO to LRU?).
 *
 * 4. EVICT THROWS ON EMPTY
 *    evict() throws when nothing can be evicted. An alternative would be
 *    returning std::optional<PageID>, but throwing is reasonable for an
 *    exceptional condition.
 *
 * 5. NO SIZE/CAPACITY QUERY
 *    No way to query how many pages are tracked or policy capacity.
 *
 * ============================================================================
 */

#include "common/types.h"

namespace buzzdb {

/**
 * @brief Abstract base class for page replacement policies.
 *
 * Implementations track which pages are in the buffer pool and decide
 * which page to evict when space is needed.
 */
class Policy {
public:
    virtual ~Policy() = default;

    /**
     * @brief Notify the policy that a page was accessed.
     * @param page_id The page that was accessed.
     * @return true if the page was already tracked by this policy,
     *         false if this is the first time seeing this page.
     *
     * CRITIQUE: Return value semantics are subtle. "Already tracked" vs
     * "first access" is policy-specific. For 2Q, this determines FIFO vs LRU.
     */
    virtual bool touch(PageID page_id) = 0;

    /**
     * @brief Select a page to evict.
     * @return The PageID of the page that should be evicted.
     * @throws std::runtime_error if no pages are available for eviction.
     *
     * CRITIQUE: Doesn't account for pinned pages. The BufferManager must
     * handle this externally, which leads to the more complex evict() overload
     * in TwoQPolicy that takes a state map.
     */
    virtual PageID evict() = 0;

    // Prevent copying
    Policy(const Policy&) = delete;
    Policy& operator=(const Policy&) = delete;

protected:
    Policy() = default;
};

}  // namespace buzzdb
