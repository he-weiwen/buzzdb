#pragma once

/**
 * @file buffer_manager.h
 * @brief BufferManager class - manages the buffer pool of in-memory pages.
 *
 * The BufferManager is the central component for page-level caching:
 * - Loads pages from disk on demand
 * - Keeps frequently-used pages in memory
 * - Evicts pages when the buffer pool is full
 * - Ensures dirty pages are written to disk
 * - Provides concurrency control for page access
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. MIXED RESPONSIBILITIES
 *    BufferManager handles:
 *    - Page loading/flushing (I/O)
 *    - Replacement policy (eviction)
 *    - Concurrency control (locking)
 *    - Pin counting (reference tracking)
 *    These could be separated into distinct classes for better modularity.
 *
 * 2. SPINNING IN fix_page
 *    When a page is locked, fix_page spins with yield() until it can acquire
 *    the lock. This is simple but potentially wasteful. A condition variable
 *    or lock queue would be more efficient under contention.
 *
 * 3. PAGE STATE TRACKING
 *    Page states (UNFIXED, EXCLUSIVE, shared count) are tracked in a separate
 *    map from the frames themselves. This duplication is error-prone.
 *    Better: embed state in BufferFrame and use atomic operations.
 *
 * 4. SINGLE GLOBAL MUTEX
 *    One mutex guards all metadata operations. This limits scalability.
 *    Better: per-page latches or striped locking.
 *
 * 5. NO READ-YOUR-WRITES GUARANTEE
 *    If a page is evicted and re-loaded, modifications are persisted.
 *    But there's no write-ahead logging (WAL), so crash recovery is impossible.
 *    This is expected for this educational implementation.
 *
 * 6. FIXED CAPACITY
 *    Buffer pool size is fixed at construction. No dynamic resizing.
 *    This is typical for database buffer managers.
 *
 * 7. HARDCODED POLICY
 *    Uses TwoQPolicy directly. A cleaner design would accept a Policy*
 *    in the constructor for dependency injection.
 *
 * 8. DESTRUCTOR FLUSH
 *    Destructor flushes all dirty pages. This is good for correctness
 *    but doesn't handle partial failures gracefully.
 *
 * ============================================================================
 */

#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <cassert>

#include "common/config.h"
#include "common/types.h"
#include "storage/storage_manager.h"
#include "buffer/buffer_frame.h"
#include "buffer/two_q_policy.h"

namespace buzzdb {

/**
 * @brief Manages the buffer pool of pages in memory.
 *
 * Provides fix_page/unfix_page interface for accessing pages with proper
 * locking and dirty tracking. Uses 2Q replacement policy.
 */
class BufferManager {
private:
    /// Storage manager for disk I/O
    StorageManager storage_manager_;

    /// The buffer pool - array of frame slots
    /// CRITIQUE: vector of unique_ptr means frames are heap-allocated separately.
    /// A single contiguous allocation would be more cache-friendly.
    std::vector<std::unique_ptr<BufferFrame>> buffer_pool_;

    /// Replacement policy
    /// CRITIQUE: Should be injected, not hardcoded
    TwoQPolicy policy_;

    /// List of empty (unused) frame slots
    std::vector<FrameID> empty_slots_;

    /// Map from PageID to FrameID for pages currently in buffer
    std::unordered_map<PageID, FrameID> frame_of_page_;

    /// Map from PageID to its current lock state
    /// CRITIQUE: Duplicates information that could be in BufferFrame
    std::unordered_map<PageID, PageState> page_state_;

    /// Global mutex for buffer manager metadata
    /// CRITIQUE: Single mutex limits concurrency
    std::mutex global_mutex_;

    /// Maximum number of pages in the buffer pool
    size_t capacity_;

public:
    // -------------------------------------------------------------------------
    // CONSTRUCTOR / DESTRUCTOR
    // -------------------------------------------------------------------------

    /**
     * @brief Construct a BufferManager with specified capacity.
     * @param capacity Maximum number of pages to hold in memory.
     * @param truncate_storage If true, truncate the database file.
     */
    explicit BufferManager(size_t capacity = MAX_PAGES_IN_MEMORY,
                           bool truncate_storage = false)
        : storage_manager_(truncate_storage)
        , capacity_(capacity) {
        // Preallocate frame slots
        buffer_pool_.resize(capacity_);

        // All slots start empty
        for (FrameID i = 0; i < capacity_; ++i) {
            empty_slots_.push_back(i);
        }

        // NOTE: Removed eager extension to MAX_PAGES. Storage is extended
        // on demand by InsertOperator or explicit extend() calls.
        // The original lab code extended eagerly, but this makes ScanOperator
        // iterate through 1000+ empty pages.
    }

    /**
     * @brief Destructor - flushes all dirty pages to disk.
     */
    ~BufferManager() {
        for (FrameID i = 0; i < buffer_pool_.size(); ++i) {
            if (buffer_pool_[i] != nullptr) {
                flush_frame(i);
            }
        }
    }

    // Non-copyable, non-movable
    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    BufferManager(BufferManager&&) = delete;
    BufferManager& operator=(BufferManager&&) = delete;

    // -------------------------------------------------------------------------
    // PAGE ACCESS INTERFACE
    // -------------------------------------------------------------------------

    /**
     * @brief Fix (pin) a page in memory and return a reference to its frame.
     * @param page_id The page to access.
     * @param exclusive If true, acquire exclusive (write) lock; otherwise shared (read).
     * @return Reference to the BufferFrame holding the page.
     * @throws buffer_full_error if buffer is full and all pages are pinned.
     *
     * The page will remain in memory until unfix_page is called.
     * Multiple threads can hold shared locks on the same page.
     * Only one thread can hold an exclusive lock.
     *
     * CRITIQUE: Spinning with yield() is inefficient. Should use condition variables.
     */
    BufferFrame& fix_page(PageID page_id, bool exclusive) {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(global_mutex_);

                auto state_it = page_state_.find(page_id);
                if (state_it == page_state_.end()) {
                    // Page not in buffer - need to load it
                    return load_page_locked(page_id, exclusive);
                } else {
                    // Page is in buffer - try to acquire lock
                    FrameID frame_id = frame_of_page_[page_id];
                    bool lock_acquired = false;

                    if (exclusive) {
                        lock_acquired = buffer_pool_[frame_id]->mutex.try_lock();
                        if (lock_acquired) {
                            // Exclusive lock requires page to be unfixed
                            assert(page_state_[page_id] == PAGE_UNFIXED);
                            page_state_[page_id] = PAGE_EXCLUSIVE;
                        }
                    } else {
                        lock_acquired = buffer_pool_[frame_id]->mutex.try_lock_shared();
                        if (lock_acquired) {
                            // Shared lock - increment count
                            assert(page_state_[page_id] >= 0);
                            ++page_state_[page_id];
                        }
                    }

                    if (lock_acquired) {
                        policy_.touch(page_id);
                        return *buffer_pool_[frame_id];
                    }
                    // Lock not acquired, will retry
                }
            }
            // Give other threads a chance
            std::this_thread::yield();
        }
    }

    /**
     * @brief Unfix (unpin) a page, releasing the lock.
     * @param frame Reference to the BufferFrame to release.
     * @param is_dirty If true, mark the page as modified.
     *
     * After unfixing, the page may be evicted at any time.
     * If is_dirty is true, the page will be written to disk before eviction.
     */
    void unfix_page(BufferFrame& frame, bool is_dirty) {
        if (is_dirty) {
            frame.set_dirty();
        }

        PageID page_id = frame.get_page_id();
        bool was_exclusive;

        // Must hold global_mutex_ while modifying page_state_ to avoid
        // data race with fix_page which reads page_state_ under this lock.
        {
            std::unique_lock<std::mutex> lock(global_mutex_);

            if (page_state_[page_id] == PAGE_EXCLUSIVE) {
                page_state_[page_id] = PAGE_UNFIXED;
                was_exclusive = true;
            } else if (page_state_[page_id] > 0) {
                --page_state_[page_id];
                was_exclusive = false;
            } else {
                throw std::runtime_error(
                    "BufferManager::unfix_page called on unfixed page");
            }
        }
        // Release global_mutex_ BEFORE page mutex to maintain lock ordering
        // (fix_page acquires global_mutex_ first, then page mutex)

        if (was_exclusive) {
            frame.mutex.unlock();
        } else {
            frame.mutex.unlock_shared();
        }
    }

    // -------------------------------------------------------------------------
    // UTILITY METHODS
    // -------------------------------------------------------------------------

    /**
     * @brief Flush a specific page to disk if dirty.
     * @param frame_id The frame to flush.
     */
    void flush_frame(FrameID frame_id) {
        if (buffer_pool_[frame_id] != nullptr && buffer_pool_[frame_id]->is_dirty()) {
            storage_manager_.flush(buffer_pool_[frame_id]->get_page_id(),
                                   buffer_pool_[frame_id]->page);
            buffer_pool_[frame_id]->clear_dirty();
        }
    }

    /**
     * @brief Extend the underlying storage.
     */
    void extend() {
        storage_manager_.extend();
    }

    /**
     * @brief Get the number of pages in the database file.
     */
    size_t getNumPages() const {
        return storage_manager_.getNumPages();
    }

    /**
     * @brief Get the buffer pool capacity.
     */
    size_t getCapacity() const {
        return capacity_;
    }

    // -------------------------------------------------------------------------
    // METHODS FOR TESTING (expose policy state)
    // -------------------------------------------------------------------------

    /// Get pages in FIFO queue (for testing).
    std::vector<PageID> get_fifo_list() const {
        return policy_.get_fifo_list();
    }

    /// Get pages in LRU queue (for testing).
    std::vector<PageID> get_lru_list() const {
        return policy_.get_lru_list();
    }

private:
    // -------------------------------------------------------------------------
    // INTERNAL METHODS
    // -------------------------------------------------------------------------

    /**
     * @brief Load a page into the buffer (called with global_mutex_ held).
     * @param page_id The page to load.
     * @param exclusive Whether to acquire exclusive lock.
     * @return Reference to the BufferFrame holding the loaded page.
     * @throws buffer_full_error if no frame can be evicted.
     */
    BufferFrame& load_page_locked(PageID page_id, bool exclusive) {
        FrameID slot;

        if (empty_slots_.empty()) {
            // Need to evict a page
            PageID victim_page = policy_.evict(page_state_);
            FrameID victim_frame = frame_of_page_[victim_page];

            // Flush victim if dirty
            if (buffer_pool_[victim_frame]->is_dirty()) {
                storage_manager_.flush(victim_page, buffer_pool_[victim_frame]->page);
            }

            // Clean up victim's metadata
            page_state_.erase(victim_page);
            frame_of_page_.erase(victim_page);
            empty_slots_.push_back(victim_frame);
        }

        // Get an empty slot
        slot = empty_slots_.back();
        empty_slots_.pop_back();

        // Auto-extend storage if page doesn't exist yet
        if (page_id >= storage_manager_.getNumPages()) {
            storage_manager_.extend(page_id);
        }

        // Load page from disk
        buffer_pool_[slot] = std::make_unique<BufferFrame>(
            storage_manager_.load(page_id), page_id);

        // Acquire lock on new frame
        if (exclusive) {
            buffer_pool_[slot]->mutex.lock();
            page_state_[page_id] = PAGE_EXCLUSIVE;
        } else {
            buffer_pool_[slot]->mutex.lock_shared();
            page_state_[page_id] = 1;
        }

        // Update metadata
        frame_of_page_[page_id] = slot;
        policy_.touch(page_id);

        return *buffer_pool_[slot];
    }
};

}  // namespace buzzdb
