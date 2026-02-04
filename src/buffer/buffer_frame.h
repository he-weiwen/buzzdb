#pragma once

/**
 * @file buffer_frame.h
 * @brief BufferFrame class - wrapper for a page in the buffer pool.
 *
 * A BufferFrame holds a page currently loaded in memory, along with
 * metadata about its state (dirty flag, page ID) and synchronization
 * primitives for concurrent access.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. PUBLIC PAGE POINTER
 *    The `page` member is public, allowing direct access to the SlottedPage.
 *    This breaks encapsulation but is necessary for the current BufferManager
 *    design where fix_page returns a reference and users access page directly.
 *    A cleaner design would have BufferFrame provide page access methods.
 *
 * 2. PUBLIC MUTEX
 *    The mutex is public so BufferManager can lock/unlock it directly.
 *    This is fragile - a user could accidentally unlock the mutex.
 *    Better: make BufferManager a friend and mutex private.
 *
 * 3. NO FRAME ID
 *    Original lab code had frame_id but commented it out. Having a frame ID
 *    would make debugging easier and allow frames to identify themselves.
 *
 * 4. DIRTY FLAG MANAGEMENT
 *    Only set_dirty() is provided, no clear_dirty(). The flag is private
 *    so only BufferManager can read/clear it (via friendship or direct access).
 *    This is actually reasonable - users shouldn't clear dirty flags.
 *
 * 5. NO PIN COUNT
 *    Pin count is maintained in BufferManager's page_state map, not here.
 *    Having it in BufferFrame would be more encapsulated but would require
 *    synchronization for the count itself.
 *
 * 6. SHARED_MUTEX FOR READ/WRITE LOCKS
 *    Using std::shared_mutex allows multiple readers or one writer.
 *    This is appropriate for page-level locking.
 *
 * ============================================================================
 */

#include <memory>
#include <shared_mutex>

#include "common/types.h"
#include "storage/slotted_page.h"

namespace buzzdb {

/**
 * @brief A frame in the buffer pool holding one page.
 *
 * BufferFrame owns a SlottedPage and tracks whether it's been modified.
 * The mutex allows concurrent access control (shared for reads, exclusive for writes).
 */
class BufferFrame {
private:
    friend class BufferManager;

    /// The page ID this frame is holding
    PageID page_id_;

    /// Whether the page has been modified since loading
    bool is_dirty_;

public:
    /// The actual page data
    /// CRITIQUE: Should be private with accessor methods
    std::unique_ptr<SlottedPage> page;

    /// Mutex for concurrent access control
    /// CRITIQUE: Should be private with BufferManager as friend
    std::shared_mutex mutex;

public:
    /**
     * @brief Construct a BufferFrame with a page.
     * @param p The SlottedPage to hold (takes ownership).
     * @param pid The page ID.
     */
    BufferFrame(std::unique_ptr<SlottedPage>&& p, PageID pid)
        : page_id_(pid)
        , is_dirty_(false)
        , page(std::move(p)) {}

    /// Destructor
    ~BufferFrame() = default;

    // -------------------------------------------------------------------------
    // DELETED OPERATIONS
    // -------------------------------------------------------------------------
    // BufferFrame is non-copyable and non-movable because:
    // 1. It owns a unique_ptr (could be made movable)
    // 2. It contains a mutex (not movable)
    // 3. External code may hold references to it
    // -------------------------------------------------------------------------

    BufferFrame() = delete;
    BufferFrame(const BufferFrame&) = delete;
    BufferFrame& operator=(const BufferFrame&) = delete;
    BufferFrame(BufferFrame&&) = delete;
    BufferFrame& operator=(BufferFrame&&) = delete;

    // -------------------------------------------------------------------------
    // PUBLIC INTERFACE
    // -------------------------------------------------------------------------

    /**
     * @brief Mark the page as dirty (modified).
     *
     * Once dirty, the page must be written to disk before eviction.
     * There's no clear_dirty() because that should only happen after
     * the BufferManager flushes the page.
     */
    void set_dirty() {
        is_dirty_ = true;
    }

    /**
     * @brief Check if the page is dirty.
     * @return true if the page has been modified.
     */
    bool is_dirty() const {
        return is_dirty_;
    }

    /**
     * @brief Get the page ID this frame holds.
     * @return The PageID.
     */
    PageID get_page_id() const {
        return page_id_;
    }

    // -------------------------------------------------------------------------
    // METHODS FOR BUFFERMANAGER (via friendship)
    // -------------------------------------------------------------------------

private:
    /**
     * @brief Clear the dirty flag after flushing to disk.
     *
     * Only BufferManager should call this after successfully writing
     * the page to storage.
     */
    void clear_dirty() {
        is_dirty_ = false;
    }

    /**
     * @brief Update the page ID (when reusing a frame for a different page).
     * @param new_page_id The new page ID.
     */
    void set_page_id(PageID new_page_id) {
        page_id_ = new_page_id;
    }
};

}  // namespace buzzdb
