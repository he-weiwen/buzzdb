#pragma once

/**
 * @file insert_operator.h
 * @brief InsertOperator - inserts tuples into storage.
 *
 * Finds a page with space and inserts the tuple, or extends the database
 * if no space is available.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. NOT A PROPER OPERATOR
 *    InsertOperator doesn't have an input operator - it's fed tuples via
 *    setTupleToInsert(). This breaks the operator tree model. Should either:
 *    - Take a child operator and insert all its output tuples
 *    - Be a separate class, not an Operator subclass
 *
 * 2. LINEAR PAGE SCAN
 *    Scans all pages to find one with space. For large databases with
 *    many full pages, this is O(n) per insert. Should maintain a free
 *    space map or free page list.
 *
 * 3. NO TRANSACTION SUPPORT
 *    Insert is immediately visible, no rollback capability.
 *
 * 4. CLONES TUPLE
 *    Uses tuple->clone() which does a deep copy. If tuple ownership
 *    could be transferred, we could avoid this.
 *
 * 5. EXTENDS DATABASE ON FULL
 *    If all pages are full, extends the database file. This is correct
 *    but could be optimized (e.g., extend by multiple pages).
 *
 * ============================================================================
 */

#include "execution/operator.h"
#include "buffer/buffer_manager.h"
#include "storage/tuple.h"

namespace buzzdb {

/**
 * @brief Inserts tuples into the database.
 *
 * Call setTupleToInsert() then next() to insert a tuple.
 */
class InsertOperator : public Operator {
private:
    BufferManager& buffer_manager_;
    std::unique_ptr<Tuple> tuple_to_insert_;

public:
    explicit InsertOperator(BufferManager& manager)
        : buffer_manager_(manager) {}

    /// Set the tuple to be inserted on next() call
    void setTupleToInsert(std::unique_ptr<Tuple> tuple) {
        tuple_to_insert_ = std::move(tuple);
    }

    void open() override {
        // Nothing to initialize
    }

    bool next() override {
        if (!tuple_to_insert_) {
            return false;
        }

        // Try to find a page with space
        size_t num_pages = buffer_manager_.getNumPages();
        for (size_t page_idx = 0; page_idx < num_pages; ++page_idx) {
            auto& frame = buffer_manager_.fix_page(
                static_cast<PageID>(page_idx), true);

            if (frame.page->addTuple(tuple_to_insert_->clone())) {
                buffer_manager_.unfix_page(frame, true);  // dirty
                tuple_to_insert_.reset();
                return true;
            }

            buffer_manager_.unfix_page(frame, false);  // not modified
        }

        // No space in existing pages - extend database
        buffer_manager_.extend();

        // Try the new page
        auto& frame = buffer_manager_.fix_page(
            static_cast<PageID>(buffer_manager_.getNumPages() - 1), true);

        if (frame.page->addTuple(tuple_to_insert_->clone())) {
            buffer_manager_.unfix_page(frame, true);
            tuple_to_insert_.reset();
            return true;
        }

        // Tuple too large for a single page
        buffer_manager_.unfix_page(frame, false);
        return false;
    }

    void close() override {
        tuple_to_insert_.reset();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // Insert operator has no output
        return {};
    }
};

}  // namespace buzzdb
