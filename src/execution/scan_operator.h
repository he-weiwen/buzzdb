#pragma once

/**
 * @file scan_operator.h
 * @brief ScanOperator - reads all tuples from storage.
 *
 * The scan operator is a leaf node in the operator tree. It iterates
 * through all pages and slots in the buffer manager, deserializing
 * tuples from slotted pages.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. FULL TABLE SCAN ONLY
 *    No predicate pushdown or index support. Every query scans all pages.
 *    This is expected for this phase but limits performance.
 *
 * 2. RELATION FILTERING IS HACKY
 *    The scan_relation field filters tuples by checking if the LAST field
 *    matches the relation name. This assumes a specific tuple format where
 *    relation name is appended. A proper catalog would be better.
 *
 * 3. USES BUFFERMANAGER.GETPAGE() DIRECTLY
 *    The original code calls getPage() which isn't in our BufferManager API.
 *    We need to use fix_page/unfix_page instead, which requires tracking
 *    which page is currently pinned.
 *
 * 4. NO ERROR HANDLING FOR DESERIALIZATION
 *    If a slot contains corrupt data, Tuple::deserialize might fail.
 *    No recovery mechanism.
 *
 * 5. MODIFIES OUTPUT IN GETOUTPUT
 *    getOutput() pops the last field (relation name) if filtering by relation.
 *    This is a side effect that makes getOutput() non-idempotent.
 *
 * ============================================================================
 */

#include <string>
#include <sstream>
#include <cassert>

#include "execution/operator.h"
#include "buffer/buffer_manager.h"
#include "storage/slotted_page.h"
#include "storage/tuple.h"
#include "common/config.h"

namespace buzzdb {

/**
 * @brief Scans all tuples from the database.
 *
 * Iterates through pages and slots, deserializing tuples.
 * Optionally filters by relation name (if stored in last field).
 */
class ScanOperator : public Operator {
private:
    BufferManager& buffer_manager_;
    std::string relation_filter_;     // Empty means scan all

    size_t current_page_index_ = 0;
    size_t current_slot_index_ = 0;
    std::unique_ptr<Tuple> current_tuple_;

    // Track currently pinned page for proper buffer management
    BufferFrame* current_frame_ = nullptr;

public:
    /// Scan all tuples
    explicit ScanOperator(BufferManager& manager)
        : buffer_manager_(manager) {}

    /// Scan tuples matching a relation name (stored in last field)
    ScanOperator(BufferManager& manager, const std::string& relation)
        : buffer_manager_(manager), relation_filter_(relation) {}

    void open() override {
        current_page_index_ = 0;
        current_slot_index_ = 0;
        current_tuple_.reset();
        current_frame_ = nullptr;
    }

    bool next() override {
        loadNextTuple();
        return current_tuple_ != nullptr;
    }

    void close() override {
        // Unpin current page if we have one pinned
        if (current_frame_) {
            buffer_manager_.unfix_page(*current_frame_, false);
            current_frame_ = nullptr;
        }
        current_page_index_ = 0;
        current_slot_index_ = 0;
        current_tuple_.reset();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (!current_tuple_) {
            return {};
        }

        // CRITIQUE: This modifies the tuple - side effect in getter
        if (!relation_filter_.empty() && !current_tuple_->fields.empty()) {
            // Remove the relation name field (last field)
            current_tuple_->fields.pop_back();
        }

        return std::move(current_tuple_->fields);
    }

private:
    void loadNextTuple() {
        while (current_page_index_ < buffer_manager_.getNumPages()) {
            // Pin current page if not already pinned
            if (!current_frame_) {
                current_frame_ = &buffer_manager_.fix_page(
                    static_cast<PageID>(current_page_index_), false);
            }

            SlottedPage* page = current_frame_->page.get();
            if (!page) {
                // Move to next page
                buffer_manager_.unfix_page(*current_frame_, false);
                current_frame_ = nullptr;
                current_page_index_++;
                current_slot_index_ = 0;
                continue;
            }

            const Slot* slots = page->getSlotArray();

            while (current_slot_index_ < MAX_SLOTS) {
                if (!slots[current_slot_index_].empty) {
                    assert(slots[current_slot_index_].offset != INVALID_VALUE);

                    // Deserialize tuple from slot
                    const char* tuple_data = page->getTupleData(
                        static_cast<SlotID>(current_slot_index_));
                    size_t tuple_len = page->getTupleLength(
                        static_cast<SlotID>(current_slot_index_));

                    std::istringstream iss(std::string(tuple_data, tuple_len));
                    current_tuple_ = Tuple::deserialize(iss);

                    // Filter by relation if specified
                    if (!relation_filter_.empty()) {
                        if (current_tuple_->fields.empty() ||
                            current_tuple_->fields.back()->asString() != relation_filter_) {
                            current_slot_index_++;
                            continue;  // Skip this tuple
                        }
                    }

                    current_slot_index_++;
                    return;  // Found a tuple
                }
                current_slot_index_++;
            }

            // Done with this page, move to next
            buffer_manager_.unfix_page(*current_frame_, false);
            current_frame_ = nullptr;
            current_page_index_++;
            current_slot_index_ = 0;
        }

        // No more tuples
        current_tuple_.reset();
    }
};

}  // namespace buzzdb
