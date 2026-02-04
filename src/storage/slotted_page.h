#pragma once

/**
 * @file slotted_page.h
 * @brief SlottedPage class - the fundamental storage unit for tuples.
 *
 * A slotted page organizes tuples within a fixed-size page using a directory
 * of slots at the beginning of the page and tuple data growing from the
 * metadata region toward the end.
 *
 * Page Layout:
 * +------------------+-------------------------------------------+
 * | Slot Directory   | Free Space / Tuple Data                   |
 * | [Slot 0]         |                                           |
 * | [Slot 1]         |         <-- tuples stored here            |
 * | ...              |                                           |
 * | [Slot N-1]       |                                           |
 * +------------------+-------------------------------------------+
 * ^                  ^                                           ^
 * 0           metadata_size                                 PAGE_SIZE
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. PUBLIC DATA MEMBERS
 *    `page_data` and `metadata_size` are public. This allows external code
 *    to directly manipulate the raw page buffer, bypassing the class's methods.
 *    Should be private with accessor methods.
 *
 * 2. FIXED SLOT COUNT
 *    MAX_SLOTS is fixed at 512, regardless of actual tuple sizes.
 *    This wastes space for pages with few large tuples and limits pages
 *    with many small tuples. A dynamic slot directory would be more flexible.
 *
 * 3. NO COMPACTION
 *    deleteTuple() marks a slot as empty but doesn't reclaim the space.
 *    The tuple data remains in place. Over time, this leads to fragmentation.
 *    A compaction mechanism would help.
 *
 * 4. SLOT REUSE LOGIC IS FRAGILE
 *    addTuple() tries to find a slot with "length >= tuple_size", but this
 *    only works if the slot was previously used. New slots have length=INVALID.
 *    The logic handles this but it's convoluted.
 *
 * 5. SERIALIZATION DEPENDENCY
 *    addTuple() calls tuple->serialize() to get string representation.
 *    This ties the page format to the text-based serialization format.
 *    A binary format would be more efficient.
 *
 * 6. NO SLOT ID RETURN
 *    addTuple() returns bool, not the slot ID where the tuple was placed.
 *    This makes it hard to build indexes pointing to specific tuples.
 *
 * 7. CONSTRUCTOR REDUNDANCY
 *    The constructor manually initializes all slots, but they're already
 *    initialized by Slot's default member initializers. The loop is redundant.
 *
 * 8. print() HARDCODED TO stdout
 *    Should take ostream& parameter.
 *
 * 9. NO CONST-CORRECTNESS FOR DATA ACCESS
 *    No const method to read tuple data without modification risk.
 *
 * ============================================================================
 */

#include <memory>
#include <cstring>
#include <sstream>
#include <iostream>
#include <cassert>

#include "common/config.h"
#include "common/types.h"
#include "slot.h"
#include "tuple.h"

namespace buzzdb {

/**
 * @brief A fixed-size page that stores tuples using a slotted directory.
 *
 * SlottedPage is the fundamental unit of storage. Pages are read from and
 * written to disk as atomic units by the StorageManager.
 */
class SlottedPage {
public:
    // -------------------------------------------------------------------------
    // PUBLIC DATA MEMBERS
    // -------------------------------------------------------------------------
    // CRITIQUE: Should be private. Left public for compatibility with
    // BufferManager and other code that directly accesses page_data.
    // -------------------------------------------------------------------------

    /// Raw page buffer (PAGE_SIZE bytes).
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);

    /// Size of the slot directory in bytes.
    /// Tuples are stored starting at this offset.
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

public:
    // -------------------------------------------------------------------------
    // CONSTRUCTORS
    // -------------------------------------------------------------------------

    /// Default constructor - creates an empty page with initialized slot directory.
    /// CRITIQUE: The loop is technically redundant since Slot has default initializers,
    /// but it ensures the in-memory representation matches what we expect.
    SlottedPage() {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            slot_array[slot_itr].length = INVALID_VALUE;
        }
    }

    /// Destructor.
    ~SlottedPage() = default;

    // Non-copyable (owns unique_ptr)
    SlottedPage(const SlottedPage&) = delete;
    SlottedPage& operator=(const SlottedPage&) = delete;

    // Movable
    SlottedPage(SlottedPage&&) = default;
    SlottedPage& operator=(SlottedPage&&) = default;

    // -------------------------------------------------------------------------
    // TUPLE OPERATIONS
    // -------------------------------------------------------------------------

    /**
     * @brief Add a tuple to the page.
     * @param tuple The tuple to add (ownership transferred).
     * @return true if the tuple was added, false if no space available.
     *
     * CRITIQUE: Returns bool, not slot ID. Caller can't know where tuple went.
     * CRITIQUE: Uses text serialization, inefficient.
     */
    bool addTuple(std::unique_ptr<Tuple> tuple) {
        // Serialize the tuple into a string
        // CRITIQUE: Text-based serialization is inefficient
        auto serializedTuple = tuple->serialize();
        size_t tuple_size = serializedTuple.size();

        // Find first slot that can accommodate the tuple
        // CRITIQUE: This logic is convoluted - handles both reuse and new slots
        size_t slot_itr = 0;
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (; slot_itr < MAX_SLOTS; slot_itr++) {
            if (slot_array[slot_itr].empty == true &&
                slot_array[slot_itr].length >= tuple_size) {
                // Found a previously-used slot with enough space
                break;
            }
        }

        if (slot_itr == MAX_SLOTS) {
            // No reusable slot found, try to find an unused slot
            for (slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
                if (slot_array[slot_itr].empty == true &&
                    slot_array[slot_itr].offset == INVALID_VALUE) {
                    // Found a never-used slot
                    break;
                }
            }
        }

        if (slot_itr == MAX_SLOTS) {
            // No available slots at all
            return false;
        }

        // Calculate offset for the tuple data
        slot_array[slot_itr].empty = false;
        size_t offset = INVALID_VALUE;

        if (slot_array[slot_itr].offset == INVALID_VALUE) {
            // This slot has never been used, calculate new offset
            if (slot_itr != 0) {
                // Find the end of the previous slot's data
                auto prev_slot_offset = slot_array[slot_itr - 1].offset;
                auto prev_slot_length = slot_array[slot_itr - 1].length;
                // CRITIQUE: Assumes slots are used sequentially. If slot 0 is deleted
                // and slot 1 exists, this calculation is wrong for reusing slot 0.
                if (prev_slot_offset != INVALID_VALUE) {
                    offset = prev_slot_offset + prev_slot_length;
                } else {
                    offset = metadata_size;
                }
            } else {
                offset = metadata_size;
            }
            slot_array[slot_itr].offset = offset;
        } else {
            // Reusing a previously-used slot
            offset = slot_array[slot_itr].offset;
        }

        // Check if tuple fits in remaining page space
        if (offset + tuple_size >= PAGE_SIZE) {
            // Doesn't fit, revert slot state
            slot_array[slot_itr].empty = true;
            slot_array[slot_itr].offset = INVALID_VALUE;
            return false;
        }

        assert(offset != INVALID_VALUE);
        assert(offset >= metadata_size);
        assert(offset + tuple_size < PAGE_SIZE);

        // Set length if this is a new slot
        if (slot_array[slot_itr].length == INVALID_VALUE) {
            slot_array[slot_itr].length = tuple_size;
        }

        // Copy serialized tuple data into page
        std::memcpy(page_data.get() + offset,
                    serializedTuple.c_str(),
                    tuple_size);

        return true;
    }

    /**
     * @brief Delete a tuple by slot index.
     * @param index The slot index to delete.
     *
     * CRITIQUE: Only marks slot as empty, doesn't reclaim space (fragmentation).
     * CRITIQUE: Silent no-op if slot doesn't exist or is already empty.
     */
    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        if (index < MAX_SLOTS && !slot_array[index].empty) {
            slot_array[index].empty = true;
            // CRITIQUE: Doesn't clear offset/length, leaving stale data
        }
    }

    // -------------------------------------------------------------------------
    // ACCESSORS
    // -------------------------------------------------------------------------

    /// Get pointer to slot array.
    Slot* getSlotArray() {
        return reinterpret_cast<Slot*>(page_data.get());
    }

    const Slot* getSlotArray() const {
        return reinterpret_cast<const Slot*>(page_data.get());
    }

    /// Get raw data pointer (for BufferManager I/O).
    char* data() { return page_data.get(); }
    const char* data() const { return page_data.get(); }

    /// Get tuple data at a specific slot.
    /// Returns nullptr if slot is empty or invalid.
    const char* getTupleData(SlotID slot_id) const {
        if (slot_id >= MAX_SLOTS) return nullptr;
        const Slot* slots = getSlotArray();
        if (slots[slot_id].empty || slots[slot_id].offset == INVALID_VALUE) {
            return nullptr;
        }
        return page_data.get() + slots[slot_id].offset;
    }

    /// Get tuple length at a specific slot.
    /// Returns 0 if slot is empty or invalid.
    size_t getTupleLength(SlotID slot_id) const {
        if (slot_id >= MAX_SLOTS) return 0;
        const Slot* slots = getSlotArray();
        if (slots[slot_id].empty || slots[slot_id].length == INVALID_VALUE) {
            return 0;
        }
        return slots[slot_id].length;
    }

    // -------------------------------------------------------------------------
    // UTILITY
    // -------------------------------------------------------------------------

    /// Print all tuples in the page to stdout.
    /// CRITIQUE: Hardcoded to stdout, should take ostream&.
    void print() const {
        print(std::cout);
    }

    /// Print all tuples in the page to specified stream.
    void print(std::ostream& os) const {
        const Slot* slot_array = getSlotArray();
        for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
            if (!slot_array[slot_itr].empty) {
                assert(slot_array[slot_itr].offset != INVALID_VALUE);
                const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
                std::istringstream iss(tuple_data);
                auto loadedTuple = Tuple::deserialize(iss);
                os << "Slot " << slot_itr << " : ["
                   << slot_array[slot_itr].offset << "] :: ";
                // Print tuple fields directly to os (since Tuple::print() goes to cout)
                for (const auto& field : loadedTuple->fields) {
                    os << field->asString() << " ";
                }
                os << "\n";
            }
        }
        os << "\n";
    }

    /// Count the number of occupied slots.
    size_t countTuples() const {
        const Slot* slot_array = getSlotArray();
        size_t count = 0;
        for (size_t i = 0; i < MAX_SLOTS; i++) {
            if (!slot_array[i].empty) {
                count++;
            }
        }
        return count;
    }
};

}  // namespace buzzdb
