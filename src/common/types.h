#pragma once

/**
 * @file types.h
 * @brief Common type definitions used throughout BuzzDB.
 *
 * This file defines type aliases and enumerations that provide
 * semantic meaning to primitive types and ensure consistency
 * across the codebase.
 */

#include <cstdint>

namespace buzzdb {

/// Unique identifier for a page on disk.
/// Pages are numbered starting from 0.
using PageID = uint16_t;

/// Identifier for a frame (slot) in the buffer pool.
/// Frames are numbered starting from 0.
using FrameID = uint64_t;

/// Identifier for a slot within a slotted page.
using SlotID = uint16_t;

/**
 * @brief Represents the data type of a Field.
 *
 * Fields are the atomic units of data in tuples.
 * Each field has exactly one of these types.
 */
enum class FieldType {
    INT,    ///< 32-bit signed integer
    FLOAT,  ///< 32-bit floating point
    STRING  ///< Variable-length null-terminated string
};

/**
 * @brief Record identifier - locates a tuple within the database.
 *
 * A RID uniquely identifies a tuple by specifying which page
 * it resides on and which slot within that page.
 */
struct RID {
    PageID page_id;
    SlotID slot_id;

    bool operator==(const RID& other) const {
        return page_id == other.page_id && slot_id == other.slot_id;
    }

    bool operator!=(const RID& other) const {
        return !(*this == other);
    }
};

}  // namespace buzzdb

// Hash function for RID (enables use in unordered containers)
namespace std {
template <>
struct hash<buzzdb::RID> {
    size_t operator()(const buzzdb::RID& rid) const {
        // Combine page_id and slot_id into a single hash
        return hash<uint32_t>()(
            (static_cast<uint32_t>(rid.page_id) << 16) | rid.slot_id
        );
    }
};
}  // namespace std
