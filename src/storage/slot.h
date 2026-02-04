#pragma once

/**
 * @file slot.h
 * @brief Slot structure for slotted page organization.
 *
 * A Slot is the metadata entry in a slotted page's directory.
 * Each slot tracks one tuple's location within the page.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. SIMPLE AND APPROPRIATE
 *    This is a POD (Plain Old Data) struct, which is appropriate for
 *    metadata that will be stored directly in page buffers.
 *
 * 2. MEMORY LAYOUT
 *    sizeof(Slot) = 1 (bool) + 2 (uint16_t) + 2 (uint16_t) = 5 bytes
 *    But due to alignment, likely padded to 6 or 8 bytes.
 *    With MAX_SLOTS=512, metadata takes 512 * sizeof(Slot) bytes.
 *
 *    Potential optimization: Pack the empty flag into offset's high bit,
 *    reducing to 4 bytes per slot. But this adds complexity.
 *
 * 3. INVARIANTS NOT ENFORCED
 *    - empty=true should imply offset=INVALID_VALUE and length=INVALID_VALUE
 *    - empty=false should imply offset!=INVALID_VALUE
 *    These are not enforced by the struct itself.
 *
 * ============================================================================
 */

#include "common/config.h"

namespace buzzdb {

/**
 * @brief Metadata for a single tuple slot in a slotted page.
 *
 * The slot directory at the beginning of each page contains an array
 * of these structures. Each slot either:
 * - Points to a tuple (empty=false, offset/length set)
 * - Is available for reuse (empty=true)
 */
struct Slot {
    /// True if this slot is empty (available for use).
    bool empty = true;

    /// Byte offset from the start of the page where the tuple data begins.
    /// INVALID_VALUE if slot has never been used.
    uint16_t offset = INVALID_VALUE;

    /// Length of the tuple data in bytes.
    /// INVALID_VALUE if slot has never been used.
    uint16_t length = INVALID_VALUE;
};

// Verify size assumptions
static_assert(sizeof(Slot) <= 8, "Slot size exceeds expected maximum");

}  // namespace buzzdb
