#pragma once

/**
 * @file config.h
 * @brief Global configuration constants for BuzzDB.
 *
 * This file contains compile-time constants that define the core parameters
 * of the database system. These values are shared across all components.
 */

#include <cstddef>
#include <cstdint>
#include <limits>

namespace buzzdb {

/// Size of a single page in bytes (4 KB).
/// This is the fundamental unit of I/O and memory management.
static constexpr size_t PAGE_SIZE = 4096;

/// Maximum number of tuple slots per page.
/// Each slot can hold one tuple's metadata (offset + length).
static constexpr size_t MAX_SLOTS = 512;

/// Maximum number of pages the database file can contain.
/// This is a simplification; a real system would grow dynamically.
static constexpr size_t MAX_PAGES = 1000;

/// Maximum number of pages that can be held in the buffer pool.
/// Pages beyond this limit will be evicted according to the replacement policy.
static constexpr size_t MAX_PAGES_IN_MEMORY = 10;

/// Sentinel value indicating an invalid or uninitialized slot/offset.
static constexpr uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max();

/// Default database filename.
inline const char* DATABASE_FILENAME = "buzzdb.dat";

}  // namespace buzzdb
