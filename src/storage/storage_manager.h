#pragma once

/**
 * @file storage_manager.h
 * @brief StorageManager class - handles disk I/O for pages.
 *
 * The StorageManager is responsible for reading and writing pages to the
 * database file. It provides the persistence layer for the buffer manager.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. PUBLIC DATA MEMBERS
 *    `fileStream` and `num_pages` are public. This breaks encapsulation and
 *    allows external code to manipulate the file directly.
 *
 * 2. THREAD SAFETY
 *    Uses mutex to protect I/O operations. This is correct but coarse-grained.
 *    A more sophisticated approach might use per-page or read-write locks.
 *
 * 3. ERROR HANDLING
 *    - load() calls exit(-1) on read failure instead of throwing
 *    - No error handling for write failures in flush()
 *    - Constructor doesn't report file creation failures clearly
 *
 * 4. SIDE EFFECTS IN extend()
 *    The extend() methods print to stdout. I/O operations shouldn't produce
 *    console output. Use logging or return status instead.
 *
 * 5. MEMORY MANAGEMENT IN extend(till_page_id)
 *    Uses raw new[]/delete[] instead of std::vector or unique_ptr.
 *    Memory leak if write fails before delete[].
 *
 * 6. FILE OPENING LOGIC
 *    The constructor's file opening logic is convoluted:
 *    - Opens for read/write
 *    - If that fails, opens for write only (creates file)
 *    - Closes and reopens for read/write
 *    A cleaner approach would use std::filesystem.
 *
 * 7. NO TRUNCATE OPTION IN LAB 2
 *    Lab 3's StorageManager has a truncate_mode parameter, but Lab 2's doesn't.
 *    This version adds it for consistency.
 *
 * 8. PAGE ID TYPE
 *    load() takes uint16_t page_id, limiting to 65535 pages.
 *    Should use PageID type consistently.
 *
 * 9. NO FSYNC
 *    flush() calls fileStream.flush() which flushes to OS buffer, but doesn't
 *    guarantee data reaches disk (no fsync). Data could be lost on crash.
 *
 * ============================================================================
 */

#include <fstream>
#include <mutex>
#include <memory>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include "common/config.h"
#include "common/types.h"
#include "slotted_page.h"

namespace buzzdb {

/**
 * @brief Manages reading and writing pages to the database file.
 *
 * The StorageManager provides a simple interface for page-level I/O.
 * It is thread-safe, using a mutex to serialize all operations.
 */
class StorageManager {
public:
    // -------------------------------------------------------------------------
    // PUBLIC DATA MEMBERS
    // -------------------------------------------------------------------------
    // CRITIQUE: Should be private. Left public for compatibility.
    // -------------------------------------------------------------------------

    /// File stream for database file.
    std::fstream fileStream;

    /// Number of pages currently in the file.
    size_t num_pages = 0;

    /// Mutex for thread-safe I/O.
    /// CRITIQUE: Should be mutable if we want const methods to lock.
    std::mutex io_mutex;

public:
    // -------------------------------------------------------------------------
    // CONSTRUCTORS / DESTRUCTOR
    // -------------------------------------------------------------------------

    /**
     * @brief Construct StorageManager and open database file.
     * @param truncate_mode If true, truncate existing file (start fresh).
     *
     * CRITIQUE: File opening logic is convoluted. Uses multiple open/close cycles.
     */
    explicit StorageManager(bool truncate_mode = true) {
        auto flags = truncate_mode
            ? std::ios::in | std::ios::out | std::ios::trunc
            : std::ios::in | std::ios::out;

        fileStream.open(DATABASE_FILENAME, flags);

        if (!fileStream) {
            // File doesn't exist or can't be opened, try creating it
            fileStream.clear();
            auto create_flags = truncate_mode
                ? std::ios::out | std::ios::trunc
                : std::ios::out;
            fileStream.open(DATABASE_FILENAME, create_flags);

            if (!fileStream) {
                throw std::runtime_error(
                    std::string("Failed to create database file: ") + DATABASE_FILENAME);
            }
        }

        fileStream.close();
        fileStream.open(DATABASE_FILENAME, std::ios::in | std::ios::out);

        if (!fileStream) {
            throw std::runtime_error(
                std::string("Failed to reopen database file: ") + DATABASE_FILENAME);
        }

        // Determine number of existing pages
        fileStream.seekg(0, std::ios::end);
        num_pages = fileStream.tellg() / PAGE_SIZE;

        // Ensure at least one page exists
        if (num_pages == 0) {
            extend();
        }
    }

    /// Destructor - closes file.
    ~StorageManager() {
        if (fileStream.is_open()) {
            fileStream.close();
        }
    }

    // Non-copyable, non-movable (owns file handle and mutex)
    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;
    StorageManager(StorageManager&&) = delete;
    StorageManager& operator=(StorageManager&&) = delete;

    // -------------------------------------------------------------------------
    // PAGE I/O
    // -------------------------------------------------------------------------

    /**
     * @brief Load a page from disk.
     * @param page_id The page to load.
     * @return A new SlottedPage containing the page data.
     * @throws std::runtime_error if read fails.
     *
     * CRITIQUE: Original used exit(-1) on failure. Changed to throw.
     */
    std::unique_ptr<SlottedPage> load(PageID page_id) {
        std::lock_guard<std::mutex> io_guard(io_mutex);

        if (page_id >= num_pages) {
            throw std::out_of_range(
                "Page ID " + std::to_string(page_id) +
                " out of range (num_pages=" + std::to_string(num_pages) + ")");
        }

        fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
        auto page = std::make_unique<SlottedPage>();

        if (!fileStream.read(page->page_data.get(), PAGE_SIZE)) {
            throw std::runtime_error(
                "Failed to read page " + std::to_string(page_id) + " from disk");
        }

        return page;
    }

    /**
     * @brief Write a page to disk.
     * @param page_id The page location to write to.
     * @param page The page data to write.
     *
     * CRITIQUE: No error handling for write failures.
     * CRITIQUE: Uses fstream::flush, not fsync - data may not reach disk.
     */
    void flush(PageID page_id, const std::unique_ptr<SlottedPage>& page) {
        std::lock_guard<std::mutex> io_guard(io_mutex);

        size_t page_offset = page_id * PAGE_SIZE;
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page->page_data.get(), PAGE_SIZE);
        fileStream.flush();
    }

    /// Overload for raw SlottedPage reference (Lab 3 compatibility).
    void flush(PageID page_id, const SlottedPage& page) {
        std::lock_guard<std::mutex> io_guard(io_mutex);

        size_t page_offset = page_id * PAGE_SIZE;
        fileStream.seekp(page_offset, std::ios::beg);
        fileStream.write(page.page_data.get(), PAGE_SIZE);
        fileStream.flush();
    }

    // -------------------------------------------------------------------------
    // FILE EXTENSION
    // -------------------------------------------------------------------------

    /**
     * @brief Extend the database file by one page.
     *
     * CRITIQUE: Original printed to stdout. Removed for clean operation.
     */
    void extend() {
        std::lock_guard<std::mutex> io_guard(io_mutex);

        // Create an empty page
        auto empty_page = std::make_unique<SlottedPage>();

        // Append to end of file
        fileStream.seekp(0, std::ios::end);
        fileStream.write(empty_page->page_data.get(), PAGE_SIZE);
        fileStream.flush();

        num_pages += 1;
    }

    /**
     * @brief Extend the database file to include at least the given page ID.
     * @param till_page_id The page ID that should be valid after extension.
     *
     * CRITIQUE: Original used raw new[]/delete[]. Changed to vector.
     * CRITIQUE: Original printed to stdout. Removed.
     */
    void extend(uint64_t till_page_id) {
        std::lock_guard<std::mutex> io_guard(io_mutex);

        if (till_page_id < num_pages) {
            return;  // Already have enough pages
        }

        uint64_t pages_to_add = till_page_id + 1 - num_pages;
        uint64_t write_size = pages_to_add * PAGE_SIZE;

        // Use vector instead of raw new[]
        std::vector<char> buffer(write_size, 0);

        fileStream.seekp(0, std::ios::end);
        fileStream.write(buffer.data(), write_size);
        fileStream.flush();

        num_pages = till_page_id + 1;
    }

    // -------------------------------------------------------------------------
    // ACCESSORS
    // -------------------------------------------------------------------------

    /// Get current number of pages.
    size_t getNumPages() const {
        return num_pages;
    }
};

}  // namespace buzzdb
