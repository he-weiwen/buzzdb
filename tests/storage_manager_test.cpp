/**
 * @file storage_manager_test.cpp
 * @brief Tests for StorageManager class.
 */

#include <iostream>
#include <cassert>
#include <cstring>
#include <filesystem>

#include "storage/storage_manager.h"

using namespace buzzdb;

// Helper to remove test database file
void cleanup_test_file() {
    std::filesystem::remove(DATABASE_FILENAME);
}

void test_create_new_database() {
    std::cout << "Testing create new database..." << std::endl;

    cleanup_test_file();

    {
        StorageManager sm(true);  // truncate mode

        // Should have at least one page
        assert(sm.getNumPages() >= 1);
        assert(sm.num_pages >= 1);

        std::cout << "    Initial pages: " << sm.getNumPages() << std::endl;
    }

    // File should exist
    assert(std::filesystem::exists(DATABASE_FILENAME));

    cleanup_test_file();
    std::cout << "  Create new database OK" << std::endl;
}

void test_write_and_read_page() {
    std::cout << "Testing write and read page..." << std::endl;

    cleanup_test_file();

    {
        StorageManager sm(true);

        // Create a page with known data
        auto page = std::make_unique<SlottedPage>();

        // Write a pattern to the page
        const char* test_data = "Hello, StorageManager!";
        size_t test_len = std::strlen(test_data);
        std::memcpy(page->page_data.get() + page->metadata_size, test_data, test_len);

        // Flush to disk
        sm.flush(0, page);
    }

    // Reopen and verify
    {
        StorageManager sm(false);  // Don't truncate

        auto loaded = sm.load(0);

        const char* test_data = "Hello, StorageManager!";
        size_t test_len = std::strlen(test_data);

        // Verify data was persisted
        assert(std::memcmp(
            loaded->page_data.get() + loaded->metadata_size,
            test_data,
            test_len) == 0);
    }

    cleanup_test_file();
    std::cout << "  Write and read page OK" << std::endl;
}

void test_extend_single() {
    std::cout << "Testing extend single page..." << std::endl;

    cleanup_test_file();

    {
        StorageManager sm(true);

        size_t initial_pages = sm.getNumPages();

        sm.extend();

        assert(sm.getNumPages() == initial_pages + 1);
    }

    cleanup_test_file();
    std::cout << "  Extend single page OK" << std::endl;
}

void test_extend_multiple() {
    std::cout << "Testing extend to specific page..." << std::endl;

    cleanup_test_file();

    {
        StorageManager sm(true);

        // Extend to page 10 (should create pages 0-10, i.e., 11 pages)
        sm.extend(10);

        assert(sm.getNumPages() == 11);

        // Should be able to write to page 10
        auto page = std::make_unique<SlottedPage>();
        const char* marker = "Page10Marker";
        std::memcpy(page->page_data.get() + page->metadata_size, marker, std::strlen(marker));
        sm.flush(10, page);
    }

    // Verify persistence
    {
        StorageManager sm(false);

        assert(sm.getNumPages() == 11);

        auto loaded = sm.load(10);
        const char* marker = "Page10Marker";
        assert(std::memcmp(
            loaded->page_data.get() + loaded->metadata_size,
            marker,
            std::strlen(marker)) == 0);
    }

    cleanup_test_file();
    std::cout << "  Extend to specific page OK" << std::endl;
}

void test_multiple_pages() {
    std::cout << "Testing multiple page operations..." << std::endl;

    cleanup_test_file();

    {
        StorageManager sm(true);

        sm.extend(4);  // Create 5 pages (0-4)

        // Write unique data to each page
        for (PageID i = 0; i < 5; i++) {
            auto page = std::make_unique<SlottedPage>();

            // Write page ID as marker
            int marker = static_cast<int>(i * 1000);
            std::memcpy(page->page_data.get() + page->metadata_size,
                       &marker, sizeof(marker));

            sm.flush(i, page);
        }
    }

    // Verify all pages
    {
        StorageManager sm(false);

        for (PageID i = 0; i < 5; i++) {
            auto loaded = sm.load(i);

            int marker;
            std::memcpy(&marker,
                       loaded->page_data.get() + loaded->metadata_size,
                       sizeof(marker));

            assert(marker == static_cast<int>(i * 1000));
        }
    }

    cleanup_test_file();
    std::cout << "  Multiple page operations OK" << std::endl;
}

void test_invalid_page_access() {
    std::cout << "Testing invalid page access..." << std::endl;

    cleanup_test_file();

    {
        StorageManager sm(true);

        // Try to load page that doesn't exist
        bool threw = false;
        try {
            sm.load(999);  // Way out of range
        } catch (const std::out_of_range& e) {
            threw = true;
            std::cout << "    Caught expected exception: " << e.what() << std::endl;
        }
        assert(threw && "Expected out_of_range exception for invalid page ID");
    }

    cleanup_test_file();
    std::cout << "  Invalid page access OK" << std::endl;
}

void test_persistence_across_reopen() {
    std::cout << "Testing persistence across reopen..." << std::endl;

    cleanup_test_file();

    // Write a tuple to page
    {
        StorageManager sm(true);

        auto page = std::make_unique<SlottedPage>();
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(42));
        tuple->addField(std::make_unique<Field>(std::string("persistent")));
        page->addTuple(std::move(tuple));

        sm.flush(0, page);
    }

    // Reopen and verify tuple is still there
    {
        StorageManager sm(false);

        auto page = sm.load(0);
        assert(page->countTuples() == 1);

        const char* data = page->getTupleData(0);
        size_t length = page->getTupleLength(0);

        std::istringstream iss(std::string(data, length));
        auto tuple = Tuple::deserialize(iss);

        assert(tuple->fields[0]->asInt() == 42);
        assert(tuple->fields[1]->asString() == "persistent");
    }

    cleanup_test_file();
    std::cout << "  Persistence across reopen OK" << std::endl;
}

void test_truncate_mode() {
    std::cout << "Testing truncate mode..." << std::endl;

    cleanup_test_file();

    // Create database with data
    {
        StorageManager sm(true);
        sm.extend(5);  // 6 pages

        auto page = std::make_unique<SlottedPage>();
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(12345));
        page->addTuple(std::move(tuple));
        sm.flush(0, page);
    }

    // Reopen with truncate - should lose all data
    {
        StorageManager sm(true);  // Truncate!

        // Should be back to initial state (1 page)
        assert(sm.getNumPages() == 1);

        // Page 0 should be empty
        auto page = sm.load(0);
        assert(page->countTuples() == 0);
    }

    cleanup_test_file();
    std::cout << "  Truncate mode OK" << std::endl;
}

void test_non_truncate_mode() {
    std::cout << "Testing non-truncate mode..." << std::endl;

    cleanup_test_file();

    // Create database with data
    {
        StorageManager sm(true);
        sm.extend(5);  // 6 pages

        auto page = std::make_unique<SlottedPage>();
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(99999));
        page->addTuple(std::move(tuple));
        sm.flush(0, page);
    }

    // Reopen without truncate - should preserve data
    {
        StorageManager sm(false);  // Don't truncate

        assert(sm.getNumPages() == 6);

        auto page = sm.load(0);
        assert(page->countTuples() == 1);

        const char* data = page->getTupleData(0);
        size_t length = page->getTupleLength(0);
        std::istringstream iss(std::string(data, length));
        auto tuple = Tuple::deserialize(iss);

        assert(tuple->fields[0]->asInt() == 99999);
    }

    cleanup_test_file();
    std::cout << "  Non-truncate mode OK" << std::endl;
}

int main() {
    std::cout << "=== StorageManager Tests ===" << std::endl;

    test_create_new_database();
    test_write_and_read_page();
    test_extend_single();
    test_extend_multiple();
    test_multiple_pages();
    test_invalid_page_access();
    test_persistence_across_reopen();
    test_truncate_mode();
    test_non_truncate_mode();

    std::cout << "=== All StorageManager tests passed ===" << std::endl;
    return 0;
}
