/**
 * @file buffer_manager_test.cpp
 * @brief Tests for BufferManager class.
 *
 * These tests are adapted from the original lab2 tests.
 */

#include <iostream>
#include <cassert>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <random>
#include <filesystem>
#include <algorithm>

#include "common/types.h"
#include "buffer/buffer_manager.h"

using namespace buzzdb;

// Helper to clean up test database file
void cleanup_test_file() {
    std::filesystem::remove(DATABASE_FILENAME);
}

// ============================================================================
// Basic Tests
// ============================================================================

void test_fix_single_page() {
    std::cout << "Testing fix single page..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        std::vector<uint64_t> expected_values(PAGE_SIZE / sizeof(uint64_t), 123);

        // Write data
        {
            auto& frame = bm.fix_page(0, true);
            std::memcpy(frame.page->page_data.get(),
                       expected_values.data(), PAGE_SIZE);
            bm.unfix_page(frame, true);

            assert(bm.get_lru_list().empty());
            assert(bm.get_fifo_list() == std::vector<PageID>{0});
        }

        // Read back and verify
        {
            std::vector<uint64_t> values(PAGE_SIZE / sizeof(uint64_t));
            auto& frame = bm.fix_page(0, false);
            std::memcpy(values.data(), frame.page->page_data.get(), PAGE_SIZE);
            bm.unfix_page(frame, false);

            // Second access promotes to LRU
            assert(bm.get_fifo_list().empty());
            assert(bm.get_lru_list() == std::vector<PageID>{0});
            assert(expected_values == values);
        }
    }

    cleanup_test_file();
    std::cout << "  Fix single page OK" << std::endl;
}

void test_persistence_across_restart() {
    std::cout << "Testing persistence across restart..." << std::endl;

    cleanup_test_file();

    // Write data
    {
        BufferManager bm(10, true);

        for (uint16_t segment = 0; segment < 3; ++segment) {
            for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
                uint64_t page_id = segment * 200 + segment_page;
                auto& frame = bm.fix_page(page_id, true);

                uint64_t& value = *reinterpret_cast<uint64_t*>(
                    frame.page->page_data.get());
                value = segment * 200 + segment_page;

                bm.unfix_page(frame, true);
            }
        }
    }

    // Read back after restart
    {
        BufferManager bm(10, false);

        for (uint16_t segment = 0; segment < 3; ++segment) {
            for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
                uint64_t page_id = segment * 200 + segment_page;
                auto& frame = bm.fix_page(page_id, false);

                uint64_t value = *reinterpret_cast<uint64_t*>(
                    frame.page->page_data.get());

                bm.unfix_page(frame, false);
                assert(value == segment * 200 + segment_page);
            }
        }
    }

    cleanup_test_file();
    std::cout << "  Persistence across restart OK" << std::endl;
}

// ============================================================================
// Eviction Tests
// ============================================================================

void test_fifo_eviction() {
    std::cout << "Testing FIFO eviction..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        // Fill buffer with pages 1-10
        for (PageID i = 1; i <= 10; ++i) {
            auto& page = bm.fix_page(i, false);
            bm.unfix_page(page, false);
        }

        std::vector<PageID> expected_fifo{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assert(expected_fifo == bm.get_fifo_list());
        assert(bm.get_lru_list().empty());

        // Access page 11 - should evict page 1
        auto& page = bm.fix_page(11, false);
        bm.unfix_page(page, false);

        expected_fifo = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        assert(expected_fifo == bm.get_fifo_list());
        assert(bm.get_lru_list().empty());
    }

    cleanup_test_file();
    std::cout << "  FIFO eviction OK" << std::endl;
}

void test_buffer_full_error() {
    std::cout << "Testing buffer full error..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        // Fix 10 pages without unfixing
        std::vector<BufferFrame*> frames;
        for (PageID i = 1; i <= 10; ++i) {
            auto& frame = bm.fix_page(i, false);
            frames.push_back(&frame);
        }

        // Trying to fix another should fail
        bool threw = false;
        try {
            bm.fix_page(11, false);
        } catch (const buffer_full_error&) {
            threw = true;
        }
        assert(threw && "Expected buffer_full_error");

        // Unfix all
        for (auto* frame : frames) {
            bm.unfix_page(*frame, false);
        }
    }

    cleanup_test_file();
    std::cout << "  Buffer full error OK" << std::endl;
}

void test_move_to_lru() {
    std::cout << "Testing move to LRU on re-access..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        // Access pages 1 and 2
        auto& fifo_page = bm.fix_page(1, false);
        auto* lru_page = &bm.fix_page(2, false);

        bm.unfix_page(fifo_page, false);
        bm.unfix_page(*lru_page, false);

        assert((bm.get_fifo_list() == std::vector<PageID>{1, 2}));
        assert(bm.get_lru_list().empty());

        // Re-access page 2 - should move to LRU
        lru_page = &bm.fix_page(2, false);
        bm.unfix_page(*lru_page, false);

        assert((bm.get_fifo_list() == std::vector<PageID>{1}));
        assert((bm.get_lru_list() == std::vector<PageID>{2}));
    }

    cleanup_test_file();
    std::cout << "  Move to LRU on re-access OK" << std::endl;
}

void test_lru_refresh() {
    std::cout << "Testing LRU refresh order..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        // Access pages and promote to LRU
        auto* page1 = &bm.fix_page(1, false);
        bm.unfix_page(*page1, false);
        page1 = &bm.fix_page(1, false);
        bm.unfix_page(*page1, false);

        auto* page2 = &bm.fix_page(2, false);
        bm.unfix_page(*page2, false);
        page2 = &bm.fix_page(2, false);
        bm.unfix_page(*page2, false);

        // LRU order: [1, 2]
        assert(bm.get_fifo_list().empty());
        assert((bm.get_lru_list() == std::vector<PageID>{1, 2}));

        // Access page 1 again - should move to end
        page1 = &bm.fix_page(1, false);
        bm.unfix_page(*page1, false);

        // LRU order: [2, 1]
        assert(bm.get_fifo_list().empty());
        assert((bm.get_lru_list() == std::vector<PageID>{2, 1}));
    }

    cleanup_test_file();
    std::cout << "  LRU refresh order OK" << std::endl;
}

// ============================================================================
// Concurrency Tests
// ============================================================================

void test_multithread_parallel_fix() {
    std::cout << "Testing multithread parallel fix..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        std::vector<std::thread> threads;
        for (size_t i = 0; i < 4; ++i) {
            threads.emplace_back([i, &bm] {
                auto& page1 = bm.fix_page(i, false);
                auto& page2 = bm.fix_page(i + 4, false);
                bm.unfix_page(page1, false);
                bm.unfix_page(page2, false);
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto fifo_list = bm.get_fifo_list();
        std::sort(fifo_list.begin(), fifo_list.end());
        std::vector<PageID> expected{0, 1, 2, 3, 4, 5, 6, 7};
        assert(fifo_list == expected);
        assert(bm.get_lru_list().empty());
    }

    cleanup_test_file();
    std::cout << "  Multithread parallel fix OK" << std::endl;
}

void test_multithread_exclusive_access() {
    std::cout << "Testing multithread exclusive access..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        // Initialize page 0 to zero
        {
            auto& frame = bm.fix_page(0, true);
            std::memset(frame.page->page_data.get(), 0, PAGE_SIZE);
            bm.unfix_page(frame, true);
        }

        // Multiple threads increment counter
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 4; ++i) {
            threads.emplace_back([&bm] {
                for (size_t j = 0; j < 1000; ++j) {
                    auto& frame = bm.fix_page(0, true);
                    uint64_t& value = *reinterpret_cast<uint64_t*>(
                        frame.page->page_data.get());
                    ++value;
                    bm.unfix_page(frame, true);
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        // Verify final value
        auto& frame = bm.fix_page(0, false);
        uint64_t value = *reinterpret_cast<uint64_t*>(
            frame.page->page_data.get());
        bm.unfix_page(frame, false);

        assert(value == 4000);
    }

    cleanup_test_file();
    std::cout << "  Multithread exclusive access OK" << std::endl;
}

void test_multithread_buffer_full() {
    std::cout << "Testing multithread buffer full..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        std::atomic<uint64_t> num_buffer_full = 0;
        std::atomic<uint64_t> finished_threads = 0;
        std::vector<std::thread> threads;
        size_t max_threads = 8;

        for (size_t i = 0; i < max_threads; ++i) {
            threads.emplace_back(
                [i, &bm, &num_buffer_full, &finished_threads, max_threads] {
                    std::vector<BufferFrame*> pages;
                    pages.reserve(8);

                    for (size_t j = 0; j < 8; ++j) {
                        try {
                            pages.push_back(&bm.fix_page(i + j * 8, false));
                        } catch (const buffer_full_error&) {
                            ++num_buffer_full;
                        }
                    }

                    ++finished_threads;

                    // Busy wait until all threads finished
                    while (finished_threads.load() < max_threads) {
                    }

                    for (auto* page : pages) {
                        bm.unfix_page(*page, false);
                    }
                });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        assert(bm.get_fifo_list().size() == 10);
        assert(bm.get_lru_list().empty());
        assert(num_buffer_full.load() == 54);
    }

    cleanup_test_file();
    std::cout << "  Multithread buffer full OK" << std::endl;
}

void test_multithread_many_pages() {
    std::cout << "Testing multithread many pages..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        std::atomic<uint64_t> num_unfixes = 0;
        std::vector<std::thread> threads;

        for (size_t i = 0; i < 4; ++i) {
            threads.emplace_back([i, &bm, &num_unfixes] {
                std::mt19937_64 engine{i};
                std::uniform_int_distribution<PageID> distr(0, 400);

                for (size_t j = 0; j < 10000; ++j) {
                    PageID next_page = distr(engine);
                    auto& page = bm.fix_page(next_page, false);
                    bm.unfix_page(page, false);
                    ++num_unfixes;
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        assert(num_unfixes.load() == 40000);
    }

    cleanup_test_file();
    std::cout << "  Multithread many pages OK" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== BufferManager Tests ===" << std::endl;

    // Basic tests
    test_fix_single_page();
    test_persistence_across_restart();

    // Eviction tests
    test_fifo_eviction();
    test_buffer_full_error();
    test_move_to_lru();
    test_lru_refresh();

    // Concurrency tests
    test_multithread_parallel_fix();
    test_multithread_exclusive_access();
    test_multithread_buffer_full();
    test_multithread_many_pages();

    std::cout << "=== All BufferManager tests passed ===" << std::endl;
    return 0;
}
