/**
 * @file policy_test.cpp
 * @brief Tests for LRU and 2Q replacement policies.
 */

#include <iostream>
#include <cassert>
#include <vector>

#include "buffer/lru_policy.h"
#include "buffer/two_q_policy.h"

using namespace buzzdb;

// ============================================================================
// LRU Policy Tests
// ============================================================================

void test_lru_empty() {
    std::cout << "Testing LRU empty state..." << std::endl;

    LRUPolicy lru;

    assert(lru.empty());
    assert(lru.size() == 0);
    assert(!lru.contains(0));

    bool threw = false;
    try {
        lru.evict();
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "Expected exception on evict from empty LRU");

    std::cout << "  LRU empty state OK" << std::endl;
}

void test_lru_basic_touch() {
    std::cout << "Testing LRU basic touch..." << std::endl;

    LRUPolicy lru;

    // First touch should return false (not already present)
    assert(!lru.touch(1));
    assert(lru.size() == 1);
    assert(lru.contains(1));

    // Second touch should return true (already present)
    assert(lru.touch(1));
    assert(lru.size() == 1);

    // Touch another page
    assert(!lru.touch(2));
    assert(lru.size() == 2);

    std::cout << "  LRU basic touch OK" << std::endl;
}

void test_lru_eviction_order() {
    std::cout << "Testing LRU eviction order..." << std::endl;

    LRUPolicy lru;

    // Add pages 1, 2, 3
    lru.touch(1);
    lru.touch(2);
    lru.touch(3);

    // LRU order should be [1, 2, 3] (1 is least recent)
    auto list = lru.get_list();
    assert(list == (std::vector<PageID>{1, 2, 3}));

    // Evict should return 1
    assert(lru.evict() == 1);
    assert(lru.size() == 2);
    assert(!lru.contains(1));

    // Touch 2 to make it most recent
    lru.touch(2);

    // Order should be [3, 2]
    list = lru.get_list();
    assert(list == (std::vector<PageID>{3, 2}));

    // Evict should return 3
    assert(lru.evict() == 3);

    std::cout << "  LRU eviction order OK" << std::endl;
}

void test_lru_remove() {
    std::cout << "Testing LRU remove..." << std::endl;

    LRUPolicy lru;

    lru.touch(1);
    lru.touch(2);
    lru.touch(3);

    // Remove middle element
    lru.remove(2);
    assert(lru.size() == 2);
    assert(!lru.contains(2));
    assert(lru.contains(1));
    assert(lru.contains(3));

    // Remove non-existent element (should be no-op)
    lru.remove(99);
    assert(lru.size() == 2);

    std::cout << "  LRU remove OK" << std::endl;
}

// ============================================================================
// 2Q Policy Tests
// ============================================================================

void test_2q_empty() {
    std::cout << "Testing 2Q empty state..." << std::endl;

    TwoQPolicy tq;

    assert(tq.size() == 0);
    assert(!tq.contains(0));
    assert(tq.get_fifo_list().empty());
    assert(tq.get_lru_list().empty());

    bool threw = false;
    try {
        tq.evict();
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "Expected exception on evict from empty 2Q");

    std::cout << "  2Q empty state OK" << std::endl;
}

void test_2q_first_touch_goes_to_fifo() {
    std::cout << "Testing 2Q first touch goes to FIFO..." << std::endl;

    TwoQPolicy tq;

    // First touch should go to FIFO
    assert(!tq.touch(1));
    assert(tq.get_fifo_list() == std::vector<PageID>{1});
    assert(tq.get_lru_list().empty());

    // Add more pages
    tq.touch(2);
    tq.touch(3);

    assert(tq.get_fifo_list() == (std::vector<PageID>{1, 2, 3}));
    assert(tq.get_lru_list().empty());

    std::cout << "  2Q first touch goes to FIFO OK" << std::endl;
}

void test_2q_second_touch_promotes_to_lru() {
    std::cout << "Testing 2Q second touch promotes to LRU..." << std::endl;

    TwoQPolicy tq;

    // First touch - goes to FIFO
    tq.touch(1);
    tq.touch(2);

    assert(tq.get_fifo_list() == (std::vector<PageID>{1, 2}));
    assert(tq.get_lru_list().empty());

    // Second touch of page 1 - should promote to LRU
    assert(tq.touch(1));  // returns true because already tracked
    assert(tq.get_fifo_list() == std::vector<PageID>{2});
    assert(tq.get_lru_list() == std::vector<PageID>{1});

    // Second touch of page 2
    tq.touch(2);
    assert(tq.get_fifo_list().empty());
    assert(tq.get_lru_list() == (std::vector<PageID>{1, 2}));

    std::cout << "  2Q second touch promotes to LRU OK" << std::endl;
}

void test_2q_lru_touch_refreshes() {
    std::cout << "Testing 2Q LRU touch refreshes..." << std::endl;

    TwoQPolicy tq;

    // Add pages and promote to LRU
    tq.touch(1);
    tq.touch(2);
    tq.touch(1);  // promote 1
    tq.touch(2);  // promote 2

    // LRU order: [1, 2]
    assert(tq.get_lru_list() == (std::vector<PageID>{1, 2}));

    // Touch 1 again - should move to end
    tq.touch(1);
    assert(tq.get_lru_list() == (std::vector<PageID>{2, 1}));

    std::cout << "  2Q LRU touch refreshes OK" << std::endl;
}

void test_2q_eviction_prefers_fifo() {
    std::cout << "Testing 2Q eviction prefers FIFO..." << std::endl;

    TwoQPolicy tq;

    // Add some pages - mix of FIFO and LRU
    tq.touch(1);
    tq.touch(2);
    tq.touch(3);
    tq.touch(1);  // promote 1 to LRU

    // FIFO: [2, 3], LRU: [1]

    // Evict should take from FIFO first
    assert(tq.evict() == 2);
    assert(tq.get_fifo_list() == std::vector<PageID>{3});

    assert(tq.evict() == 3);
    assert(tq.get_fifo_list().empty());

    // Now evict from LRU
    assert(tq.evict() == 1);
    assert(tq.get_lru_list().empty());

    std::cout << "  2Q eviction prefers FIFO OK" << std::endl;
}

void test_2q_evict_with_state() {
    std::cout << "Testing 2Q evict with state..." << std::endl;

    TwoQPolicy tq;

    tq.touch(1);
    tq.touch(2);
    tq.touch(3);

    // Simulate page states
    std::unordered_map<PageID, PageState> states;
    states[1] = 1;            // shared lock
    states[2] = PAGE_UNFIXED; // unfixed
    states[3] = PAGE_EXCLUSIVE; // exclusive lock

    // Should skip 1 (pinned) and evict 2 (unfixed)
    assert(tq.evict(states) == 2);

    // Now only 1 and 3 remain, both pinned
    states.erase(2);
    bool threw = false;
    try {
        tq.evict(states);
    } catch (const buffer_full_error&) {
        threw = true;
    }
    assert(threw && "Expected buffer_full_error when all pages pinned");

    std::cout << "  2Q evict with state OK" << std::endl;
}

void test_2q_remove() {
    std::cout << "Testing 2Q remove..." << std::endl;

    TwoQPolicy tq;

    tq.touch(1);
    tq.touch(2);
    tq.touch(1);  // promote to LRU

    // FIFO: [2], LRU: [1]

    // Remove from LRU
    tq.remove(1);
    assert(!tq.contains(1));
    assert(tq.get_lru_list().empty());

    // Remove from FIFO
    tq.remove(2);
    assert(!tq.contains(2));
    assert(tq.get_fifo_list().empty());

    std::cout << "  2Q remove OK" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Policy Tests ===" << std::endl;

    // LRU tests
    test_lru_empty();
    test_lru_basic_touch();
    test_lru_eviction_order();
    test_lru_remove();

    // 2Q tests
    test_2q_empty();
    test_2q_first_touch_goes_to_fifo();
    test_2q_second_touch_promotes_to_lru();
    test_2q_lru_touch_refreshes();
    test_2q_eviction_prefers_fifo();
    test_2q_evict_with_state();
    test_2q_remove();

    std::cout << "=== All Policy tests passed ===" << std::endl;
    return 0;
}
