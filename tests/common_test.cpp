/**
 * @file common_test.cpp
 * @brief Tests for common types and configuration.
 */

#include <iostream>
#include <cassert>
#include <unordered_set>

#include "common/config.h"
#include "common/types.h"

using namespace buzzdb;

void test_config_constants() {
    std::cout << "Testing config constants..." << std::endl;

    // Verify constants have expected values
    assert(PAGE_SIZE == 4096);
    assert(MAX_SLOTS == 512);
    assert(MAX_PAGES == 1000);
    assert(MAX_PAGES_IN_MEMORY == 10);
    assert(INVALID_VALUE == 65535);  // std::numeric_limits<uint16_t>::max()

    // Verify slot metadata fits within page
    // Each slot is roughly 5 bytes (bool + 2*uint16_t), 512 slots = ~2560 bytes
    // This leaves room for tuple data
    static_assert(sizeof(bool) + 2 * sizeof(uint16_t) <= 8,
                  "Slot size assumption check");

    std::cout << "  PAGE_SIZE: " << PAGE_SIZE << " bytes" << std::endl;
    std::cout << "  MAX_SLOTS: " << MAX_SLOTS << std::endl;
    std::cout << "  MAX_PAGES_IN_MEMORY: " << MAX_PAGES_IN_MEMORY << std::endl;
    std::cout << "  Config constants OK" << std::endl;
}

void test_type_aliases() {
    std::cout << "Testing type aliases..." << std::endl;

    // Verify types have expected sizes
    static_assert(sizeof(PageID) == 2, "PageID should be 2 bytes");
    static_assert(sizeof(FrameID) == 8, "FrameID should be 8 bytes");
    static_assert(sizeof(SlotID) == 2, "SlotID should be 2 bytes");

    // Verify we can use the types
    PageID page = 42;
    FrameID frame = 100;
    SlotID slot = 7;

    assert(page == 42);
    assert(frame == 100);
    assert(slot == 7);

    std::cout << "  Type aliases OK" << std::endl;
}

void test_field_type_enum() {
    std::cout << "Testing FieldType enum..." << std::endl;

    // Verify enum values are distinct
    assert(FieldType::INT != FieldType::FLOAT);
    assert(FieldType::FLOAT != FieldType::STRING);
    assert(FieldType::INT != FieldType::STRING);

    // Verify we can use in switch
    FieldType ft = FieldType::INT;
    switch (ft) {
        case FieldType::INT:
            break;
        case FieldType::FLOAT:
            assert(false && "Should not reach FLOAT");
            break;
        case FieldType::STRING:
            assert(false && "Should not reach STRING");
            break;
    }

    std::cout << "  FieldType enum OK" << std::endl;
}

void test_rid_struct() {
    std::cout << "Testing RID struct..." << std::endl;

    // Create RIDs
    RID rid1{10, 5};
    RID rid2{10, 5};
    RID rid3{10, 6};
    RID rid4{11, 5};

    // Test equality
    assert(rid1 == rid2);
    assert(!(rid1 != rid2));

    // Test inequality (different slot)
    assert(rid1 != rid3);
    assert(!(rid1 == rid3));

    // Test inequality (different page)
    assert(rid1 != rid4);

    // Test in unordered_set (uses hash function)
    std::unordered_set<RID> rid_set;
    rid_set.insert(rid1);
    rid_set.insert(rid2);  // Duplicate, should not increase size
    rid_set.insert(rid3);

    assert(rid_set.size() == 2);
    assert(rid_set.count(rid1) == 1);
    assert(rid_set.count(rid3) == 1);
    assert(rid_set.count(rid4) == 0);

    std::cout << "  RID struct OK" << std::endl;
}

int main() {
    std::cout << "=== Common Types Test ===" << std::endl;

    test_config_constants();
    test_type_aliases();
    test_field_type_enum();
    test_rid_struct();

    std::cout << "=== All tests passed ===" << std::endl;
    return 0;
}
