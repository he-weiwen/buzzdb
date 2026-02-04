/**
 * @file slotted_page_test.cpp
 * @brief Tests for SlottedPage class.
 */

#include <iostream>
#include <sstream>
#include <cassert>

#include "storage/slotted_page.h"

using namespace buzzdb;

void test_empty_page() {
    std::cout << "Testing empty page..." << std::endl;

    SlottedPage page;

    // Verify initial state
    assert(page.countTuples() == 0);
    assert(page.metadata_size == sizeof(Slot) * MAX_SLOTS);

    // All slots should be empty
    const Slot* slots = page.getSlotArray();
    for (size_t i = 0; i < MAX_SLOTS; i++) {
        assert(slots[i].empty == true);
        assert(slots[i].offset == INVALID_VALUE);
        assert(slots[i].length == INVALID_VALUE);
    }

    std::cout << "  Empty page OK" << std::endl;
}

void test_add_single_tuple() {
    std::cout << "Testing add single tuple..." << std::endl;

    SlottedPage page;

    // Create a simple tuple
    auto tuple = std::make_unique<Tuple>();
    tuple->addField(std::make_unique<Field>(42));
    tuple->addField(std::make_unique<Field>(std::string("hello")));

    bool added = page.addTuple(std::move(tuple));
    assert(added);
    assert(page.countTuples() == 1);

    // Verify slot 0 is now occupied
    const Slot* slots = page.getSlotArray();
    assert(slots[0].empty == false);
    assert(slots[0].offset >= page.metadata_size);
    assert(slots[0].length > 0);

    std::cout << "  Add single tuple OK" << std::endl;
}

void test_add_multiple_tuples() {
    std::cout << "Testing add multiple tuples..." << std::endl;

    SlottedPage page;

    // Add several tuples
    for (int i = 0; i < 10; i++) {
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(i));
        tuple->addField(std::make_unique<Field>(std::string("test")));

        bool added = page.addTuple(std::move(tuple));
        assert(added);
    }

    assert(page.countTuples() == 10);

    // Verify slots are sequential
    const Slot* slots = page.getSlotArray();
    for (int i = 0; i < 10; i++) {
        assert(slots[i].empty == false);
        if (i > 0) {
            // Each tuple should be after the previous one
            assert(slots[i].offset > slots[i-1].offset);
        }
    }

    std::cout << "  Add multiple tuples OK" << std::endl;
}

void test_delete_tuple() {
    std::cout << "Testing delete tuple..." << std::endl;

    SlottedPage page;

    // Add tuples
    for (int i = 0; i < 5; i++) {
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(i));
        page.addTuple(std::move(tuple));
    }

    assert(page.countTuples() == 5);

    // Delete middle tuple
    page.deleteTuple(2);
    assert(page.countTuples() == 4);

    // Verify slot 2 is now empty but others are not
    const Slot* slots = page.getSlotArray();
    assert(slots[0].empty == false);
    assert(slots[1].empty == false);
    assert(slots[2].empty == true);  // Deleted
    assert(slots[3].empty == false);
    assert(slots[4].empty == false);

    // Delete first and last
    page.deleteTuple(0);
    page.deleteTuple(4);
    assert(page.countTuples() == 2);

    std::cout << "  Delete tuple OK" << std::endl;
}

void test_page_full() {
    std::cout << "Testing page full behavior..." << std::endl;

    SlottedPage page;

    // Add tuples until page is full
    // Each tuple is roughly "2 0 4 <int> " = ~10 bytes + field count
    // With metadata overhead, we can fit many small tuples
    int count = 0;
    while (count < MAX_SLOTS) {
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(count));

        bool added = page.addTuple(std::move(tuple));
        if (!added) {
            break;  // Page full
        }
        count++;
    }

    std::cout << "    Added " << count << " tuples before page full" << std::endl;

    // Should have added some but not all MAX_SLOTS (due to data space limit)
    assert(count > 0);
    assert(count < MAX_SLOTS);  // Data space should run out before slot space

    std::cout << "  Page full behavior OK" << std::endl;
}

void test_tuple_retrieval() {
    std::cout << "Testing tuple retrieval..." << std::endl;

    SlottedPage page;

    // Add a tuple with known values
    auto tuple = std::make_unique<Tuple>();
    tuple->addField(std::make_unique<Field>(12345));
    tuple->addField(std::make_unique<Field>(std::string("retrieve_me")));
    page.addTuple(std::move(tuple));

    // Retrieve tuple data
    const char* data = page.getTupleData(0);
    size_t length = page.getTupleLength(0);

    assert(data != nullptr);
    assert(length > 0);

    // Deserialize and verify
    std::istringstream iss(std::string(data, length));
    auto retrieved = Tuple::deserialize(iss);

    assert(retrieved->fields.size() == 2);
    assert(retrieved->fields[0]->asInt() == 12345);
    assert(retrieved->fields[1]->asString() == "retrieve_me");

    std::cout << "  Tuple retrieval OK" << std::endl;
}

void test_invalid_slot_access() {
    std::cout << "Testing invalid slot access..." << std::endl;

    SlottedPage page;

    // Empty slot should return nullptr
    assert(page.getTupleData(0) == nullptr);
    assert(page.getTupleLength(0) == 0);

    // Out of range slot should return nullptr
    assert(page.getTupleData(MAX_SLOTS + 1) == nullptr);
    assert(page.getTupleLength(MAX_SLOTS + 1) == 0);

    // Add a tuple, then check adjacent empty slot
    auto tuple = std::make_unique<Tuple>();
    tuple->addField(std::make_unique<Field>(1));
    page.addTuple(std::move(tuple));

    assert(page.getTupleData(0) != nullptr);  // Has tuple
    assert(page.getTupleData(1) == nullptr);  // Empty

    std::cout << "  Invalid slot access OK" << std::endl;
}

void test_print() {
    std::cout << "Testing print()..." << std::endl;

    SlottedPage page;

    auto tuple1 = std::make_unique<Tuple>();
    tuple1->addField(std::make_unique<Field>(100));
    page.addTuple(std::move(tuple1));

    auto tuple2 = std::make_unique<Tuple>();
    tuple2->addField(std::make_unique<Field>(200));
    page.addTuple(std::move(tuple2));

    // Print to stringstream
    std::ostringstream oss;
    page.print(oss);

    std::string output = oss.str();
    std::cout << "    Output: " << output;

    // Should contain both tuple values
    assert(output.find("100") != std::string::npos);
    assert(output.find("200") != std::string::npos);

    std::cout << "  print() OK" << std::endl;
}

void test_data_access() {
    std::cout << "Testing raw data access..." << std::endl;

    SlottedPage page;

    // Verify data() returns valid pointer
    char* data = page.data();
    const char* const_data = static_cast<const SlottedPage&>(page).data();

    assert(data != nullptr);
    assert(const_data != nullptr);
    assert(data == const_data);

    // Verify page_data member is accessible (public, per original design)
    assert(page.page_data.get() == data);

    std::cout << "  Raw data access OK" << std::endl;
}

int main() {
    std::cout << "=== SlottedPage Tests ===" << std::endl;

    test_empty_page();
    test_add_single_tuple();
    test_add_multiple_tuples();
    test_delete_tuple();
    test_page_full();
    test_tuple_retrieval();
    test_invalid_slot_access();
    test_print();
    test_data_access();

    std::cout << "=== All SlottedPage tests passed ===" << std::endl;
    return 0;
}
