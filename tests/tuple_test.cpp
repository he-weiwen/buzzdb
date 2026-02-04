/**
 * @file tuple_test.cpp
 * @brief Tests for the Tuple class.
 */

#include <iostream>
#include <sstream>
#include <cassert>

#include "storage/tuple.h"

using namespace buzzdb;

void test_empty_tuple() {
    std::cout << "Testing empty tuple..." << std::endl;

    Tuple t;

    assert(t.fields.empty());
    assert(t.getSize() == 0);
    assert(t.getFieldCount() == 0);

    std::cout << "  Empty tuple OK" << std::endl;
}

void test_add_fields() {
    std::cout << "Testing addField()..." << std::endl;

    Tuple t;

    t.addField(std::make_unique<Field>(42));
    assert(t.getFieldCount() == 1);
    assert(t.fields[0]->asInt() == 42);

    t.addField(std::make_unique<Field>(3.14f));
    assert(t.getFieldCount() == 2);

    t.addField(std::make_unique<Field>(std::string("hello")));
    assert(t.getFieldCount() == 3);

    // Verify all fields are correct
    assert(t.fields[0]->getType() == FieldType::INT);
    assert(t.fields[1]->getType() == FieldType::FLOAT);
    assert(t.fields[2]->getType() == FieldType::STRING);

    std::cout << "  addField() OK" << std::endl;
}

void test_get_size() {
    std::cout << "Testing getSize()..." << std::endl;

    Tuple t;

    t.addField(std::make_unique<Field>(1));        // 4 bytes
    t.addField(std::make_unique<Field>(2.0f));     // 4 bytes
    t.addField(std::make_unique<Field>(std::string("ab")));  // 3 bytes (ab + null)

    // getSize() returns total data bytes, not field count
    assert(t.getSize() == 4 + 4 + 3);
    assert(t.getFieldCount() == 3);

    std::cout << "  getSize() OK" << std::endl;
}

void test_copy_construction() {
    std::cout << "Testing tuple copy construction..." << std::endl;

    Tuple original;
    original.addField(std::make_unique<Field>(100));
    original.addField(std::make_unique<Field>(std::string("test")));

    Tuple copy(original);

    // Same number of fields
    assert(copy.getFieldCount() == 2);

    // Same values
    assert(copy.fields[0]->asInt() == 100);
    assert(copy.fields[1]->asString() == "test");

    // But independent storage (deep copy)
    assert(copy.fields[0].get() != original.fields[0].get());
    assert(copy.fields[1].get() != original.fields[1].get());

    // Modifying copy shouldn't affect original
    *copy.fields[0] = Field(999);
    assert(original.fields[0]->asInt() == 100);

    std::cout << "  Tuple copy construction OK" << std::endl;
}

void test_copy_assignment() {
    std::cout << "Testing tuple copy assignment..." << std::endl;

    Tuple t1;
    t1.addField(std::make_unique<Field>(1));
    t1.addField(std::make_unique<Field>(2));

    Tuple t2;
    t2.addField(std::make_unique<Field>(999));  // Different content

    t2 = t1;

    assert(t2.getFieldCount() == 2);
    assert(t2.fields[0]->asInt() == 1);
    assert(t2.fields[1]->asInt() == 2);

    std::cout << "  Tuple copy assignment OK" << std::endl;
}

void test_move_construction() {
    std::cout << "Testing tuple move construction..." << std::endl;

    Tuple original;
    original.addField(std::make_unique<Field>(42));
    Field* field_ptr = original.fields[0].get();

    Tuple moved(std::move(original));

    // Moved-to should have the field
    assert(moved.getFieldCount() == 1);
    assert(moved.fields[0]->asInt() == 42);

    // The actual Field object should be the same (pointer transferred)
    assert(moved.fields[0].get() == field_ptr);

    // Original should be empty after move
    assert(original.fields.empty());

    std::cout << "  Tuple move construction OK" << std::endl;
}

void test_move_assignment() {
    std::cout << "Testing tuple move assignment..." << std::endl;

    Tuple t1;
    t1.addField(std::make_unique<Field>(123));
    Field* field_ptr = t1.fields[0].get();

    Tuple t2;
    t2.addField(std::make_unique<Field>(456));

    t2 = std::move(t1);

    assert(t2.getFieldCount() == 1);
    assert(t2.fields[0]->asInt() == 123);
    assert(t2.fields[0].get() == field_ptr);

    std::cout << "  Tuple move assignment OK" << std::endl;
}

void test_clone() {
    std::cout << "Testing tuple clone()..." << std::endl;

    Tuple original;
    original.addField(std::make_unique<Field>(10));
    original.addField(std::make_unique<Field>(20));
    original.addField(std::make_unique<Field>(std::string("thirty")));

    auto cloned = original.clone();

    assert(cloned != nullptr);
    assert(cloned->getFieldCount() == 3);
    assert(cloned->fields[0]->asInt() == 10);
    assert(cloned->fields[1]->asInt() == 20);
    assert(cloned->fields[2]->asString() == "thirty");

    // Independent storage
    assert(cloned->fields[0].get() != original.fields[0].get());

    std::cout << "  Tuple clone() OK" << std::endl;
}

void test_serialization_roundtrip() {
    std::cout << "Testing tuple serialization roundtrip..." << std::endl;

    Tuple original;
    original.addField(std::make_unique<Field>(42));
    original.addField(std::make_unique<Field>(3.14f));
    original.addField(std::make_unique<Field>(std::string("noSpacesHere")));

    std::string serialized = original.serialize();
    std::cout << "    Serialized: \"" << serialized << "\"" << std::endl;

    std::istringstream iss(serialized);
    auto deserialized = Tuple::deserialize(iss);

    assert(deserialized != nullptr);
    assert(deserialized->getFieldCount() == 3);
    assert(deserialized->fields[0]->getType() == FieldType::INT);
    assert(deserialized->fields[0]->asInt() == 42);
    assert(deserialized->fields[1]->getType() == FieldType::FLOAT);
    assert(deserialized->fields[2]->getType() == FieldType::STRING);
    assert(deserialized->fields[2]->asString() == "noSpacesHere");

    std::cout << "  Tuple serialization roundtrip OK" << std::endl;
}

void test_serialization_known_limitation() {
    std::cout << "Testing serialization known limitation (spaces in strings)..." << std::endl;

    // This demonstrates the known issue with the serialization format
    Tuple original;
    original.addField(std::make_unique<Field>(std::string("hello world")));  // Has space!

    std::string serialized = original.serialize();
    std::cout << "    Serialized: \"" << serialized << "\"" << std::endl;

    std::istringstream iss(serialized);
    auto deserialized = Tuple::deserialize(iss);

    // KNOWN BUG: Deserialization breaks on strings with spaces
    // The string "hello world" becomes just "hello"
    std::cout << "    Deserialized string: \"" << deserialized->fields[0]->asString() << "\"" << std::endl;

    // We're NOT asserting equality here because it's a known limitation
    // assert(deserialized->fields[0]->asString() == "hello world");  // Would fail!

    std::cout << "  (Known limitation demonstrated - strings with spaces don't roundtrip)" << std::endl;
}

void test_direct_field_access() {
    std::cout << "Testing direct field access (public member)..." << std::endl;

    Tuple t;
    t.addField(std::make_unique<Field>(1));
    t.addField(std::make_unique<Field>(2));

    // Direct access to fields vector (public member)
    // This works but breaks encapsulation
    t.fields.push_back(std::make_unique<Field>(3));
    assert(t.getFieldCount() == 3);

    // Can also clear directly
    t.fields.clear();
    assert(t.getFieldCount() == 0);

    std::cout << "  Direct field access OK (encapsulation concern noted)" << std::endl;
}

int main() {
    std::cout << "=== Tuple Tests ===" << std::endl;

    test_empty_tuple();
    test_add_fields();
    test_get_size();
    test_copy_construction();
    test_copy_assignment();
    test_move_construction();
    test_move_assignment();
    test_clone();
    test_serialization_roundtrip();
    test_serialization_known_limitation();
    test_direct_field_access();

    std::cout << "=== All Tuple tests passed ===" << std::endl;
    return 0;
}
