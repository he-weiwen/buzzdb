/**
 * @file field_test.cpp
 * @brief Tests for the Field class.
 */

#include <iostream>
#include <sstream>
#include <cassert>
#include <cmath>

#include "storage/field.h"

using namespace buzzdb;

void test_int_field_construction() {
    std::cout << "Testing INT field construction..." << std::endl;

    Field f(42);

    assert(f.getType() == FieldType::INT);
    assert(f.asInt() == 42);
    assert(f.data_length == sizeof(int));

    // asString should convert int to string representation
    assert(f.asString() == "42");

    std::cout << "  INT field construction OK" << std::endl;
}

void test_float_field_construction() {
    std::cout << "Testing FLOAT field construction..." << std::endl;

    Field f(3.14f);

    assert(f.getType() == FieldType::FLOAT);
    assert(std::abs(f.asFloat() - 3.14f) < 0.001f);
    assert(f.data_length == sizeof(float));

    std::cout << "  FLOAT field construction OK" << std::endl;
}

void test_string_field_construction() {
    std::cout << "Testing STRING field construction..." << std::endl;

    Field f(std::string("hello"));

    assert(f.getType() == FieldType::STRING);
    assert(f.asString() == "hello");
    assert(f.data_length == 6);  // "hello" + null terminator

    std::cout << "  STRING field construction OK" << std::endl;
}

void test_copy_construction() {
    std::cout << "Testing copy construction..." << std::endl;

    Field original(100);
    Field copy(original);

    // Values should be equal
    assert(copy.asInt() == 100);
    assert(copy.getType() == FieldType::INT);

    // But data buffers should be independent
    assert(copy.data.get() != original.data.get());

    std::cout << "  Copy construction OK" << std::endl;
}

void test_move_construction() {
    std::cout << "Testing move construction..." << std::endl;

    Field original(200);
    char* original_data = original.data.get();

    Field moved(std::move(original));

    // Moved-to should have the value
    assert(moved.asInt() == 200);

    // Data pointer should have been transferred
    assert(moved.data.get() == original_data);

    // Original's data should be null after move
    assert(original.data.get() == nullptr);

    std::cout << "  Move construction OK" << std::endl;
}

void test_copy_assignment() {
    std::cout << "Testing copy assignment..." << std::endl;

    Field f1(10);
    Field f2(20);

    f2 = f1;

    assert(f2.asInt() == 10);
    assert(f2.data.get() != f1.data.get());  // Independent copies

    // Self-assignment should work
    f1 = f1;
    assert(f1.asInt() == 10);

    std::cout << "  Copy assignment OK" << std::endl;
}

void test_move_assignment() {
    std::cout << "Testing move assignment..." << std::endl;

    Field f1(30);
    Field f2(40);
    char* f1_data = f1.data.get();

    f2 = std::move(f1);

    assert(f2.asInt() == 30);
    assert(f2.data.get() == f1_data);

    std::cout << "  Move assignment OK" << std::endl;
}

void test_comparison_operators() {
    std::cout << "Testing comparison operators..." << std::endl;

    // INT comparisons
    Field i1(10);
    Field i2(10);
    Field i3(20);

    assert(i1 == i2);
    assert(!(i1 == i3));
    assert(i1 != i3);
    assert(i1 < i3);
    assert(i3 > i1);
    assert(i1 <= i2);
    assert(i1 <= i3);
    assert(i3 >= i1);
    assert(i1 >= i2);

    // STRING comparisons
    Field s1(std::string("apple"));
    Field s2(std::string("apple"));
    Field s3(std::string("banana"));

    assert(s1 == s2);
    assert(s1 < s3);  // "apple" < "banana"

    // Different types
    Field intF(42);
    Field strF(std::string("42"));
    assert(!(intF == strF));  // Different types are not equal

    std::cout << "  Comparison operators OK" << std::endl;
}

void test_arithmetic_operators() {
    std::cout << "Testing arithmetic operators..." << std::endl;

    Field intF(10);
    intF += 5;
    assert(intF.asInt() == 15);

    Field floatF(2.5f);
    floatF += 1.5f;
    assert(std::abs(floatF.asFloat() - 4.0f) < 0.001f);

    // Adding int to float field should do nothing (silent ignore)
    Field floatF2(3.0f);
    floatF2 += 1;  // This does nothing
    assert(std::abs(floatF2.asFloat() - 3.0f) < 0.001f);

    std::cout << "  Arithmetic operators OK" << std::endl;
}

void test_serialization_roundtrip() {
    std::cout << "Testing serialization roundtrip..." << std::endl;

    // INT
    {
        Field original(12345);
        std::string serialized = original.serialize();
        std::istringstream iss(serialized);
        auto deserialized = Field::deserialize(iss);

        assert(deserialized != nullptr);
        assert(deserialized->getType() == FieldType::INT);
        assert(deserialized->asInt() == 12345);
    }

    // FLOAT
    {
        Field original(98.76f);
        std::string serialized = original.serialize();
        std::istringstream iss(serialized);
        auto deserialized = Field::deserialize(iss);

        assert(deserialized != nullptr);
        assert(deserialized->getType() == FieldType::FLOAT);
        // Note: float serialization may lose precision
        assert(std::abs(deserialized->asFloat() - 98.76f) < 0.01f);
    }

    // STRING (without spaces - spaces would break!)
    {
        Field original(std::string("teststring"));
        std::string serialized = original.serialize();
        std::istringstream iss(serialized);
        auto deserialized = Field::deserialize(iss);

        assert(deserialized != nullptr);
        assert(deserialized->getType() == FieldType::STRING);
        assert(deserialized->asString() == "teststring");
    }

    std::cout << "  Serialization roundtrip OK" << std::endl;
}

void test_clone() {
    std::cout << "Testing clone()..." << std::endl;

    Field original(999);
    auto cloned = original.clone();

    assert(cloned != nullptr);
    assert(cloned->asInt() == 999);
    assert(cloned->data.get() != original.data.get());  // Independent copy

    std::cout << "  clone() OK" << std::endl;
}

int main() {
    std::cout << "=== Field Tests ===" << std::endl;

    test_int_field_construction();
    test_float_field_construction();
    test_string_field_construction();
    test_copy_construction();
    test_move_construction();
    test_copy_assignment();
    test_move_assignment();
    test_comparison_operators();
    test_arithmetic_operators();
    test_serialization_roundtrip();
    test_clone();

    std::cout << "=== All Field tests passed ===" << std::endl;
    return 0;
}
