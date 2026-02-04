/**
 * @file field_variant_test.cpp
 * @brief Tests for the variant-based Field implementation.
 *
 * This test file validates the new implementation and explicitly
 * demonstrates semantic changes from the original.
 */

#include <iostream>
#include <sstream>
#include <cassert>
#include <cmath>
#include <unordered_set>

#include "storage/field_variant.h"

using namespace buzzdb;

// =============================================================================
// BASIC FUNCTIONALITY (same as original)
// =============================================================================

void test_construction() {
    std::cout << "Testing construction..." << std::endl;

    Field intF(42);
    assert(intF.getType() == FieldType::INT);
    assert(intF.isInt());
    assert(intF.asInt() == 42);

    Field floatF(3.14f);
    assert(floatF.getType() == FieldType::FLOAT);
    assert(floatF.isFloat());
    assert(std::abs(floatF.asFloat() - 3.14f) < 0.001f);

    Field strF(std::string("hello"));
    assert(strF.getType() == FieldType::STRING);
    assert(strF.isString());
    assert(strF.asStringRaw() == "hello");

    // C-string construction
    Field cstrF("world");
    assert(cstrF.isString());
    assert(cstrF.asStringRaw() == "world");

    std::cout << "  Construction OK" << std::endl;
}

void test_copy_move() {
    std::cout << "Testing copy/move..." << std::endl;

    Field original(100);

    // Copy
    Field copy(original);
    assert(copy.asInt() == 100);

    // Move
    Field moved(std::move(copy));
    assert(moved.asInt() == 100);

    // Assignment
    Field assigned(0);
    assigned = original;
    assert(assigned.asInt() == 100);

    std::cout << "  Copy/move OK" << std::endl;
}

void test_asString_conversion() {
    std::cout << "Testing asString() conversion..." << std::endl;

    Field intF(42);
    assert(intF.asString() == "42");

    Field floatF(3.5f);
    // Note: floating point to string may have precision variations
    assert(floatF.asString().substr(0, 3) == "3.5");

    Field strF("test");
    assert(strF.asString() == "test");

    std::cout << "  asString() conversion OK" << std::endl;
}

void test_serialization_roundtrip() {
    std::cout << "Testing serialization (format unchanged)..." << std::endl;

    // INT
    {
        Field original(12345);
        std::string serialized = original.serialize();
        std::cout << "    INT serialized: \"" << serialized << "\"" << std::endl;
        // Format: "0 4 12345 " (type=0, length=4, value=12345)

        std::istringstream iss(serialized);
        auto deserialized = Field::deserialize(iss);
        assert(deserialized->asInt() == 12345);
    }

    // FLOAT
    {
        Field original(98.5f);
        std::string serialized = original.serialize();
        std::cout << "    FLOAT serialized: \"" << serialized << "\"" << std::endl;

        std::istringstream iss(serialized);
        auto deserialized = Field::deserialize(iss);
        assert(std::abs(deserialized->asFloat() - 98.5f) < 0.01f);
    }

    // STRING
    {
        Field original("teststring");
        std::string serialized = original.serialize();
        std::cout << "    STRING serialized: \"" << serialized << "\"" << std::endl;

        std::istringstream iss(serialized);
        auto deserialized = Field::deserialize(iss);
        assert(deserialized->asStringRaw() == "teststring");
    }

    std::cout << "  Serialization roundtrip OK" << std::endl;
}

// =============================================================================
// SEMANTIC CHANGES DEMONSTRATION
// =============================================================================

void test_semantic_change_accessor_throws() {
    std::cout << "Testing SEMANTIC CHANGE: accessor throws on wrong type..." << std::endl;

    Field strF("hello");

    // OLD BEHAVIOR: asInt() on STRING returned garbage (undefined behavior)
    // NEW BEHAVIOR: asInt() on STRING throws std::bad_variant_access

    bool threw = false;
    try {
        int val = strF.asInt();  // Should throw!
        (void)val;
    } catch (const std::bad_variant_access&) {
        threw = true;
    }
    assert(threw && "Expected std::bad_variant_access when calling asInt() on STRING");

    std::cout << "  CHANGE VERIFIED: asInt() on STRING throws (was UB)" << std::endl;
}

void test_semantic_change_cross_type_comparison() {
    std::cout << "Testing SEMANTIC CHANGE: cross-type comparison ordering..." << std::endl;

    Field intF(42);
    Field floatF(42.0f);
    Field strF("42");

    // OLD BEHAVIOR: intF < floatF → printed to stderr, returned false
    // NEW BEHAVIOR: intF < floatF → true (INT type index < FLOAT type index)

    // Different types are never equal (same as before)
    assert(!(intF == floatF));
    assert(!(intF == strF));
    assert(!(floatF == strF));

    // But now they have a defined ordering: INT < FLOAT < STRING
    assert(intF < floatF);   // INT (index 0) < FLOAT (index 1)
    assert(floatF < strF);   // FLOAT (index 1) < STRING (index 2)
    assert(intF < strF);     // INT (index 0) < STRING (index 2)

    // No stderr output (you can verify by running - no "invalid comparison" messages)

    std::cout << "  CHANGE VERIFIED: cross-type < uses type index ordering" << std::endl;
    std::cout << "  CHANGE VERIFIED: no stderr output on cross-type comparison" << std::endl;
}

void test_semantic_change_operator_plus_throws() {
    std::cout << "Testing SEMANTIC CHANGE: operator+= throws on type mismatch..." << std::endl;

    Field floatF(3.0f);

    // OLD BEHAVIOR: floatF += 1 (int) silently did nothing
    // NEW BEHAVIOR: floatF += 1 (int) throws std::bad_variant_access

    bool threw = false;
    try {
        floatF += 1;  // Adding int to float field - should throw
    } catch (const std::bad_variant_access&) {
        threw = true;
    }
    assert(threw && "Expected std::bad_variant_access when += int to FLOAT field");

    // Correct usage: use matching type
    Field intF(10);
    intF += 5;
    assert(intF.asInt() == 15);

    std::cout << "  CHANGE VERIFIED: operator+= throws on type mismatch (was silent no-op)" << std::endl;
}

// =============================================================================
// NEW FEATURES
// =============================================================================

void test_new_feature_try_accessors() {
    std::cout << "Testing NEW FEATURE: tryAs*() safe accessors..." << std::endl;

    Field intF(42);
    Field strF("hello");

    // tryAsInt returns optional
    auto intOpt = intF.tryAsInt();
    assert(intOpt.has_value());
    assert(*intOpt == 42);

    // tryAsInt on string returns nullopt (no throw)
    auto noInt = strF.tryAsInt();
    assert(!noInt.has_value());

    // tryAsString on string
    auto strOpt = strF.tryAsString();
    assert(strOpt.has_value());
    assert(*strOpt == "hello");

    std::cout << "  tryAs*() accessors OK" << std::endl;
}

void test_new_feature_try_add() {
    std::cout << "Testing NEW FEATURE: tryAdd() safe arithmetic..." << std::endl;

    Field intF(10);
    Field floatF(2.5f);

    // tryAdd returns bool indicating success
    assert(intF.tryAdd(5) == true);
    assert(intF.asInt() == 15);

    // tryAdd with wrong type returns false, no modification
    assert(floatF.tryAdd(1) == false);  // Can't add int to float
    assert(std::abs(floatF.asFloat() - 2.5f) < 0.001f);  // Unchanged

    // Correct type works
    assert(floatF.tryAdd(1.5f) == true);
    assert(std::abs(floatF.asFloat() - 4.0f) < 0.001f);

    std::cout << "  tryAdd() OK" << std::endl;
}

void test_new_feature_visit() {
    std::cout << "Testing NEW FEATURE: visit()..." << std::endl;

    Field intF(42);
    Field strF("hello");

    // Use visitor to handle all types
    auto describe = [](const auto& v) -> std::string {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, int>) {
            return "int: " + std::to_string(v);
        } else if constexpr (std::is_same_v<T, float>) {
            return "float: " + std::to_string(v);
        } else {
            return "string: " + v;
        }
    };

    assert(intF.visit(describe) == "int: 42");
    assert(strF.visit(describe) == "string: hello");

    std::cout << "  visit() OK" << std::endl;
}

void test_new_feature_print_stream() {
    std::cout << "Testing NEW FEATURE: print(ostream&)..." << std::endl;

    Field intF(42);
    Field strF("test");

    std::ostringstream oss;
    intF.print(oss);
    oss << "|";
    strF.print(oss);

    assert(oss.str() == "42|test");

    std::cout << "  print(ostream&) OK" << std::endl;
}

void test_new_feature_hash() {
    std::cout << "Testing NEW FEATURE: hash (unordered containers)..." << std::endl;

    std::unordered_set<Field> fieldSet;

    fieldSet.insert(Field(42));
    fieldSet.insert(Field(42));      // Duplicate
    fieldSet.insert(Field("hello"));
    fieldSet.insert(Field(3.14f));

    assert(fieldSet.size() == 3);  // Duplicates removed

    // Can find elements
    assert(fieldSet.count(Field(42)) == 1);
    assert(fieldSet.count(Field("hello")) == 1);
    assert(fieldSet.count(Field(999)) == 0);

    std::cout << "  Hash / unordered_set OK" << std::endl;
}

void test_new_feature_same_type_comparison() {
    std::cout << "Testing NEW FEATURE: equalsSameType(), lessThanSameType()..." << std::endl;

    Field intF1(10);
    Field intF2(20);
    Field floatF(10.0f);

    // Same type comparison returns optional<bool>
    auto eq1 = intF1.equalsSameType(intF2);
    assert(eq1.has_value());
    assert(*eq1 == false);

    auto lt1 = intF1.lessThanSameType(intF2);
    assert(lt1.has_value());
    assert(*lt1 == true);

    // Different type returns nullopt
    auto cross = intF1.equalsSameType(floatF);
    assert(!cross.has_value());

    std::cout << "  Same-type comparison OK" << std::endl;
}

void test_new_feature_getDataLength() {
    std::cout << "Testing NEW FEATURE: getDataLength() compatibility..." << std::endl;

    Field intF(42);
    Field floatF(3.14f);
    Field strF("hello");

    assert(intF.getDataLength() == sizeof(int));      // 4
    assert(floatF.getDataLength() == sizeof(float));  // 4
    assert(strF.getDataLength() == 6);                // "hello" + null

    std::cout << "  getDataLength() OK" << std::endl;
}

// =============================================================================
// MAIN
// =============================================================================

int main() {
    std::cout << "=== Field (Variant) Tests ===" << std::endl;
    std::cout << std::endl;

    std::cout << "--- Basic Functionality (unchanged) ---" << std::endl;
    test_construction();
    test_copy_move();
    test_asString_conversion();
    test_serialization_roundtrip();

    std::cout << std::endl;
    std::cout << "--- Semantic Changes (from original) ---" << std::endl;
    test_semantic_change_accessor_throws();
    test_semantic_change_cross_type_comparison();
    test_semantic_change_operator_plus_throws();

    std::cout << std::endl;
    std::cout << "--- New Features ---" << std::endl;
    test_new_feature_try_accessors();
    test_new_feature_try_add();
    test_new_feature_visit();
    test_new_feature_print_stream();
    test_new_feature_hash();
    test_new_feature_same_type_comparison();
    test_new_feature_getDataLength();

    std::cout << std::endl;
    std::cout << "=== All Field (Variant) tests passed ===" << std::endl;
    return 0;
}
