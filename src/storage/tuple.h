#pragma once

/**
 * @file tuple.h
 * @brief Tuple class - a row of fields in BuzzDB.
 *
 * A Tuple is an ordered collection of Fields, representing a single row
 * in a relation (table). Tuples are the primary unit of data manipulation
 * in the query execution layer.
 *
 * ============================================================================
 * DESIGN CRITIQUE (preserving original design, documenting concerns)
 * ============================================================================
 *
 * 1. PUBLIC DATA MEMBER
 *    `fields` is public, allowing external code to directly manipulate the
 *    vector. This breaks encapsulation and makes it hard to maintain invariants.
 *    Should be private with accessor methods.
 *
 * 2. OWNERSHIP MODEL
 *    Uses vector<unique_ptr<Field>>, meaning:
 *    - Each tuple owns its fields (good for memory safety)
 *    - Moving a tuple is cheap (moves pointers, not data)
 *    - But copying requires deep clone (expensive)
 *    - Field access requires indirection through pointer
 *
 *    Alternative: vector<Field> with value semantics would be simpler but
 *    require Field to be copyable (which it now is).
 *
 * 3. getSize() RETURNS DATA SIZE, NOT FIELD COUNT
 *    The name is ambiguous. It returns sum of field data_lengths, not the
 *    number of fields. Should be named getDataSize() or getTotalBytes().
 *    For field count, use fields.size() directly (since fields is public).
 *
 * 4. SERIALIZATION INHERITS Field's PROBLEMS
 *    Since it delegates to Field::serialize(), it inherits all the issues:
 *    - Text-based (inefficient)
 *    - Strings with spaces break parsing
 *    - Format: "field_count field1 field2 ..."
 *
 * 5. NO SCHEMA INFORMATION
 *    Tuples don't know their schema (column names, expected types).
 *    This is typical for a "physical" tuple representation, but means:
 *    - No validation that fields match expected schema
 *    - Field access is by index only, not by name
 *    - Type errors are runtime, not compile-time
 *
 * 6. NO EQUALITY OPERATOR
 *    Cannot compare two tuples for equality directly.
 *    Have to manually compare fields.
 *
 * 7. NO MOVE OPERATIONS DEFINED
 *    Relies on compiler-generated move constructor/assignment.
 *    This is fine since the only member is a vector, but being explicit
 *    would be clearer.
 *
 * 8. clone() vs COPY CONSTRUCTOR
 *    Both exist. clone() returns unique_ptr<Tuple>, copy constructor
 *    returns Tuple. The clone() method is needed because operators work
 *    with unique_ptr<Tuple>, but this is somewhat redundant.
 *
 * 9. print() HARDCODED TO stdout
 *    Same issue as Field::print(). Should take ostream& parameter.
 *
 * 10. NO NULL/MISSING VALUE SUPPORT
 *     All fields must have values. No way to represent SQL NULL.
 *     Would need optional<Field> or a null flag per field.
 *
 * ============================================================================
 */

#include <vector>
#include <memory>
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>

#include "field.h"

namespace buzzdb {

/**
 * @brief A tuple (row) consisting of an ordered sequence of fields.
 *
 * Tuples are the fundamental data unit in query processing. They represent
 * rows flowing through the operator tree.
 */
class Tuple {
public:
    // -------------------------------------------------------------------------
    // PUBLIC DATA MEMBER
    // -------------------------------------------------------------------------
    // CRITIQUE: Should be private. Left public for compatibility with existing
    // code that directly accesses and modifies fields.
    // -------------------------------------------------------------------------

    std::vector<std::unique_ptr<Field>> fields;

    // -------------------------------------------------------------------------
    // CONSTRUCTORS
    // -------------------------------------------------------------------------

    /// Default constructor - creates empty tuple.
    Tuple() = default;

    /// Destructor.
    ~Tuple() = default;

    // Copy constructor (deep copy).
    Tuple(const Tuple& other) {
        fields.reserve(other.fields.size());
        for (const auto& field : other.fields) {
            fields.push_back(field->clone());
        }
    }

    // Copy assignment (deep copy).
    Tuple& operator=(const Tuple& other) {
        if (this != &other) {
            fields.clear();
            fields.reserve(other.fields.size());
            for (const auto& field : other.fields) {
                fields.push_back(field->clone());
            }
        }
        return *this;
    }

    // Move constructor (default is fine).
    Tuple(Tuple&&) = default;

    // Move assignment (default is fine).
    Tuple& operator=(Tuple&&) = default;

    // -------------------------------------------------------------------------
    // FIELD MANIPULATION
    // -------------------------------------------------------------------------

    /// Add a field to the end of the tuple.
    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    /// Get the total size of all field data in bytes.
    /// CRITIQUE: Name is ambiguous - this is data size, not field count.
    /// Consider renaming to getDataSize() or getTotalBytes().
    size_t getSize() const {
        size_t size = 0;
        for (const auto& field : fields) {
            size += field->data_length;
        }
        return size;
    }

    /// Get number of fields.
    /// (Added for clarity since getSize() doesn't return this)
    size_t getFieldCount() const {
        return fields.size();
    }

    // -------------------------------------------------------------------------
    // SERIALIZATION
    // -------------------------------------------------------------------------

    /// Serialize to string format: "field_count field1_serialized field2_serialized ..."
    /// CRITIQUE: Inherits all Field::serialize() problems (text-based, space issues).
    std::string serialize() {
        std::stringstream buffer;
        buffer << fields.size() << ' ';
        for (const auto& field : fields) {
            buffer << field->serialize();
        }
        return buffer.str();
    }

    /// Serialize to output file stream.
    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    /// Deserialize from input stream.
    /// CRITIQUE: Returns nullptr implicitly if field deserialization fails
    /// (Field::deserialize returns nullptr on unknown type).
    static std::unique_ptr<Tuple> deserialize(std::istream& in) {
        auto tuple = std::make_unique<Tuple>();
        size_t fieldCount;
        in >> fieldCount;
        for (size_t i = 0; i < fieldCount; ++i) {
            auto field = Field::deserialize(in);
            // CRITIQUE: No null check - if Field::deserialize returns nullptr,
            // we'll add a null pointer to the tuple.
            tuple->addField(std::move(field));
        }
        return tuple;
    }

    // -------------------------------------------------------------------------
    // UTILITY
    // -------------------------------------------------------------------------

    /// Create a heap-allocated deep copy.
    /// CRITIQUE: Redundant with copy constructor, but needed because operators
    /// work with unique_ptr<Tuple>.
    std::unique_ptr<Tuple> clone() const {
        auto clonedTuple = std::make_unique<Tuple>();
        for (const auto& field : fields) {
            clonedTuple->addField(field->clone());
        }
        return clonedTuple;
    }

    /// Print all fields to stdout, space-separated, with trailing newline.
    /// CRITIQUE: Hardcoded to std::cout. Should take ostream& parameter.
    void print() const {
        for (const auto& field : fields) {
            field->print();
            std::cout << " ";
        }
        std::cout << "\n";
    }
};

}  // namespace buzzdb
