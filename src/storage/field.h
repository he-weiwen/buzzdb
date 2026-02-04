#pragma once

/**
 * @file field.h
 * @brief Field class - the atomic unit of data in BuzzDB.
 *
 * A Field represents a single value in a tuple. It can hold an integer,
 * float, or string value. This is essentially a discriminated union
 * (tagged variant) implemented manually.
 *
 * ============================================================================
 * DESIGN CRITIQUE (preserving original design, documenting concerns)
 * ============================================================================
 *
 * 1. MANUAL VARIANT IMPLEMENTATION
 *    The Field class manually implements what std::variant<int, float, std::string>
 *    would provide. This is educational but has drawbacks:
 *    - More code to maintain
 *    - Easy to make mistakes with reinterpret_cast
 *    - No compile-time type safety when accessing values
 *
 *    Consider: std::variant would eliminate the raw char[] buffer and type punning.
 *
 * 2. RAW MEMORY MANAGEMENT
 *    Uses std::unique_ptr<char[]> as a type-erased buffer with reinterpret_cast
 *    to access typed values. This works but is fragile:
 *    - Alignment is not guaranteed (char[] has alignment 1)
 *    - For int/float this happens to work on most platforms but is technically UB
 *    - std::aligned_storage or alignas would be more correct
 *
 * 3. SERIALIZATION FORMAT
 *    Uses text-based serialization with space separators:
 *      "0 4 42 " for INT with value 42
 *    Issues:
 *    - Inefficient (text vs binary)
 *    - Strings with spaces will break parsing (no escaping/quoting)
 *    - data_length is serialized but then ignored during deserialization
 *
 * 4. COMPARISON OPERATORS
 *    - operator== throws on unknown type, but operator!= returns false (inconsistent)
 *    - operator!= prints to cerr on type mismatch (side effect in comparison!)
 *    - Comparing different types: == returns false, but != also returns false
 *      This violates: (a != b) should equal !(a == b)
 *    - Float comparison uses exact equality (problematic for floating point)
 *
 * 5. asString() BEHAVIOR
 *    For INT/FLOAT types, asString() converts to string representation.
 *    For STRING type, it returns the stored string.
 *    This asymmetry can be confusing - it's both an accessor and a converter.
 *
 * 6. operator+= SILENTLY IGNORES TYPE MISMATCH
 *    Field(42) += 1.5f  // Does nothing, no error
 *    This silent failure could hide bugs.
 *
 * 7. COPY CONSTRUCTOR vs COPY ASSIGNMENT INCONSISTENCY
 *    Copy constructor uses: data(new char[data_length])
 *    Copy assignment uses:  data = std::make_unique<char[]>(data_length)
 *    Both work, but the inconsistency suggests hasty implementation.
 *
 * 8. PUBLIC DATA MEMBERS
 *    type, data_length, data are all public. This breaks encapsulation.
 *    External code can directly modify these, creating invalid states.
 *
 * ============================================================================
 */

#include <memory>
#include <string>
#include <sstream>
#include <fstream>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include "common/types.h"

namespace buzzdb {

/**
 * @brief A dynamically-typed field that can hold INT, FLOAT, or STRING values.
 *
 * Fields are the atomic units of data in tuples. Each field has a type tag
 * and stores its value in a raw byte buffer.
 */
class Field {
public:
    // -------------------------------------------------------------------------
    // PUBLIC DATA MEMBERS
    // -------------------------------------------------------------------------
    // CRITIQUE: These should be private with accessors. Left public for
    // compatibility with existing code that directly accesses them.
    // -------------------------------------------------------------------------

    FieldType type;
    size_t data_length;
    std::unique_ptr<char[]> data;

public:
    // -------------------------------------------------------------------------
    // CONSTRUCTORS
    // -------------------------------------------------------------------------

    /// Construct an INT field.
    Field(int i) : type(FieldType::INT) {
        data_length = sizeof(int);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &i, data_length);
    }

    /// Construct a FLOAT field.
    Field(float f) : type(FieldType::FLOAT) {
        data_length = sizeof(float);
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), &f, data_length);
    }

    /// Construct a STRING field.
    Field(const std::string& s) : type(FieldType::STRING) {
        data_length = s.size() + 1;  // include null-terminator
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), s.c_str(), data_length);
    }

    // -------------------------------------------------------------------------
    // COPY/MOVE OPERATIONS
    // -------------------------------------------------------------------------

    /// Copy constructor.
    /// CRITIQUE: Uses raw new instead of make_unique (inconsistent with assignment).
    Field(const Field& other)
        : type(other.type)
        , data_length(other.data_length)
        , data(new char[data_length])
    {
        std::memcpy(data.get(), other.data.get(), data_length);
    }

    /// Move constructor.
    Field(Field&& other) noexcept
        : type(other.type)
        , data_length(other.data_length)
        , data(std::move(other.data))
    {
        // CRITIQUE: Does not reset other.type or other.data_length.
        // The moved-from object is in a valid but unspecified state.
    }

    /// Copy assignment operator.
    Field& operator=(const Field& other) {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        data = std::make_unique<char[]>(data_length);
        std::memcpy(data.get(), other.data.get(), data_length);
        return *this;
    }

    /// Move assignment operator (implicitly deleted, adding for completeness).
    Field& operator=(Field&& other) noexcept {
        if (&other == this) {
            return *this;
        }
        type = other.type;
        data_length = other.data_length;
        data = std::move(other.data);
        return *this;
    }

    // -------------------------------------------------------------------------
    // ACCESSORS
    // -------------------------------------------------------------------------

    FieldType getType() const { return type; }

    /// Get value as int.
    /// CRITIQUE: No type checking - calling on FLOAT/STRING is undefined behavior.
    int asInt() const {
        return *reinterpret_cast<int*>(data.get());
    }

    /// Get value as float.
    /// CRITIQUE: No type checking - calling on INT/STRING is undefined behavior.
    float asFloat() const {
        return *reinterpret_cast<float*>(data.get());
    }

    /// Get value as string.
    /// CRITIQUE: Inconsistent behavior - converts INT/FLOAT to string representation,
    /// but returns stored value for STRING. This is both accessor and converter.
    std::string asString() const {
        switch (type) {
            case FieldType::INT:
                return std::to_string(asInt());
            case FieldType::FLOAT:
                return std::to_string(asFloat());
            case FieldType::STRING:
                return std::string(data.get());
            default:
                return "";
        }
    }

    // -------------------------------------------------------------------------
    // ARITHMETIC OPERATORS
    // -------------------------------------------------------------------------

    /// Add integer to field (only works for INT fields).
    /// CRITIQUE: Silently does nothing for non-INT fields. Should throw or return error.
    Field& operator+=(const int val) {
        if (type == FieldType::INT) {
            *reinterpret_cast<int*>(data.get()) += val;
        }
        return *this;
    }

    /// Add float to field (only works for FLOAT fields).
    /// CRITIQUE: Silently does nothing for non-FLOAT fields.
    Field& operator+=(const float val) {
        if (type == FieldType::FLOAT) {
            *reinterpret_cast<float*>(data.get()) += val;
        }
        return *this;
    }

    // -------------------------------------------------------------------------
    // SERIALIZATION
    // -------------------------------------------------------------------------

    /// Serialize to string format: "type data_length value "
    /// CRITIQUE: Text-based format is inefficient. Strings with spaces will break.
    std::string serialize() {
        std::stringstream buffer;
        // CRITIQUE: Casting enum to int for serialization. Works but fragile if enum changes.
        buffer << static_cast<int>(type) << ' ' << data_length << ' ';
        if (type == FieldType::STRING) {
            buffer << data.get() << ' ';
        } else if (type == FieldType::INT) {
            buffer << *reinterpret_cast<int*>(data.get()) << ' ';
        } else if (type == FieldType::FLOAT) {
            buffer << *reinterpret_cast<float*>(data.get()) << ' ';
        }
        return buffer.str();
    }

    /// Serialize to output file stream.
    void serialize(std::ofstream& out) {
        std::string serializedData = this->serialize();
        out << serializedData;
    }

    /// Deserialize from input stream.
    /// CRITIQUE: Reads data_length but ignores it (reconstructs from value instead).
    /// CRITIQUE: Returns nullptr on unknown type instead of throwing.
    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type_int;
        in >> type_int;
        size_t length;
        in >> length;  // CRITIQUE: This value is read but never used!

        if (type_int == static_cast<int>(FieldType::STRING)) {
            std::string val;
            in >> val;  // CRITIQUE: Breaks on strings containing spaces
            return std::make_unique<Field>(val);
        } else if (type_int == static_cast<int>(FieldType::INT)) {
            int val;
            in >> val;
            return std::make_unique<Field>(val);
        } else if (type_int == static_cast<int>(FieldType::FLOAT)) {
            float val;
            in >> val;
            return std::make_unique<Field>(val);
        }
        return nullptr;
    }

    // -------------------------------------------------------------------------
    // UTILITY
    // -------------------------------------------------------------------------

    /// Create a heap-allocated copy.
    std::unique_ptr<Field> clone() const {
        return std::make_unique<Field>(*this);
    }

    /// Print to stdout.
    /// CRITIQUE: Hardcoded to std::cout. Should take ostream& parameter.
    void print() const {
        switch (type) {
            case FieldType::INT:
                std::cout << asInt();
                break;
            case FieldType::FLOAT:
                std::cout << asFloat();
                break;
            case FieldType::STRING:
                std::cout << asString();
                break;
        }
    }
};

// =============================================================================
// COMPARISON OPERATORS (free functions)
// =============================================================================

/// Equality comparison.
/// CRITIQUE: Throws on default case, but other operators return false. Inconsistent.
inline bool operator==(const Field& lhs, const Field& rhs) {
    if (lhs.type != rhs.type) return false;

    switch (lhs.type) {
        case FieldType::INT:
            return *reinterpret_cast<const int*>(lhs.data.get()) ==
                   *reinterpret_cast<const int*>(rhs.data.get());
        case FieldType::FLOAT:
            // CRITIQUE: Exact float comparison is problematic
            return *reinterpret_cast<const float*>(lhs.data.get()) ==
                   *reinterpret_cast<const float*>(rhs.data.get());
        case FieldType::STRING:
            return std::string(lhs.data.get(), lhs.data_length - 1) ==
                   std::string(rhs.data.get(), rhs.data_length - 1);
        default:
            throw std::runtime_error("Unsupported field type for comparison.");
    }
}

/// Inequality comparison.
/// CRITIQUE: Prints to cerr on type mismatch (side effect!).
/// CRITIQUE: Returns false for type mismatch, but so does ==. Violates !(a==b) == (a!=b).
inline bool operator!=(const Field& lhs, const Field& rhs) {
    if (lhs.getType() != rhs.getType()) {
        // CRITIQUE: This cerr output is a side effect in a comparison operator!
        std::cerr << "invalid comparison: incompatible type\n";
        return false;
    }
    switch (lhs.getType()) {
        case FieldType::FLOAT:
            return lhs.asFloat() != rhs.asFloat();
        case FieldType::INT:
            return lhs.asInt() != rhs.asInt();
        case FieldType::STRING:
            return lhs.asString() != rhs.asString();
        default:
            return false;
    }
}

/// Less-than comparison.
/// CRITIQUE: Same issues as operator!=.
inline bool operator<(const Field& lhs, const Field& rhs) {
    if (lhs.getType() != rhs.getType()) {
        std::cerr << "invalid comparison: incompatible type\n";
        return false;
    }
    switch (lhs.getType()) {
        case FieldType::FLOAT:
            return lhs.asFloat() < rhs.asFloat();
        case FieldType::INT:
            return lhs.asInt() < rhs.asInt();
        case FieldType::STRING:
            return lhs.asString() < rhs.asString();
        default:
            return false;
    }
}

/// Greater-than comparison.
inline bool operator>(const Field& lhs, const Field& rhs) {
    if (lhs.getType() != rhs.getType()) {
        std::cerr << "invalid comparison: incompatible type\n";
        return false;
    }
    switch (lhs.getType()) {
        case FieldType::FLOAT:
            return lhs.asFloat() > rhs.asFloat();
        case FieldType::INT:
            return lhs.asInt() > rhs.asInt();
        case FieldType::STRING:
            return lhs.asString() > rhs.asString();
        default:
            return false;
    }
}

/// Less-than-or-equal comparison.
inline bool operator<=(const Field& lhs, const Field& rhs) {
    if (lhs.getType() != rhs.getType()) {
        std::cerr << "invalid comparison: incompatible type\n";
        return false;
    }
    switch (lhs.getType()) {
        case FieldType::FLOAT:
            return lhs.asFloat() <= rhs.asFloat();
        case FieldType::INT:
            return lhs.asInt() <= rhs.asInt();
        case FieldType::STRING:
            return lhs.asString() <= rhs.asString();
        default:
            return false;
    }
}

/// Greater-than-or-equal comparison.
inline bool operator>=(const Field& lhs, const Field& rhs) {
    if (lhs.getType() != rhs.getType()) {
        std::cerr << "invalid comparison: incompatible type\n";
        return false;
    }
    switch (lhs.getType()) {
        case FieldType::FLOAT:
            return lhs.asFloat() >= rhs.asFloat();
        case FieldType::INT:
            return lhs.asInt() >= rhs.asInt();
        case FieldType::STRING:
            return lhs.asString() >= rhs.asString();
        default:
            return false;
    }
}

}  // namespace buzzdb
