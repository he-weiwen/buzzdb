#pragma once

/**
 * @file field_variant.h
 * @brief Variant-based Field implementation (proposed replacement for field.h)
 *
 * This is a modernized implementation of the Field class using std::variant.
 * It addresses the design issues in the original implementation while maintaining
 * interface compatibility where possible.
 *
 * ============================================================================
 * SEMANTIC CHANGES FROM ORIGINAL (field.h)
 * ============================================================================
 *
 * 1. TYPE ACCESSOR BEHAVIOR
 *    Original: asInt()/asFloat() on wrong type → undefined behavior (silent)
 *    New:      asInt()/asFloat() on wrong type → throws std::bad_variant_access
 *
 *    Migration: Use getType() check before calling, or use safe accessors.
 *
 * 2. CROSS-TYPE COMPARISON
 *    Original: Field(42) == Field(42.0f) → false (different types)
 *              Field(42) <  Field(42.0f) → false (prints to stderr, returns false)
 *    New:      Field(42) == Field(42.0f) → false (different types)
 *              Field(42) <  Field(42.0f) → true (ordered by type index: INT < FLOAT < STRING)
 *
 *    Rationale: Variant's natural ordering. This is actually more consistent -
 *    provides total ordering, which is useful for sorting mixed-type data.
 *
 * 3. COMPARISON SIDE EFFECTS
 *    Original: operator!=, <, >, <=, >= print to stderr on type mismatch
 *    New:      No side effects. Comparisons are pure functions.
 *
 * 4. operator+= BEHAVIOR
 *    Original: Silent no-op on type mismatch
 *    New:      Throws std::bad_variant_access on type mismatch
 *
 *    Migration: Check type before using +=, or use new addTo() method.
 *
 * 5. REMOVED PUBLIC MEMBERS
 *    Original: type, data, data_length are public
 *    New:      All members private. Use getType(), getData*() accessors.
 *
 *    Migration: Replace direct member access with accessor calls.
 *
 * 6. SERIALIZATION FORMAT
 *    Unchanged. Same text format: "type_int data_length value "
 *    Existing serialized data remains compatible.
 *
 * 7. MEMORY LAYOUT
 *    Original: Always heap-allocates data buffer
 *    New:      int/float stored inline, string uses SSO (small string optimization)
 *
 *    Result: Better performance for int/float fields.
 *
 * ============================================================================
 * NEW FEATURES
 * ============================================================================
 *
 * 1. tryAsInt(), tryAsFloat(), tryAsString() - return std::optional, no throw
 * 2. visit() - apply visitor to underlying value
 * 3. print(std::ostream&) - output to any stream
 * 4. hash() - for use in hash containers
 *
 * ============================================================================
 */

#include <variant>
#include <string>
#include <optional>
#include <memory>
#include <sstream>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <functional>
#include <cmath>

#include "common/types.h"

namespace buzzdb {

/**
 * @brief A type-safe field using std::variant.
 *
 * Internally stores one of: int, float, std::string.
 * The variant index corresponds to FieldType enum values.
 */
class Field {
public:
    // Variant type - order MUST match FieldType enum: INT=0, FLOAT=1, STRING=2
    using ValueType = std::variant<int, float, std::string>;

private:
    ValueType value_;

public:
    // =========================================================================
    // CONSTRUCTORS
    // =========================================================================

    Field(int i) : value_(i) {}
    Field(float f) : value_(f) {}
    Field(const std::string& s) : value_(s) {}
    Field(std::string&& s) : value_(std::move(s)) {}

    // Convenience: construct from C-string (avoids ambiguity)
    Field(const char* s) : value_(std::string(s)) {}

    // Default/copy/move - all defaulted (variant handles everything)
    Field() : value_(0) {}  // Default to INT 0
    Field(const Field&) = default;
    Field(Field&&) = default;
    Field& operator=(const Field&) = default;
    Field& operator=(Field&&) = default;
    ~Field() = default;

    // =========================================================================
    // TYPE QUERY
    // =========================================================================

    /// Get the field type.
    FieldType getType() const {
        return static_cast<FieldType>(value_.index());
    }

    /// Check if field holds a specific type.
    bool isInt() const { return std::holds_alternative<int>(value_); }
    bool isFloat() const { return std::holds_alternative<float>(value_); }
    bool isString() const { return std::holds_alternative<std::string>(value_); }

    // =========================================================================
    // VALUE ACCESSORS (throwing)
    // =========================================================================

    /// Get value as int. Throws std::bad_variant_access if not INT.
    /// SEMANTIC CHANGE: Original returned garbage, this throws.
    int asInt() const { return std::get<int>(value_); }

    /// Get value as float. Throws std::bad_variant_access if not FLOAT.
    float asFloat() const { return std::get<float>(value_); }

    /// Get raw string value. Throws std::bad_variant_access if not STRING.
    const std::string& asStringRaw() const { return std::get<std::string>(value_); }

    /// Get value as string (converts int/float to string representation).
    /// This is the only accessor that works for all types.
    std::string asString() const {
        return std::visit([](const auto& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
                return v;
            } else {
                return std::to_string(v);
            }
        }, value_);
    }

    // =========================================================================
    // VALUE ACCESSORS (non-throwing, return optional)
    // NEW FEATURE: Safe accessors that don't throw.
    // =========================================================================

    std::optional<int> tryAsInt() const {
        if (auto* p = std::get_if<int>(&value_)) return *p;
        return std::nullopt;
    }

    std::optional<float> tryAsFloat() const {
        if (auto* p = std::get_if<float>(&value_)) return *p;
        return std::nullopt;
    }

    std::optional<std::string> tryAsString() const {
        if (auto* p = std::get_if<std::string>(&value_)) return *p;
        return std::nullopt;
    }

    // =========================================================================
    // VISITOR SUPPORT
    // NEW FEATURE: Apply a visitor to the underlying value.
    // =========================================================================

    template<typename Visitor>
    decltype(auto) visit(Visitor&& vis) const {
        return std::visit(std::forward<Visitor>(vis), value_);
    }

    template<typename Visitor>
    decltype(auto) visit(Visitor&& vis) {
        return std::visit(std::forward<Visitor>(vis), value_);
    }

    // =========================================================================
    // ARITHMETIC OPERATORS
    // =========================================================================

    /// Add integer to INT field. Throws if not INT.
    /// SEMANTIC CHANGE: Original silently ignored type mismatch.
    Field& operator+=(int val) {
        std::get<int>(value_) += val;  // Throws if not int
        return *this;
    }

    /// Add float to FLOAT field. Throws if not FLOAT.
    Field& operator+=(float val) {
        std::get<float>(value_) += val;  // Throws if not float
        return *this;
    }

    /// Safe arithmetic: returns true if successful, false if type mismatch.
    /// NEW FEATURE: Non-throwing alternative to operator+=.
    bool tryAdd(int val) {
        if (auto* p = std::get_if<int>(&value_)) {
            *p += val;
            return true;
        }
        return false;
    }

    bool tryAdd(float val) {
        if (auto* p = std::get_if<float>(&value_)) {
            *p += val;
            return true;
        }
        return false;
    }

    // =========================================================================
    // SERIALIZATION
    // Format preserved for compatibility: "type_int data_length value "
    // =========================================================================

    std::string serialize() const {
        std::stringstream buffer;
        buffer << static_cast<int>(getType()) << ' ';

        std::visit([&buffer](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int>) {
                buffer << sizeof(int) << ' ' << v << ' ';
            } else if constexpr (std::is_same_v<T, float>) {
                buffer << sizeof(float) << ' ' << v << ' ';
            } else {  // string
                buffer << (v.size() + 1) << ' ' << v << ' ';
            }
        }, value_);

        return buffer.str();
    }

    void serialize(std::ofstream& out) const {
        out << serialize();
    }

    /// Deserialize from input stream.
    /// NOTE: Still has the spaces-in-strings limitation (format unchanged).
    static std::unique_ptr<Field> deserialize(std::istream& in) {
        int type_int;
        in >> type_int;

        size_t length;
        in >> length;  // Read but not used (reconstructed from value)

        switch (static_cast<FieldType>(type_int)) {
            case FieldType::INT: {
                int val;
                in >> val;
                return std::make_unique<Field>(val);
            }
            case FieldType::FLOAT: {
                float val;
                in >> val;
                return std::make_unique<Field>(val);
            }
            case FieldType::STRING: {
                std::string val;
                in >> val;  // Still breaks on spaces - format limitation
                return std::make_unique<Field>(val);
            }
            default:
                return nullptr;
        }
    }

    // =========================================================================
    // UTILITY
    // =========================================================================

    std::unique_ptr<Field> clone() const {
        return std::make_unique<Field>(*this);
    }

    /// Print to specified stream.
    /// NEW FEATURE: Parameterized output stream.
    void print(std::ostream& os) const {
        std::visit([&os](const auto& v) { os << v; }, value_);
    }

    /// Print to stdout (backward compatible).
    void print() const { print(std::cout); }

    /// Hash function for use in containers.
    /// NEW FEATURE: Enables use in unordered_set/map.
    size_t hash() const {
        return std::visit([](const auto& v) -> size_t {
            return std::hash<std::decay_t<decltype(v)>>{}(v);
        }, value_);
    }

    // =========================================================================
    // COMPARISON OPERATORS
    // SEMANTIC CHANGE: No stderr output. Cross-type comparison uses type index.
    // =========================================================================

    /// Equality: same type AND same value.
    bool operator==(const Field& other) const {
        return value_ == other.value_;
    }

    bool operator!=(const Field& other) const {
        return value_ != other.value_;
    }

    /// Ordering: first by type index, then by value within same type.
    /// SEMANTIC CHANGE: Cross-type comparison now has defined behavior.
    /// INT < FLOAT < STRING (by type index), then lexicographic within type.
    bool operator<(const Field& other) const {
        return value_ < other.value_;
    }

    bool operator>(const Field& other) const {
        return value_ > other.value_;
    }

    bool operator<=(const Field& other) const {
        return value_ <= other.value_;
    }

    bool operator>=(const Field& other) const {
        return value_ >= other.value_;
    }

    // =========================================================================
    // SAME-TYPE COMPARISON (for cases where you want to compare values only)
    // NEW FEATURE: Explicit same-type comparison with clear semantics.
    // =========================================================================

    /// Compare values only if same type. Returns nullopt if types differ.
    std::optional<bool> equalsSameType(const Field& other) const {
        if (getType() != other.getType()) return std::nullopt;
        return value_ == other.value_;
    }

    std::optional<bool> lessThanSameType(const Field& other) const {
        if (getType() != other.getType()) return std::nullopt;
        return value_ < other.value_;
    }

    // =========================================================================
    // COMPATIBILITY ACCESSORS
    // These provide access patterns similar to the old public members.
    // Provided for migration; prefer the cleaner accessors above.
    // =========================================================================

    /// Get data length (for serialization compatibility).
    /// Returns the same values as the old data_length member.
    size_t getDataLength() const {
        return std::visit([](const auto& v) -> size_t {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) {
                return v.size() + 1;  // Include null terminator
            } else {
                return sizeof(T);
            }
        }, value_);
    }
};

}  // namespace buzzdb

// =============================================================================
// STD::HASH SPECIALIZATION
// Enables Field to be used as key in unordered_map/unordered_set.
// =============================================================================

namespace std {
template<>
struct hash<buzzdb::Field> {
    size_t operator()(const buzzdb::Field& f) const {
        return f.hash();
    }
};
}  // namespace std
