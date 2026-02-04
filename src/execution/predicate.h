#pragma once

/**
 * @file predicate.h
 * @brief Predicate classes for filtering tuples in SELECT operations.
 *
 * Predicates are expressions that evaluate to true/false given a tuple.
 * Used by SelectOperator to filter rows.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. TUPLE TYPE IS AWKWARD
 *    Predicates operate on vector<unique_ptr<Field>>, which is the output
 *    format of operators. This ties predicates to the operator interface.
 *    Would be cleaner to operate on Tuple& or a TupleView.
 *
 * 2. OPERAND OWNERSHIP IS CONFUSING
 *    SimplePredicate::Operand owns a Field via unique_ptr for DIRECT values,
 *    but uses an index for INDIRECT. The Operand struct must be movable
 *    but not copyable, which makes predicate construction awkward.
 *
 * 3. TYPE CHECKING AT RUNTIME
 *    Comparing fields of different types logs error and returns false.
 *    Should probably throw an exception for type mismatch.
 *
 * 4. NO EXPRESSION TREE
 *    This is a simplified two-level model: SimplePredicate for leaves,
 *    ComplexPredicate for AND/OR combinations. A proper expression tree
 *    would support nested boolean expressions and other operators.
 *
 * 5. STRING COMPARISON SEMANTICS
 *    String comparisons use lexicographic ordering, which may not match
 *    user expectations for case sensitivity or locale-aware sorting.
 *
 * ============================================================================
 */

#include <vector>
#include <memory>
#include <iostream>

#include "storage/field.h"

namespace buzzdb {

/// Alias for tuple representation used in operator output
using TupleFields = std::vector<std::unique_ptr<Field>>;

/**
 * @brief Abstract interface for predicates.
 *
 * A predicate evaluates a condition on a tuple and returns true/false.
 */
class IPredicate {
public:
    virtual ~IPredicate() = default;

    /**
     * @brief Evaluate the predicate on a tuple.
     * @param tuple_fields The fields of the tuple to evaluate.
     * @return true if the tuple satisfies the predicate.
     */
    virtual bool check(const TupleFields& tuple_fields) const = 0;
};

/**
 * @brief A simple comparison predicate (e.g., column > value).
 *
 * Compares two operands using a comparison operator.
 * Operands can be direct values or references to tuple columns.
 */
class SimplePredicate : public IPredicate {
public:
    /// Type of operand
    enum class OperandType { DIRECT, INDIRECT };

    /// Comparison operators
    enum class ComparisonOperator { EQ, NE, GT, GE, LT, LE };

    /**
     * @brief Represents one side of a comparison.
     *
     * Can be either:
     * - DIRECT: A constant value (e.g., 42)
     * - INDIRECT: A column reference (e.g., column 2 of the tuple)
     */
    struct Operand {
        std::unique_ptr<Field> direct_value;  // For DIRECT type
        size_t index = 0;                      // For INDIRECT type
        OperandType type;

        /// Construct a direct (constant) operand
        explicit Operand(std::unique_ptr<Field> value)
            : direct_value(std::move(value)), type(OperandType::DIRECT) {}

        /// Construct an indirect (column reference) operand
        explicit Operand(size_t column_index)
            : index(column_index), type(OperandType::INDIRECT) {}

        // Move-only
        Operand(Operand&&) = default;
        Operand& operator=(Operand&&) = default;
        Operand(const Operand&) = delete;
        Operand& operator=(const Operand&) = delete;
    };

private:
    Operand left_operand_;
    Operand right_operand_;
    ComparisonOperator comparison_op_;

public:
    SimplePredicate(Operand left, Operand right, ComparisonOperator op)
        : left_operand_(std::move(left))
        , right_operand_(std::move(right))
        , comparison_op_(op) {}

    bool check(const TupleFields& tuple_fields) const override {
        // Resolve operands to field pointers
        const Field* left_field = resolveOperand(left_operand_, tuple_fields);
        const Field* right_field = resolveOperand(right_operand_, tuple_fields);

        if (!left_field || !right_field) {
            std::cerr << "Error: Invalid field reference in predicate\n";
            return false;
        }

        if (left_field->getType() != right_field->getType()) {
            std::cerr << "Error: Type mismatch in predicate comparison\n";
            return false;
        }

        // Compare based on field type
        switch (left_field->getType()) {
            case FieldType::INT:
                return compare(left_field->asInt(), right_field->asInt());
            case FieldType::FLOAT:
                return compare(left_field->asFloat(), right_field->asFloat());
            case FieldType::STRING:
                return compare(left_field->asString(), right_field->asString());
            default:
                std::cerr << "Error: Unknown field type\n";
                return false;
        }
    }

private:
    const Field* resolveOperand(const Operand& op, const TupleFields& tuple) const {
        if (op.type == OperandType::DIRECT) {
            return op.direct_value.get();
        } else {
            if (op.index >= tuple.size()) {
                return nullptr;
            }
            return tuple[op.index].get();
        }
    }

    template<typename T>
    bool compare(const T& left, const T& right) const {
        switch (comparison_op_) {
            case ComparisonOperator::EQ: return left == right;
            case ComparisonOperator::NE: return left != right;
            case ComparisonOperator::GT: return left > right;
            case ComparisonOperator::GE: return left >= right;
            case ComparisonOperator::LT: return left < right;
            case ComparisonOperator::LE: return left <= right;
            default: return false;
        }
    }
};

/**
 * @brief A compound predicate combining multiple predicates with AND/OR.
 *
 * Allows building expressions like: (A > 10) AND (B < 20) OR (C = 'foo')
 */
class ComplexPredicate : public IPredicate {
public:
    enum class LogicOperator { AND, OR };

private:
    std::vector<std::unique_ptr<IPredicate>> predicates_;
    LogicOperator logic_op_;

public:
    explicit ComplexPredicate(LogicOperator op) : logic_op_(op) {}

    void addPredicate(std::unique_ptr<IPredicate> predicate) {
        predicates_.push_back(std::move(predicate));
    }

    bool check(const TupleFields& tuple_fields) const override {
        if (predicates_.empty()) {
            return true;  // Empty predicate is always true
        }

        if (logic_op_ == LogicOperator::AND) {
            for (const auto& pred : predicates_) {
                if (!pred->check(tuple_fields)) {
                    return false;  // Short-circuit: one false makes AND false
                }
            }
            return true;
        } else {  // OR
            for (const auto& pred : predicates_) {
                if (pred->check(tuple_fields)) {
                    return true;  // Short-circuit: one true makes OR true
                }
            }
            return false;
        }
    }
};

}  // namespace buzzdb
