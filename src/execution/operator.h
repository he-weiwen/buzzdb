#pragma once

/**
 * @file operator.h
 * @brief Base classes for the iterator-based query execution model.
 *
 * Query execution uses the Volcano/iterator model where each operator:
 * - open(): Initialize state
 * - next(): Produce next tuple (returns false when exhausted)
 * - close(): Clean up resources
 * - getOutput(): Get the current tuple's fields
 *
 * Operators form a tree where data flows from leaves (scans) up to the root.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. GETOUTPUT RETURNS OWNED POINTERS
 *    getOutput() returns vector<unique_ptr<Field>>, transferring ownership.
 *    This means:
 *    - Each call to getOutput() invalidates the previous result
 *    - Caller must move fields out or clone them
 *    - No way to "peek" at output without consuming it
 *    A better design might return const reference to internal storage,
 *    or use a Tuple& out-parameter.
 *
 * 2. NO ERROR HANDLING
 *    next() returns bool but can't signal errors. Operators use cerr and
 *    return false on error, conflating "no more tuples" with "error occurred".
 *    Should throw exceptions or return std::expected/variant.
 *
 * 3. OPEN/CLOSE NOT RAII
 *    Resource management relies on explicit open()/close() calls.
 *    If caller forgets close(), resources may leak. Could use RAII wrapper
 *    or make destructor call close().
 *
 * 4. NO SCHEMA INFORMATION
 *    Operators don't expose output schema (column types, names).
 *    This makes debugging and optimization harder.
 *
 * 5. RAW POINTER TO INPUT
 *    UnaryOperator/BinaryOperator store raw Operator* to inputs.
 *    This is intentional (avoids ownership complexity) but fragile.
 *    Caller must ensure inputs outlive the operator.
 *
 * 6. GETOUTPUT CAN BE CALLED MULTIPLE TIMES
 *    Some operators allow multiple getOutput() calls per next(), others don't.
 *    The contract is unclear and varies by implementation.
 *
 * ============================================================================
 */

#include <vector>
#include <memory>

#include "storage/field.h"

namespace buzzdb {

/**
 * @brief Abstract base class for all query operators.
 *
 * Implements the iterator/Volcano model for query execution.
 * Operators are composed into a tree, with data flowing upward.
 */
class Operator {
public:
    virtual ~Operator() = default;

    /**
     * @brief Initialize the operator for iteration.
     *
     * Must be called before first next() call.
     * May perform expensive setup (e.g., building hash tables).
     */
    virtual void open() = 0;

    /**
     * @brief Advance to the next tuple.
     * @return true if a new tuple is available, false if exhausted.
     *
     * After returning true, getOutput() returns the new tuple.
     * After returning false, getOutput() behavior is undefined.
     */
    virtual bool next() = 0;

    /**
     * @brief Clean up resources.
     *
     * Should be called when done iterating, even if next() returned false.
     */
    virtual void close() = 0;

    /**
     * @brief Get the current tuple's fields.
     * @return Vector of field pointers for the current tuple.
     *
     * CRITIQUE: Returns owned pointers - transfers ownership to caller.
     * Behavior after next() returns false is undefined.
     */
    virtual std::vector<std::unique_ptr<Field>> getOutput() = 0;

protected:
    Operator() = default;

    // Prevent copying (operators have complex internal state)
    Operator(const Operator&) = delete;
    Operator& operator=(const Operator&) = delete;
};

/**
 * @brief Base class for operators with a single input.
 *
 * Examples: Select, Project, Sort, Print
 */
class UnaryOperator : public Operator {
protected:
    /// Pointer to input operator (not owned)
    /// CRITIQUE: Raw pointer - caller must ensure input outlives this operator
    Operator* input;

public:
    explicit UnaryOperator(Operator& input) : input(&input) {}
    ~UnaryOperator() override = default;
};

/**
 * @brief Base class for operators with two inputs.
 *
 * Examples: Join, Union, Intersect
 */
class BinaryOperator : public Operator {
protected:
    /// Pointers to input operators (not owned)
    Operator* input_left;
    Operator* input_right;

public:
    explicit BinaryOperator(Operator& left, Operator& right)
        : input_left(&left), input_right(&right) {}
    ~BinaryOperator() override = default;
};

}  // namespace buzzdb
