#pragma once

/**
 * @file print_operator.h
 * @brief PrintOperator - outputs tuples to a stream.
 *
 * A utility operator that prints tuples as it passes them through.
 * Typically used at the root of an operator tree for debugging or output.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. CONSUMES INPUT BUT RETURNS EMPTY
 *    getOutput() returns empty vector, so PrintOperator must be a terminal
 *    operator. Can't compose it in the middle of a pipeline.
 *
 * 2. CALLS GETOUTPUT() TWICE PER TUPLE
 *    next() calls input->getOutput() to print, but doesn't store the result.
 *    This works because the tuple is consumed, but violates the expectation
 *    that getOutput() can be called multiple times.
 *
 * 3. OUTPUT FORMAT IS FIXED
 *    Comma-separated values with newline. No CSV escaping, no headers,
 *    no configurable delimiter.
 *
 * ============================================================================
 */

#include <ostream>

#include "execution/operator.h"

namespace buzzdb {

/**
 * @brief Prints tuples from input to an output stream.
 *
 * Each tuple is printed as comma-separated field values, one per line.
 */
class PrintOperator : public UnaryOperator {
private:
    std::ostream& stream_;

public:
    PrintOperator(Operator& input, std::ostream& stream)
        : UnaryOperator(input), stream_(stream) {}

    void open() override {
        input->open();
    }

    bool next() override {
        if (!input->next()) {
            return false;
        }

        // Print the tuple
        auto fields = input->getOutput();
        bool first = true;
        for (const auto& field : fields) {
            if (!first) {
                stream_ << ", ";
            }
            stream_ << field->asString();
            first = false;
        }
        stream_ << '\n';

        return true;
    }

    void close() override {
        input->close();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        // PrintOperator is a sink - no output to pass up
        return {};
    }
};

}  // namespace buzzdb
