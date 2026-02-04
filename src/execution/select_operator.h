#pragma once

/**
 * @file select_operator.h
 * @brief SelectOperator - filters tuples based on a predicate.
 *
 * Corresponds to the WHERE clause in SQL.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. CLONES FIELDS IN NEXT()
 *    When a tuple passes the predicate, fields are cloned into currentOutput.
 *    This is necessary because input->getOutput() transfers ownership, but
 *    it means extra allocations for every qualifying tuple.
 *
 * 2. GETOUTPUT() CLONES AGAIN
 *    getOutput() returns clones of currentOutput, so each qualifying tuple
 *    is cloned twice. This is wasteful.
 *
 * 3. NO PREDICATE PUSHDOWN
 *    Predicate is evaluated after reading the full tuple. If the underlying
 *    storage supported predicate pushdown, we could skip reading non-matching
 *    tuples entirely.
 *
 * ============================================================================
 */

#include "execution/operator.h"
#include "execution/predicate.h"

namespace buzzdb {

/**
 * @brief Filters tuples from input based on a predicate.
 *
 * Only tuples where predicate->check() returns true are passed through.
 */
class SelectOperator : public UnaryOperator {
private:
    std::unique_ptr<IPredicate> predicate_;
    bool has_output_ = false;
    std::vector<std::unique_ptr<Field>> current_output_;

public:
    SelectOperator(Operator& input, std::unique_ptr<IPredicate> predicate)
        : UnaryOperator(input), predicate_(std::move(predicate)) {}

    void open() override {
        input->open();
        has_output_ = false;
        current_output_.clear();
    }

    bool next() override {
        while (input->next()) {
            auto output = input->getOutput();

            if (predicate_->check(output)) {
                // Clone fields into current_output_
                // CRITIQUE: This clone is necessary but wasteful
                current_output_.clear();
                for (const auto& field : output) {
                    current_output_.push_back(field->clone());
                }
                has_output_ = true;
                return true;
            }
        }

        has_output_ = false;
        current_output_.clear();
        return false;
    }

    void close() override {
        input->close();
        current_output_.clear();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (!has_output_) {
            return {};
        }

        // CRITIQUE: Another clone - fields are cloned twice per tuple
        std::vector<std::unique_ptr<Field>> result;
        for (const auto& field : current_output_) {
            result.push_back(field->clone());
        }
        return result;
    }
};

}  // namespace buzzdb
