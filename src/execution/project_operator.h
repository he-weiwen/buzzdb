#pragma once

/**
 * @file project_operator.h
 * @brief ProjectOperator - selects specific columns from tuples.
 *
 * Corresponds to the SELECT column-list in SQL.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. NO BOUNDS CHECKING
 *    If attr_indexes contains an index >= tuple size, behavior is undefined.
 *    Should validate indices.
 *
 * 2. GETOUTPUT() MOVES - NON-IDEMPOTENT
 *    Returns std::move(out_), so calling getOutput() twice gives empty vector
 *    on second call. This is the pattern throughout, but fragile.
 *
 * 3. NO EXPRESSION SUPPORT
 *    Can only select existing columns. Can't compute new columns
 *    (e.g., SELECT a + b). Would need expression evaluation.
 *
 * ============================================================================
 */

#include "execution/operator.h"

namespace buzzdb {

/**
 * @brief Projects (selects) specific columns from input tuples.
 *
 * Output tuples contain only the fields at the specified indices.
 */
class ProjectOperator : public UnaryOperator {
private:
    std::vector<size_t> attr_indices_;
    std::vector<std::unique_ptr<Field>> out_;

public:
    ProjectOperator(Operator& input, std::vector<size_t> attr_indices)
        : UnaryOperator(input), attr_indices_(std::move(attr_indices)) {}

    void open() override {
        input->open();
    }

    bool next() override {
        if (!input->next()) {
            return false;
        }

        auto fields = input->getOutput();

        out_.clear();
        out_.reserve(attr_indices_.size());

        for (size_t idx : attr_indices_) {
            // CRITIQUE: No bounds checking
            out_.push_back(std::move(fields[idx]));
        }

        return true;
    }

    void close() override {
        input->close();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        return std::move(out_);
    }
};

}  // namespace buzzdb
