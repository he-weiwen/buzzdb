#pragma once

/**
 * @file hash_join_operator.h
 * @brief HashJoinOperator - inner equi-join using hash table.
 *
 * Implements a simple hash join:
 * 1. Build phase: Hash all tuples from left input on join attribute
 * 2. Probe phase: For each right tuple, probe hash table for matches
 * 3. Output: Concatenated left+right tuples for all matches
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. MATERIALIZES ALL OUTPUT IN MEMORY
 *    All matching tuple pairs are computed in open() and stored in outputs_.
 *    This can use unbounded memory for large joins. A proper implementation
 *    would produce matches lazily in next().
 *
 * 2. STRING KEY HASHING
 *    Uses field->asString() as hash key, which:
 *    - Is slow (string conversion + hashing)
 *    - May have collision issues (different values same string repr)
 *    Should use proper field hashing.
 *
 * 3. INNER JOIN ONLY
 *    No support for LEFT/RIGHT/FULL OUTER joins.
 *
 * 4. SINGLE EQUI-JOIN ONLY
 *    Can only join on one attribute with equality. No support for
 *    composite keys or non-equality conditions.
 *
 * 5. BUILD SIDE NOT CONFIGURABLE
 *    Always builds hash table on left input. Optimizer should choose
 *    smaller input for build side.
 *
 * ============================================================================
 */

#include <unordered_map>
#include <string>

#include "execution/operator.h"

namespace buzzdb {

/**
 * @brief Inner equi-join using a hash table.
 *
 * Joins two inputs on specified attributes using hash join algorithm.
 * Output tuples are concatenation of matching left and right tuples.
 */
class HashJoinOperator : public BinaryOperator {
private:
    using Tup = std::vector<std::unique_ptr<Field>>;

    size_t left_attr_index_;
    size_t right_attr_index_;

    /// Hash table: join key -> list of tuples with that key
    std::unordered_map<std::string, std::vector<Tup>> hash_table_;

    /// All output tuples (computed in open)
    std::vector<Tup> outputs_;

    /// Current output index for iteration
    int output_index_ = -1;

public:
    HashJoinOperator(Operator& left, Operator& right,
                     size_t left_attr_index, size_t right_attr_index)
        : BinaryOperator(left, right)
        , left_attr_index_(left_attr_index)
        , right_attr_index_(right_attr_index) {}

    void open() override {
        hash_table_.clear();
        outputs_.clear();
        output_index_ = -1;

        // Build phase: hash left input
        input_left->open();
        while (input_left->next()) {
            auto tup = input_left->getOutput();
            std::string key = tup[left_attr_index_]->asString();
            hash_table_[key].push_back(std::move(tup));
        }

        // Probe phase: match right input
        input_right->open();
        while (input_right->next()) {
            auto right_tup = input_right->getOutput();
            std::string key = right_tup[right_attr_index_]->asString();

            auto it = hash_table_.find(key);
            if (it != hash_table_.end()) {
                // Match found - produce output for each left tuple
                for (const auto& left_tup : it->second) {
                    outputs_.push_back(mergeTuples(left_tup, right_tup));
                }
            }
        }
    }

    bool next() override {
        ++output_index_;
        return output_index_ < static_cast<int>(outputs_.size());
    }

    void close() override {
        input_left->close();
        input_right->close();
        hash_table_.clear();
        outputs_.clear();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (output_index_ < 0 || output_index_ >= static_cast<int>(outputs_.size())) {
            return {};
        }
        return std::move(outputs_[output_index_]);
    }

private:
    /// Concatenate two tuples (cloning all fields)
    Tup mergeTuples(const Tup& left, const Tup& right) {
        Tup result;
        result.reserve(left.size() + right.size());

        for (const auto& f : left) {
            result.push_back(f->clone());
        }
        for (const auto& f : right) {
            result.push_back(f->clone());
        }

        return result;
    }
};

}  // namespace buzzdb
