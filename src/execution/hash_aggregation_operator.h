#pragma once

/**
 * @file hash_aggregation_operator.h
 * @brief HashAggregationOperator - GROUP BY with aggregate functions.
 *
 * Uses a hash table to group tuples and compute aggregates per group.
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. STRING KEY FOR GROUPING
 *    Group key is computed by concatenating asString() of all group-by
 *    attributes. This is fragile - different values might have same string
 *    representation, or separator needed to avoid "ab" + "c" == "a" + "bc".
 *
 * 2. MATERIALIZES ALL GROUPS IN MEMORY
 *    All groups are computed in open(). For many distinct groups, this
 *    could exhaust memory. Could use external aggregation with spilling.
 *
 * 3. LIMITED AGGREGATE OUTPUT SCHEMA
 *    Output is: group-by attributes followed by aggregate results.
 *    No way to reorder or rename outputs.
 *
 * 4. COUNT INITIALIZATION
 *    COUNT starts at 1 for first tuple, which is correct, but other
 *    aggregates clone the first tuple's value. This assumes first tuple
 *    has valid data for MIN/MAX initialization.
 *
 * 5. ITERATOR-BASED OUTPUT
 *    Uses map iterator for output, which works but moves data out of the
 *    map, destroying it. Can only iterate once.
 *
 * ============================================================================
 */

#include <unordered_map>
#include <string>

#include "execution/operator.h"
#include "execution/aggregation.h"

namespace buzzdb {

/**
 * @brief Groups tuples and computes aggregate functions.
 *
 * Output tuples contain: group-by attributes + aggregate results.
 */
class HashAggregationOperator : public UnaryOperator {
private:
    using Tup = std::vector<std::unique_ptr<Field>>;

    std::vector<size_t> group_by_attrs_;
    std::vector<AggrFunc> aggr_funcs_;

    /// Hash table: group key -> output tuple (group attrs + aggregates)
    std::unordered_map<std::string, Tup> groups_;

    /// Iterator for output
    std::unordered_map<std::string, Tup>::iterator output_it_;
    bool before_first_ = true;

public:
    HashAggregationOperator(Operator& input,
                            std::vector<size_t> group_by_attrs,
                            std::vector<AggrFunc> aggr_funcs)
        : UnaryOperator(input)
        , group_by_attrs_(std::move(group_by_attrs))
        , aggr_funcs_(std::move(aggr_funcs)) {}

    void open() override {
        groups_.clear();
        before_first_ = true;

        input->open();
        while (input->next()) {
            auto tup = input->getOutput();
            std::string group_key = computeGroupKey(tup);

            auto it = groups_.find(group_key);
            if (it == groups_.end()) {
                // New group - initialize
                Tup& group_tup = groups_[group_key];

                // Copy group-by attributes
                for (size_t idx : group_by_attrs_) {
                    group_tup.push_back(tup[idx]->clone());
                }

                // Initialize aggregates
                for (const auto& aggr : aggr_funcs_) {
                    if (aggr.func == AggrFuncType::COUNT) {
                        group_tup.push_back(std::make_unique<Field>(1));
                    } else {
                        // Initialize with first value
                        group_tup.push_back(tup[aggr.attr_index]->clone());
                    }
                }
            } else {
                // Existing group - update aggregates
                Tup& group_tup = it->second;
                size_t aggr_offset = group_by_attrs_.size();

                for (size_t i = 0; i < aggr_funcs_.size(); ++i) {
                    AggrFunc aggr = aggr_funcs_[i];  // Copy for non-const ref
                    aggregate(group_tup[aggr_offset + i], tup, aggr);
                }
            }
        }
    }

    bool next() override {
        if (before_first_) {
            output_it_ = groups_.begin();
            before_first_ = false;
            return output_it_ != groups_.end();
        }

        ++output_it_;
        return output_it_ != groups_.end();
    }

    void close() override {
        input->close();
        groups_.clear();
    }

    std::vector<std::unique_ptr<Field>> getOutput() override {
        if (output_it_ == groups_.end()) {
            return {};
        }
        return std::move(output_it_->second);
    }

private:
    /// Compute group key by concatenating group-by attribute values
    /// CRITIQUE: This is fragile - no separator between values
    std::string computeGroupKey(const Tup& tup) {
        std::string key;
        for (size_t idx : group_by_attrs_) {
            key += tup[idx]->asString();
        }
        return key;
    }
};

}  // namespace buzzdb
