#pragma once

/**
 * @file aggregation.h
 * @brief Types and utilities for aggregate functions (COUNT, SUM, MIN, MAX).
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. LIMITED AGGREGATE FUNCTIONS
 *    Only COUNT, SUM, MIN, MAX are supported. Missing AVG (would need to
 *    track count and sum separately), STDDEV, etc.
 *
 * 2. AGGREGATE MODIFIES ACCUMULATOR IN-PLACE
 *    The aggregate() function modifies the accumulator Field using operator+=.
 *    This couples aggregation to Field's += operator implementation.
 *
 * 3. NO TYPE VALIDATION
 *    SUM on a string field would produce nonsense. Should validate types.
 *
 * 4. COUNT USES INT FIELD
 *    COUNT stores result in an INT field, which could overflow for large
 *    datasets. Should use 64-bit integer.
 *
 * ============================================================================
 */

#include <memory>
#include <vector>

#include "storage/field.h"

namespace buzzdb {

/**
 * @brief Types of aggregate functions.
 */
enum class AggrFuncType {
    COUNT,  ///< Count of rows
    SUM,    ///< Sum of values
    MIN,    ///< Minimum value
    MAX     ///< Maximum value
};

/**
 * @brief Specification for an aggregate function.
 */
struct AggrFunc {
    AggrFuncType func;      ///< The aggregate function to apply
    size_t attr_index;      ///< Index of the attribute to aggregate
};

/// Alias for tuple representation
using Tup = std::vector<std::unique_ptr<Field>>;

/**
 * @brief Apply an aggregate function to update an accumulator.
 * @param acc The accumulator field (modified in place).
 * @param tuple The current tuple being aggregated.
 * @param aggr The aggregate function specification.
 *
 * CRITIQUE: Modifies acc in-place using Field's += operator.
 * This only works because Field has custom += that handles type dispatch.
 */
inline void aggregate(std::unique_ptr<Field>& acc, Tup& tuple, AggrFunc& aggr) {
    switch (aggr.func) {
        case AggrFuncType::COUNT:
            *acc += 1;
            break;

        case AggrFuncType::SUM:
            if (tuple[aggr.attr_index]->getType() == FieldType::INT) {
                *acc += tuple[aggr.attr_index]->asInt();
            } else if (tuple[aggr.attr_index]->getType() == FieldType::FLOAT) {
                *acc += tuple[aggr.attr_index]->asFloat();
            }
            // CRITIQUE: Silently does nothing for STRING type
            break;

        case AggrFuncType::MIN:
            if (*acc > *tuple[aggr.attr_index]) {
                *acc = *tuple[aggr.attr_index];
            }
            break;

        case AggrFuncType::MAX:
            if (*acc < *tuple[aggr.attr_index]) {
                *acc = *tuple[aggr.attr_index];
            }
            break;
    }
}

}  // namespace buzzdb
