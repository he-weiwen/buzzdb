#pragma once

/**
 * @file query_parser.h
 * @brief Simple regex-based query parser from Lab 4.
 *
 * This is a minimal parser that handles a specific query syntax:
 *   SELECT {1}, {2} FROM {TABLE} [JOIN {TABLE2} ON {1} = {2}]
 *   [WHERE {col} > X and {col} < Y] [SUM{col}] [GROUP BY {col}]
 *
 * ============================================================================
 * DESIGN CRITIQUE
 * ============================================================================
 *
 * 1. NOT A REAL PARSER
 *    Uses regex for each clause independently. No proper tokenization,
 *    no AST, no syntax validation. Will silently accept malformed queries.
 *
 * 2. LIMITED SYNTAX
 *    - Column references use {1}, {2} etc. (1-indexed)
 *    - WHERE only supports "col > X and col < Y" pattern
 *    - Only inner join supported
 *    - Only SUM aggregate
 *
 * 3. RELATION NAME IN TUPLE
 *    Assumes tuples have relation name as last field for filtering.
 *    This is a hack to simulate multiple tables in a single-file DB.
 *
 * 4. DEBUG OUTPUT
 *    parseQuery prints relation name to stdout. Should be removed or
 *    made optional.
 *
 * ============================================================================
 * FUTURE IMPROVEMENTS
 * ============================================================================
 *
 * This should be rewritten with:
 * - Proper lexer (tokenizer)
 * - Recursive descent or parser generator (ANTLR, Bison)
 * - AST representation
 * - Semantic analysis (type checking, name resolution)
 * - Query optimizer
 *
 * ============================================================================
 */

#include <string>
#include <vector>
#include <regex>
#include <optional>
#include <limits>
#include <iostream>

#include "execution/operators.h"
#include "buffer/buffer_manager.h"

namespace buzzdb {

/**
 * @brief Parsed components of a query.
 */
struct QueryComponents {
    std::vector<size_t> selectAttributes;

    bool sumOperation = false;
    int sumAttributeIndex = -1;

    bool groupBy = false;
    int groupByAttributeIndex = -1;

    bool whereCondition = false;
    int whereAttributeIndex = -1;
    int lowerBound = std::numeric_limits<int>::min();
    int upperBound = std::numeric_limits<int>::max();

    bool innerJoin = false;
    int joinAttributeIndex1 = -1;
    int joinAttributeIndex2 = -1;

    std::string relation;
    std::string joinRelation;
};

/**
 * @brief Parse a query string into components.
 * @param query The query string to parse.
 * @return Parsed query components.
 *
 * CRITIQUE: Uses regex, prints debug output, fragile.
 */
inline QueryComponents parseQuery(const std::string& query) {
    QueryComponents components;

    // Parse selected attributes: SELECT {1}, {2}
    // Regex groups: (1)=first digit, (2)=optional ", {digit}", (3)=second digit
    std::regex selectRegex("SELECT \\{(\\d+)\\}(, \\{(\\d+)\\})?");
    std::smatch selectMatches;
    if (std::regex_search(query, selectMatches, selectRegex)) {
        // Group 1: first attribute
        if (!selectMatches[1].str().empty()) {
            components.selectAttributes.push_back(
                static_cast<size_t>(std::stoi(selectMatches[1]) - 1));
        }
        // Group 3: optional second attribute (group 2 is the full ", {n}" part)
        if (selectMatches.size() > 3 && !selectMatches[3].str().empty()) {
            components.selectAttributes.push_back(
                static_cast<size_t>(std::stoi(selectMatches[3]) - 1));
        }
    }

    // Parse FROM clause
    size_t offset = query.find("FROM");
    if (offset != std::string::npos) {
        offset += 4;
        size_t start = query.find('{', offset);
        if (start != std::string::npos) {
            start++;
            size_t end = query.find('}', start);
            if (end != std::string::npos) {
                components.relation = query.substr(start, end - start);
            }
        }
    }

    // Parse JOIN clause
    size_t joinOffset = query.find("JOIN");
    if (joinOffset != std::string::npos) {
        components.innerJoin = true;
        joinOffset += 4;

        size_t start = query.find('{', joinOffset);
        if (start != std::string::npos) {
            start++;
            size_t end = query.find('}', start);
            if (end != std::string::npos) {
                components.joinRelation = query.substr(start, end - start);

                // Parse ON clause: ON {1} = {2}
                size_t onOffset = query.find("ON", end);
                if (onOffset != std::string::npos) {
                    onOffset += 2;
                    start = query.find('{', onOffset);
                    if (start != std::string::npos) {
                        start++;
                        end = query.find('}', start);
                        if (end != std::string::npos) {
                            components.joinAttributeIndex1 =
                                std::stoi(query.substr(start, end - start)) - 1;

                            // Find second attribute after =
                            size_t eqOffset = query.find('=', end);
                            if (eqOffset != std::string::npos) {
                                start = query.find('{', eqOffset);
                                if (start != std::string::npos) {
                                    start++;
                                    end = query.find('}', start);
                                    if (end != std::string::npos) {
                                        components.joinAttributeIndex2 =
                                            std::stoi(query.substr(start, end - start)) - 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Parse SUM{col}
    std::regex sumRegex("SUM\\{(\\d+)\\}");
    std::smatch sumMatches;
    if (std::regex_search(query, sumMatches, sumRegex)) {
        components.sumOperation = true;
        components.sumAttributeIndex = std::stoi(sumMatches[1]) - 1;
    }

    // Parse GROUP BY {col}
    std::regex groupByRegex("GROUP BY \\{(\\d+)\\}");
    std::smatch groupByMatches;
    if (std::regex_search(query, groupByMatches, groupByRegex)) {
        components.groupBy = true;
        components.groupByAttributeIndex = std::stoi(groupByMatches[1]) - 1;
    }

    // Parse WHERE {col} > X and {col} < Y
    std::regex whereRegex("\\{(\\d+)\\} > (\\d+) and \\{(\\d+)\\} < (\\d+)");
    std::smatch whereMatches;
    if (std::regex_search(query, whereMatches, whereRegex)) {
        components.whereCondition = true;
        components.whereAttributeIndex = std::stoi(whereMatches[1]) - 1;
        components.lowerBound = std::stoi(whereMatches[2]);

        // Verify same attribute used for both conditions
        if (std::stoi(whereMatches[3]) - 1 == components.whereAttributeIndex) {
            components.upperBound = std::stoi(whereMatches[4]);
        } else {
            std::cerr << "Warning: WHERE clause uses different attributes\n";
        }
    }

    return components;
}

/**
 * @brief Print parsed query components for debugging.
 */
inline void prettyPrint(const QueryComponents& components) {
    std::cout << "Query Components:\n";

    std::cout << "  Relation: " << components.relation << "\n";

    std::cout << "  Selected Attributes: ";
    for (auto attr : components.selectAttributes) {
        std::cout << "{" << (attr + 1) << "} ";
    }
    std::cout << "\n";

    if (components.innerJoin) {
        std::cout << "  JOIN: " << components.joinRelation
                  << " ON {" << (components.joinAttributeIndex1 + 1) << "} = {"
                  << (components.joinAttributeIndex2 + 1) << "}\n";
    }

    if (components.whereCondition) {
        std::cout << "  WHERE: {" << (components.whereAttributeIndex + 1)
                  << "} > " << components.lowerBound
                  << " AND < " << components.upperBound << "\n";
    }

    if (components.sumOperation) {
        std::cout << "  SUM: {" << (components.sumAttributeIndex + 1) << "}\n";
    }

    if (components.groupBy) {
        std::cout << "  GROUP BY: {" << (components.groupByAttributeIndex + 1) << "}\n";
    }
}

/**
 * @brief Execute a parsed query and return results.
 * @param components Parsed query components.
 * @param buffer_manager The buffer manager for data access.
 * @return Vector of result tuples (each tuple is a vector of Fields).
 */
inline std::vector<std::vector<std::unique_ptr<Field>>>
executeQuery(const QueryComponents& components, BufferManager& buffer_manager) {

    // Create scan operators
    ScanOperator scanOp(buffer_manager, components.relation);
    ScanOperator scanOp2(buffer_manager, components.joinRelation);

    // Build operator tree bottom-up
    Operator* rootOp = &scanOp;

    // Optional operators (using optional for lifetime management)
    std::optional<HashJoinOperator> joinOpBuffer;
    std::optional<SelectOperator> selectOpBuffer;
    std::optional<HashAggregationOperator> hashAggOpBuffer;

    // Apply JOIN if specified
    if (components.innerJoin) {
        joinOpBuffer.emplace(*rootOp, scanOp2,
                             static_cast<size_t>(components.joinAttributeIndex1),
                             static_cast<size_t>(components.joinAttributeIndex2));
        rootOp = &*joinOpBuffer;
    }

    // Apply WHERE if specified
    if (components.whereAttributeIndex != -1) {
        auto pred1 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(static_cast<size_t>(components.whereAttributeIndex)),
            SimplePredicate::Operand(std::make_unique<Field>(components.lowerBound)),
            SimplePredicate::ComparisonOperator::GT
        );

        auto pred2 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(static_cast<size_t>(components.whereAttributeIndex)),
            SimplePredicate::Operand(std::make_unique<Field>(components.upperBound)),
            SimplePredicate::ComparisonOperator::LT
        );

        auto complexPred = std::make_unique<ComplexPredicate>(
            ComplexPredicate::LogicOperator::AND);
        complexPred->addPredicate(std::move(pred1));
        complexPred->addPredicate(std::move(pred2));

        selectOpBuffer.emplace(*rootOp, std::move(complexPred));
        rootOp = &*selectOpBuffer;
    }

    // Apply aggregation if specified
    if (components.sumOperation || components.groupBy) {
        std::vector<size_t> groupByAttrs;
        if (components.groupBy) {
            groupByAttrs.push_back(static_cast<size_t>(components.groupByAttributeIndex));
        }

        std::vector<AggrFunc> aggrFuncs;
        if (components.sumOperation) {
            aggrFuncs.push_back({AggrFuncType::SUM,
                                 static_cast<size_t>(components.sumAttributeIndex)});
        }

        hashAggOpBuffer.emplace(*rootOp, groupByAttrs, aggrFuncs);
        rootOp = &*hashAggOpBuffer;
    }

    // Execute and collect results
    std::vector<std::vector<std::unique_ptr<Field>>> result;

    rootOp->open();
    while (rootOp->next()) {
        auto output = rootOp->getOutput();
        std::vector<std::unique_ptr<Field>> tuple;
        for (auto& field : output) {
            tuple.push_back(field->clone());
        }
        result.push_back(std::move(tuple));
    }
    rootOp->close();

    return result;
}

}  // namespace buzzdb
