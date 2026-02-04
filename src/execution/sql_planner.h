#pragma once

/**
 * @file sql_planner.h
 * @brief Converts SQL AST to query operator tree.
 *
 * ============================================================================
 * DESIGN NOTES
 * ============================================================================
 *
 * The planner performs AST-to-operator translation without optimization.
 * This is intentionally simple:
 *
 * 1. FROM clause -> ScanOperator
 * 2. JOIN clause -> HashJoinOperator on top of two scans
 * 3. WHERE clause -> SelectOperator with predicate
 * 4. GROUP BY + aggregates -> HashAggregationOperator
 * 5. SELECT columns -> ProjectOperator (if not *)
 *
 * A real query planner would:
 * - Choose between hash join, nested loop, merge join
 * - Push down predicates
 * - Reorder joins for optimal cost
 * - Use indexes when available
 *
 * EXPRESSION TO PREDICATE CONVERSION:
 * Uses dynamic_cast to dispatch on expression type.
 * This is where inheritance shows its tradeoff - we need runtime type checks.
 * Alternative would be visitor pattern, but dynamic_cast is simpler for
 * this small number of expression types.
 *
 * ============================================================================
 */

#include <memory>
#include <stdexcept>

#include "execution/sql_ast.h"
#include "execution/operators.h"
#include "buffer/buffer_manager.h"

namespace buzzdb {

// ============================================================================
// Forward Declarations
// ============================================================================

inline SimplePredicate::Operand exprToOperand(const Expr* expr);
inline bool hasAggregates(const SelectStmt& stmt);
inline std::vector<AggrFunc> extractAggregates(const SelectStmt& stmt);

// ============================================================================
// Expression to Predicate Conversion
// ============================================================================

/**
 * Convert an AST expression to an operator predicate.
 * Returns nullptr for expressions that can't be converted to predicates.
 */
inline std::unique_ptr<IPredicate> exprToPredicate(const Expr* expr) {
    if (!expr) return nullptr;

    // Binary expression -> SimplePredicate or ComplexPredicate
    if (auto* binary = dynamic_cast<const BinaryExpr*>(expr)) {
        // Logical operators -> ComplexPredicate
        if (binary->op == BinaryExpr::Op::AND || binary->op == BinaryExpr::Op::OR) {
            auto logicOp = (binary->op == BinaryExpr::Op::AND)
                ? ComplexPredicate::LogicOperator::AND
                : ComplexPredicate::LogicOperator::OR;

            auto complex = std::make_unique<ComplexPredicate>(logicOp);
            complex->addPredicate(exprToPredicate(binary->left.get()));
            complex->addPredicate(exprToPredicate(binary->right.get()));
            return complex;
        }

        // Comparison operators -> SimplePredicate
        SimplePredicate::ComparisonOperator cmpOp;
        switch (binary->op) {
            case BinaryExpr::Op::EQ: cmpOp = SimplePredicate::ComparisonOperator::EQ; break;
            case BinaryExpr::Op::NE: cmpOp = SimplePredicate::ComparisonOperator::NE; break;
            case BinaryExpr::Op::LT: cmpOp = SimplePredicate::ComparisonOperator::LT; break;
            case BinaryExpr::Op::GT: cmpOp = SimplePredicate::ComparisonOperator::GT; break;
            case BinaryExpr::Op::LE: cmpOp = SimplePredicate::ComparisonOperator::LE; break;
            case BinaryExpr::Op::GE: cmpOp = SimplePredicate::ComparisonOperator::GE; break;
            default:
                throw std::runtime_error("Unsupported comparison operator");
        }

        // Convert left operand
        SimplePredicate::Operand leftOp = exprToOperand(binary->left.get());
        SimplePredicate::Operand rightOp = exprToOperand(binary->right.get());

        return std::make_unique<SimplePredicate>(
            std::move(leftOp), std::move(rightOp), cmpOp);
    }

    throw std::runtime_error("Cannot convert expression to predicate: " + expr->toString());
}

/**
 * Convert an AST expression to a predicate operand.
 */
inline SimplePredicate::Operand exprToOperand(const Expr* expr) {
    // Column reference
    if (auto* col = dynamic_cast<const ColumnExpr*>(expr)) {
        if (col->index) {
            // {n} style - convert to 0-indexed
            return SimplePredicate::Operand(static_cast<size_t>(*col->index - 1));
        }
        // Named column - would need schema lookup, not implemented
        throw std::runtime_error("Named columns require schema lookup (not implemented)");
    }

    // Literal value
    if (auto* lit = dynamic_cast<const LiteralExpr*>(expr)) {
        return std::visit([](const auto& v) -> SimplePredicate::Operand {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int64_t>) {
                return SimplePredicate::Operand(std::make_unique<Field>(static_cast<int>(v)));
            } else if constexpr (std::is_same_v<T, std::string>) {
                return SimplePredicate::Operand(std::make_unique<Field>(v));
            } else if constexpr (std::is_same_v<T, double>) {
                // Field doesn't support double, use int
                return SimplePredicate::Operand(std::make_unique<Field>(static_cast<int>(v)));
            } else {
                // monostate = NULL, not supported in predicates
                throw std::runtime_error("NULL not supported in predicates");
            }
        }, lit->value);
    }

    throw std::runtime_error("Cannot convert expression to operand: " + expr->toString());
}

// ============================================================================
// Query Planner
// ============================================================================

/**
 * Result of query planning - holds the operator tree and any resources
 * needed to keep it alive.
 */
struct PlannedQuery {
    // The root operator to execute
    Operator* root;

    // Storage for operators (needed because operators store references to children)
    std::unique_ptr<ScanOperator> scan1;
    std::unique_ptr<ScanOperator> scan2;
    std::unique_ptr<HashJoinOperator> join;
    std::unique_ptr<SelectOperator> select;
    std::unique_ptr<HashAggregationOperator> aggregate;
    std::unique_ptr<ProjectOperator> project;

    // Storage for predicate (needs to outlive SelectOperator)
    std::unique_ptr<IPredicate> predicate;
};

/**
 * Plan a SELECT statement into an operator tree.
 *
 * @param stmt The parsed SELECT statement.
 * @param bm The buffer manager for data access.
 * @return A PlannedQuery containing the operator tree.
 */
inline PlannedQuery plan(const SelectStmt& stmt, BufferManager& bm) {
    PlannedQuery result;

    // Start with scan of FROM table
    result.scan1 = std::make_unique<ScanOperator>(bm, stmt.fromTable.name);
    result.root = result.scan1.get();

    // Add JOIN if present
    if (stmt.join) {
        result.scan2 = std::make_unique<ScanOperator>(bm, stmt.join->table.name);

        // Extract join column indices from the ON condition
        // Expects: {n} = {m} or col1 = col2
        auto* joinCond = dynamic_cast<const BinaryExpr*>(stmt.join->condition.get());
        if (!joinCond || joinCond->op != BinaryExpr::Op::EQ) {
            throw std::runtime_error("JOIN ON must be an equality condition");
        }

        auto* leftCol = dynamic_cast<const ColumnExpr*>(joinCond->left.get());
        auto* rightCol = dynamic_cast<const ColumnExpr*>(joinCond->right.get());
        if (!leftCol || !rightCol || !leftCol->index || !rightCol->index) {
            throw std::runtime_error("JOIN ON columns must be {n} style references");
        }

        result.join = std::make_unique<HashJoinOperator>(
            *result.root, *result.scan2,
            static_cast<size_t>(*leftCol->index - 1),
            static_cast<size_t>(*rightCol->index - 1)
        );
        result.root = result.join.get();
    }

    // Add WHERE filter if present
    if (stmt.whereClause) {
        result.predicate = exprToPredicate(stmt.whereClause.get());
        result.select = std::make_unique<SelectOperator>(
            *result.root, std::move(result.predicate));
        result.root = result.select.get();
    }

    // Add aggregation if GROUP BY or aggregate functions present
    if (!stmt.groupBy.empty() || hasAggregates(stmt)) {
        std::vector<size_t> groupByAttrs;
        for (const auto& expr : stmt.groupBy) {
            auto* col = dynamic_cast<const ColumnExpr*>(expr.get());
            if (!col || !col->index) {
                throw std::runtime_error("GROUP BY must use {n} style column references");
            }
            groupByAttrs.push_back(static_cast<size_t>(*col->index - 1));
        }

        std::vector<AggrFunc> aggrFuncs = extractAggregates(stmt);

        result.aggregate = std::make_unique<HashAggregationOperator>(
            *result.root, groupByAttrs, aggrFuncs);
        result.root = result.aggregate.get();
    }

    return result;
}

/**
 * Check if the SELECT clause contains aggregate functions.
 */
inline bool hasAggregates(const SelectStmt& stmt) {
    for (const auto& col : stmt.columns) {
        if (dynamic_cast<const AggregateExpr*>(col.get())) {
            return true;
        }
    }
    return false;
}

/**
 * Extract aggregate functions from SELECT clause.
 */
inline std::vector<AggrFunc> extractAggregates(const SelectStmt& stmt) {
    std::vector<AggrFunc> result;

    for (const auto& col : stmt.columns) {
        if (auto* agg = dynamic_cast<const AggregateExpr*>(col.get())) {
            AggrFuncType funcType;
            switch (agg->aggType) {
                case AggregateType::SUM: funcType = AggrFuncType::SUM; break;
                case AggregateType::COUNT: funcType = AggrFuncType::COUNT; break;
                case AggregateType::MIN: funcType = AggrFuncType::MIN; break;
                case AggregateType::MAX: funcType = AggrFuncType::MAX; break;
                default:
                    throw std::runtime_error("Unsupported aggregate function");
            }

            auto* argCol = dynamic_cast<const ColumnExpr*>(agg->argument.get());
            if (!argCol || !argCol->index) {
                throw std::runtime_error("Aggregate argument must be {n} style column reference");
            }

            result.push_back({funcType, static_cast<size_t>(*argCol->index - 1)});
        }
    }

    return result;
}

// ============================================================================
// Execute Query (Convenience)
// ============================================================================

/**
 * Parse, plan, and execute a SQL query.
 *
 * @param query The SQL query string.
 * @param bm The buffer manager.
 * @return Vector of result rows, each row is a vector of Fields.
 */
inline std::vector<std::vector<std::unique_ptr<Field>>>
executeSQL(const std::string& query, BufferManager& bm) {
    // Parse
    auto stmt = parse(query);

    // Plan
    auto planned = plan(*stmt, bm);

    // Execute
    std::vector<std::vector<std::unique_ptr<Field>>> results;

    planned.root->open();
    while (planned.root->next()) {
        auto output = planned.root->getOutput();
        std::vector<std::unique_ptr<Field>> row;
        for (auto& field : output) {
            row.push_back(field->clone());
        }
        results.push_back(std::move(row));
    }
    planned.root->close();

    return results;
}

}  // namespace buzzdb
