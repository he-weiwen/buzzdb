#pragma once

/**
 * @file sql_ast.h
 * @brief Abstract Syntax Tree nodes for SQL queries.
 *
 * ============================================================================
 * DESIGN NOTES: VARIANT VS INHERITANCE
 * ============================================================================
 *
 * This file uses a HYBRID approach:
 *
 * 1. INHERITANCE for Expr nodes (expressions like columns, literals, a > b)
 *    - Expressions are recursive: BinaryExpr contains child Expr nodes
 *    - With std::variant, recursion requires awkward unique_ptr wrappers:
 *        using Expr = std::variant<Literal, ColumnRef, std::unique_ptr<BinaryExpr>>;
 *    - Inheritance handles recursion naturally:
 *        struct BinaryExpr : Expr { std::unique_ptr<Expr> left, right; };
 *    - Tradeoff: must use dynamic_cast or visitor pattern for dispatch
 *
 * 2. VARIANT for leaf values (literal values, aggregate types)
 *    - Flat, non-recursive sum types are perfect for variant
 *    - Compiler enforces exhaustive handling with std::visit
 *    - Example: LiteralValue = variant<int64_t, string, double, monostate>
 *
 * WHY NOT ALL-VARIANT?
 *    Recursive variant requires: variant<A, B, unique_ptr<RecursiveType>>
 *    Then every pattern match needs to unwrap the pointer:
 *        std::visit(overloaded{
 *            [](const std::unique_ptr<BinaryExpr>& b) { ... b->left ... }
 *        }, expr);
 *    This adds friction without meaningful benefit for tree structures.
 *
 * WHY NOT ALL-INHERITANCE?
 *    For flat sum types (literals, aggregates), variant is cleaner:
 *    - No virtual destructor needed
 *    - No heap allocation for small types
 *    - std::visit ensures all cases handled
 *
 * ============================================================================
 */

#include <memory>
#include <vector>
#include <string>
#include <variant>
#include <optional>

namespace buzzdb {

// ============================================================================
// Literal Values (variant - flat sum type)
// ============================================================================

/**
 * Literal value in SQL.
 * monostate represents NULL.
 */
using LiteralValue = std::variant<std::monostate, int64_t, double, std::string>;

// ============================================================================
// Expressions (inheritance - recursive tree structure)
// ============================================================================

/**
 * Base class for all expressions.
 * Using inheritance because expressions are recursive (BinaryExpr contains Exprs).
 */
struct Expr {
    virtual ~Expr() = default;

    // For debugging
    virtual std::string toString() const = 0;
};

using ExprPtr = std::unique_ptr<Expr>;

/**
 * Column reference: table.column or just column
 * Also handles {n} style references from Lab 4.
 */
struct ColumnExpr : Expr {
    std::optional<std::string> table;  // optional table/alias prefix
    std::string column;                 // column name
    std::optional<int> index;           // {n} style: 1-indexed column number

    explicit ColumnExpr(std::string col)
        : column(std::move(col)) {}

    ColumnExpr(std::string tbl, std::string col)
        : table(std::move(tbl)), column(std::move(col)) {}

    explicit ColumnExpr(int idx)
        : column("{" + std::to_string(idx) + "}"), index(idx) {}

    std::string toString() const override {
        if (index) return "{" + std::to_string(*index) + "}";
        if (table) return *table + "." + column;
        return column;
    }
};

/**
 * Literal expression: 42, 'hello', 3.14, NULL
 */
struct LiteralExpr : Expr {
    LiteralValue value;

    explicit LiteralExpr(LiteralValue v) : value(std::move(v)) {}

    std::string toString() const override {
        return std::visit([](const auto& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::monostate>) return "NULL";
            else if constexpr (std::is_same_v<T, std::string>) return "'" + v + "'";
            else return std::to_string(v);
        }, value);
    }
};

/**
 * Binary expression: left op right
 */
struct BinaryExpr : Expr {
    enum class Op {
        // Comparison
        EQ, NE, LT, GT, LE, GE,
        // Logical
        AND, OR
    };

    ExprPtr left;
    Op op;
    ExprPtr right;

    BinaryExpr(ExprPtr l, Op o, ExprPtr r)
        : left(std::move(l)), op(o), right(std::move(r)) {}

    std::string toString() const override {
        static const char* opNames[] = {"=", "!=", "<", ">", "<=", ">=", "AND", "OR"};
        return "(" + left->toString() + " " + opNames[static_cast<int>(op)] + " " + right->toString() + ")";
    }
};

/**
 * Unary expression: NOT expr, -expr
 */
struct UnaryExpr : Expr {
    enum class Op { NOT, NEG };

    ExprPtr operand;
    Op op;

    UnaryExpr(Op o, ExprPtr e) : operand(std::move(e)), op(o) {}

    std::string toString() const override {
        return (op == Op::NOT ? "NOT " : "-") + operand->toString();
    }
};

/**
 * Star expression: SELECT *
 */
struct StarExpr : Expr {
    std::optional<std::string> table;  // optional: table.*

    StarExpr() = default;
    explicit StarExpr(std::string tbl) : table(std::move(tbl)) {}

    std::string toString() const override {
        return table ? *table + ".*" : "*";
    }
};

// ============================================================================
// Aggregate Functions (variant - flat sum type)
// ============================================================================

enum class AggregateType { SUM, COUNT, MIN, MAX, AVG };

struct AggregateExpr : Expr {
    AggregateType aggType;
    ExprPtr argument;  // the column/expression being aggregated

    AggregateExpr(AggregateType t, ExprPtr arg)
        : aggType(t), argument(std::move(arg)) {}

    std::string toString() const override {
        static const char* names[] = {"SUM", "COUNT", "MIN", "MAX", "AVG"};
        return std::string(names[static_cast<int>(aggType)]) + "(" + argument->toString() + ")";
    }
};

// ============================================================================
// Table References
// ============================================================================

struct TableRef {
    std::string name;
    std::optional<std::string> alias;

    explicit TableRef(std::string n, std::optional<std::string> a = std::nullopt)
        : name(std::move(n)), alias(std::move(a)) {}
};

// ============================================================================
// JOIN Clause
// ============================================================================

struct JoinClause {
    TableRef table;
    ExprPtr condition;  // ON condition

    JoinClause(TableRef t, ExprPtr cond)
        : table(std::move(t)), condition(std::move(cond)) {}
};

// ============================================================================
// SELECT Statement
// ============================================================================

struct SelectStmt {
    std::vector<ExprPtr> columns;           // SELECT clause (may include StarExpr)
    TableRef fromTable;                      // FROM clause
    std::optional<JoinClause> join;          // optional JOIN
    ExprPtr whereClause;                     // optional WHERE (nullptr if none)
    std::vector<ExprPtr> groupBy;            // optional GROUP BY
    std::vector<std::pair<ExprPtr, bool>> orderBy;  // optional ORDER BY (expr, isAsc)
    std::optional<int> limit;                // optional LIMIT

    explicit SelectStmt(TableRef from) : fromTable(std::move(from)) {}
};

}  // namespace buzzdb
