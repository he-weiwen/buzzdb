#pragma once

/**
 * @file sql_parser.h
 * @brief Recursive descent SQL parser.
 *
 * ============================================================================
 * DESIGN NOTES
 * ============================================================================
 *
 * RECURSIVE DESCENT parsing - one function per grammar rule:
 *   parseSelect()    -> SELECT statement
 *   parseExpr()      -> entry point for expressions
 *   parseOr()        -> OR has lowest precedence
 *   parseAnd()       -> AND has higher precedence than OR
 *   parseComparison()-> comparison operators (=, <, >, etc.)
 *   parsePrimary()   -> literals, columns, parenthesized expressions
 *
 * PRECEDENCE (low to high):
 *   OR -> AND -> NOT -> comparisons -> primary
 *
 * ERROR HANDLING:
 *   Throws ParseError with line/column information.
 *   No error recovery - fails fast on first error.
 *
 * LAB 4 COMPATIBILITY:
 *   Supports both {n} column references and named columns.
 *   SUM{n} syntax is converted to SUM({n}) internally.
 *
 * ============================================================================
 */

#include <stdexcept>
#include <sstream>

#include "execution/sql_lexer.h"
#include "execution/sql_ast.h"

namespace buzzdb {

// ============================================================================
// Parse Error
// ============================================================================

class ParseError : public std::runtime_error {
public:
    ParseError(const std::string& msg, size_t line, size_t col)
        : std::runtime_error(formatMessage(msg, line, col))
        , line_(line), column_(col) {}

    size_t line() const { return line_; }
    size_t column() const { return column_; }

private:
    size_t line_, column_;

    static std::string formatMessage(const std::string& msg, size_t line, size_t col) {
        std::ostringstream oss;
        oss << "Parse error at line " << line << ", column " << col << ": " << msg;
        return oss.str();
    }
};

// ============================================================================
// Parser
// ============================================================================

class Parser {
public:
    explicit Parser(std::vector<Token> tokens)
        : tokens_(std::move(tokens)), pos_(0) {}

    /**
     * Parse a SELECT statement.
     */
    std::unique_ptr<SelectStmt> parseSelect() {
        expect(TokenType::SELECT, "Expected SELECT");

        // Parse column list
        std::vector<ExprPtr> columns;
        columns.push_back(parseSelectItem());
        while (match(TokenType::COMMA)) {
            columns.push_back(parseSelectItem());
        }

        // FROM clause
        expect(TokenType::FROM, "Expected FROM");
        auto fromTable = parseTableRef();

        auto stmt = std::make_unique<SelectStmt>(std::move(fromTable));
        stmt->columns = std::move(columns);

        // Optional JOIN
        if (match(TokenType::JOIN)) {
            auto joinTable = parseTableRef();
            expect(TokenType::ON, "Expected ON after JOIN table");
            auto joinCond = parseExpr();
            stmt->join = JoinClause(std::move(joinTable), std::move(joinCond));
        }

        // Optional WHERE
        if (match(TokenType::WHERE)) {
            stmt->whereClause = parseExpr();
        }

        // Optional aggregate (Lab 4 style: SUM{n})
        if (check(TokenType::SUM) || check(TokenType::COUNT) ||
            check(TokenType::MIN) || check(TokenType::MAX) || check(TokenType::AVG)) {
            // This is a hack for Lab 4 compatibility where SUM{3} appears after WHERE
            // In standard SQL, aggregates are in SELECT clause
            auto aggExpr = parseAggregate();
            // Replace the select columns with the aggregate
            stmt->columns.clear();
            stmt->columns.push_back(std::move(aggExpr));
        }

        // Optional GROUP BY
        if (match(TokenType::GROUP)) {
            expect(TokenType::BY, "Expected BY after GROUP");
            stmt->groupBy.push_back(parseExpr());
            while (match(TokenType::COMMA)) {
                stmt->groupBy.push_back(parseExpr());
            }
        }

        // Optional ORDER BY
        if (match(TokenType::ORDER)) {
            expect(TokenType::BY, "Expected BY after ORDER");
            do {
                auto expr = parseExpr();
                bool asc = true;
                if (match(TokenType::DESC)) asc = false;
                else match(TokenType::ASC);  // optional ASC
                stmt->orderBy.emplace_back(std::move(expr), asc);
            } while (match(TokenType::COMMA));
        }

        // Optional LIMIT
        if (match(TokenType::LIMIT)) {
            auto& tok = expect(TokenType::INT_LIT, "Expected integer after LIMIT");
            stmt->limit = static_cast<int>(tok.asInt());
        }

        return stmt;
    }

private:
    std::vector<Token> tokens_;
    size_t pos_;

    // -------------------------------------------------------------------------
    // Token Navigation
    // -------------------------------------------------------------------------

    const Token& peek() const { return tokens_[pos_]; }

    const Token& previous() const { return tokens_[pos_ - 1]; }

    bool atEnd() const { return peek().type == TokenType::END_OF_INPUT; }

    const Token& advance() {
        if (!atEnd()) pos_++;
        return previous();
    }

    bool check(TokenType type) const {
        return !atEnd() && peek().type == type;
    }

    bool match(TokenType type) {
        if (check(type)) {
            advance();
            return true;
        }
        return false;
    }

    const Token& expect(TokenType type, const std::string& message) {
        if (check(type)) return advance();
        throw ParseError(message, peek().line, peek().column);
    }

    // -------------------------------------------------------------------------
    // Parsing Rules
    // -------------------------------------------------------------------------

    ExprPtr parseSelectItem() {
        // Check for aggregate functions in select
        if (check(TokenType::SUM) || check(TokenType::COUNT) ||
            check(TokenType::MIN) || check(TokenType::MAX) || check(TokenType::AVG)) {
            return parseAggregate();
        }

        // Check for * or {*}
        if (match(TokenType::STAR)) {
            return std::make_unique<StarExpr>();
        }

        return parseExpr();
    }

    TableRef parseTableRef() {
        // Handle {TABLE} syntax
        if (check(TokenType::COLUMN_REF)) {
            throw ParseError("Expected table name, not column reference", peek().line, peek().column);
        }

        // Check for {IDENT} style (Lab 4 uses braces around table names too)
        // Actually the lexer converts {IDENT} to COLUMN_REF only if it's a number
        // For {TABLE}, it would fail. Let me handle this differently.

        std::string name;
        if (check(TokenType::IDENT)) {
            name = advance().asString();
        } else {
            throw ParseError("Expected table name", peek().line, peek().column);
        }

        std::optional<std::string> alias;
        // Simple alias: FROM table t (no AS keyword for brevity)
        if (check(TokenType::IDENT) && !check(TokenType::JOIN) && !check(TokenType::WHERE) &&
            !check(TokenType::ON) && !check(TokenType::GROUP) && !check(TokenType::ORDER)) {
            alias = advance().asString();
        }

        return TableRef(std::move(name), std::move(alias));
    }

    ExprPtr parseAggregate() {
        AggregateType aggType;
        if (match(TokenType::SUM)) aggType = AggregateType::SUM;
        else if (match(TokenType::COUNT)) aggType = AggregateType::COUNT;
        else if (match(TokenType::MIN)) aggType = AggregateType::MIN;
        else if (match(TokenType::MAX)) aggType = AggregateType::MAX;
        else if (match(TokenType::AVG)) aggType = AggregateType::AVG;
        else throw ParseError("Expected aggregate function", peek().line, peek().column);

        // Lab 4 style: SUM{3} - no parentheses
        if (check(TokenType::COLUMN_REF)) {
            int idx = static_cast<int>(advance().asInt());
            return std::make_unique<AggregateExpr>(aggType,
                std::make_unique<ColumnExpr>(idx));
        }

        // Standard SQL style: SUM(expr)
        expect(TokenType::LPAREN, "Expected '(' after aggregate function");
        auto arg = parseExpr();
        expect(TokenType::RPAREN, "Expected ')' after aggregate argument");

        return std::make_unique<AggregateExpr>(aggType, std::move(arg));
    }

    // -------------------------------------------------------------------------
    // Expression Parsing (Precedence Climbing)
    // -------------------------------------------------------------------------

    ExprPtr parseExpr() {
        return parseOr();
    }

    ExprPtr parseOr() {
        auto left = parseAnd();

        while (match(TokenType::OR)) {
            auto right = parseAnd();
            left = std::make_unique<BinaryExpr>(
                std::move(left), BinaryExpr::Op::OR, std::move(right));
        }

        return left;
    }

    ExprPtr parseAnd() {
        auto left = parseNot();

        while (match(TokenType::AND)) {
            auto right = parseNot();
            left = std::make_unique<BinaryExpr>(
                std::move(left), BinaryExpr::Op::AND, std::move(right));
        }

        return left;
    }

    ExprPtr parseNot() {
        if (match(TokenType::NOT)) {
            auto operand = parseNot();
            return std::make_unique<UnaryExpr>(UnaryExpr::Op::NOT, std::move(operand));
        }
        return parseComparison();
    }

    ExprPtr parseComparison() {
        auto left = parsePrimary();

        if (check(TokenType::EQ) || check(TokenType::NE) ||
            check(TokenType::LT) || check(TokenType::GT) ||
            check(TokenType::LE) || check(TokenType::GE)) {

            BinaryExpr::Op op;
            switch (advance().type) {
                case TokenType::EQ: op = BinaryExpr::Op::EQ; break;
                case TokenType::NE: op = BinaryExpr::Op::NE; break;
                case TokenType::LT: op = BinaryExpr::Op::LT; break;
                case TokenType::GT: op = BinaryExpr::Op::GT; break;
                case TokenType::LE: op = BinaryExpr::Op::LE; break;
                case TokenType::GE: op = BinaryExpr::Op::GE; break;
                default: throw ParseError("Unexpected operator", previous().line, previous().column);
            }

            auto right = parsePrimary();
            left = std::make_unique<BinaryExpr>(std::move(left), op, std::move(right));
        }

        return left;
    }

    ExprPtr parsePrimary() {
        // Integer literal
        if (match(TokenType::INT_LIT)) {
            return std::make_unique<LiteralExpr>(previous().asInt());
        }

        // String literal
        if (match(TokenType::STRING_LIT)) {
            return std::make_unique<LiteralExpr>(previous().asString());
        }

        // Column reference {n}
        if (match(TokenType::COLUMN_REF)) {
            return std::make_unique<ColumnExpr>(static_cast<int>(previous().asInt()));
        }

        // Star
        if (match(TokenType::STAR)) {
            return std::make_unique<StarExpr>();
        }

        // Identifier (column or table.column)
        if (match(TokenType::IDENT)) {
            std::string first = previous().asString();

            if (match(TokenType::DOT)) {
                if (match(TokenType::STAR)) {
                    return std::make_unique<StarExpr>(first);
                }
                expect(TokenType::IDENT, "Expected column name after '.'");
                return std::make_unique<ColumnExpr>(first, previous().asString());
            }

            return std::make_unique<ColumnExpr>(first);
        }

        // Parenthesized expression
        if (match(TokenType::LPAREN)) {
            auto expr = parseExpr();
            expect(TokenType::RPAREN, "Expected ')' after expression");
            return expr;
        }

        throw ParseError("Expected expression", peek().line, peek().column);
    }
};

// ============================================================================
// Convenience Function
// ============================================================================

/**
 * Parse a SQL query string into an AST.
 */
inline std::unique_ptr<SelectStmt> parse(const std::string& query) {
    Lexer lexer(query);
    auto tokens = lexer.tokenize();
    Parser parser(std::move(tokens));
    return parser.parseSelect();
}

}  // namespace buzzdb
