#pragma once

/**
 * @file sql_lexer.h
 * @brief SQL tokenizer/lexer.
 *
 * ============================================================================
 * DESIGN NOTES
 * ============================================================================
 *
 * Token representation uses std::variant for the token value because:
 * - Tokens are a flat, non-recursive sum type (perfect fit for variant)
 * - Compiler enforces exhaustive handling via std::visit
 * - No virtual dispatch overhead (though negligible here)
 * - Value semantics - tokens can be copied, stored in vectors naturally
 *
 * The lexer is intentionally simple:
 * - Single-pass, no backtracking
 * - Keywords are case-insensitive (standard SQL behavior)
 * - Supports both {n} column references (Lab 4 compat) and identifiers
 * - Minimal error recovery - fails fast on invalid input
 *
 * ============================================================================
 */

#include <string>
#include <vector>
#include <variant>
#include <cctype>
#include <stdexcept>
#include <unordered_map>

namespace buzzdb {

// ============================================================================
// Token Types
// ============================================================================

enum class TokenType {
    // Keywords
    SELECT, FROM, WHERE, JOIN, ON, AND, OR, NOT,
    GROUP, BY, SUM, COUNT, MIN, MAX, AVG,
    ORDER, ASC, DESC, LIMIT,

    // Literals and identifiers
    IDENT,          // table/column name
    INT_LIT,        // integer literal
    STRING_LIT,     // 'string literal'
    COLUMN_REF,     // {n} style column reference (Lab 4 compat)

    // Operators
    EQ,             // =
    NE,             // != or <>
    LT,             // <
    GT,             // >
    LE,             // <=
    GE,             // >=

    // Punctuation
    LPAREN,         // (
    RPAREN,         // )
    COMMA,          // ,
    STAR,           // *
    DOT,            // .

    // Special
    END_OF_INPUT,
    INVALID
};

/**
 * Token value - uses variant because tokens are a flat sum type.
 * monostate = no value (keywords, punctuation)
 * int64_t = integer literals and column references
 * string = identifiers and string literals
 */
using TokenValue = std::variant<std::monostate, int64_t, std::string>;

struct Token {
    TokenType type;
    TokenValue value;
    size_t line;
    size_t column;

    Token(TokenType t, size_t ln, size_t col)
        : type(t), value(std::monostate{}), line(ln), column(col) {}

    Token(TokenType t, int64_t v, size_t ln, size_t col)
        : type(t), value(v), line(ln), column(col) {}

    Token(TokenType t, std::string v, size_t ln, size_t col)
        : type(t), value(std::move(v)), line(ln), column(col) {}

    // Convenience accessors
    int64_t asInt() const { return std::get<int64_t>(value); }
    const std::string& asString() const { return std::get<std::string>(value); }
    bool hasValue() const { return !std::holds_alternative<std::monostate>(value); }
};

// ============================================================================
// Lexer
// ============================================================================

class Lexer {
public:
    explicit Lexer(std::string input)
        : input_(std::move(input)), pos_(0), line_(1), column_(1) {}

    std::vector<Token> tokenize() {
        std::vector<Token> tokens;

        while (!atEnd()) {
            skipWhitespace();
            if (atEnd()) break;

            tokens.push_back(nextToken());
        }

        tokens.emplace_back(TokenType::END_OF_INPUT, line_, column_);
        return tokens;
    }

private:
    std::string input_;
    size_t pos_;
    size_t line_;
    size_t column_;

    // Keyword lookup table
    static const std::unordered_map<std::string, TokenType>& keywords() {
        static const std::unordered_map<std::string, TokenType> kw = {
            {"SELECT", TokenType::SELECT}, {"FROM", TokenType::FROM},
            {"WHERE", TokenType::WHERE}, {"JOIN", TokenType::JOIN},
            {"ON", TokenType::ON}, {"AND", TokenType::AND},
            {"OR", TokenType::OR}, {"NOT", TokenType::NOT},
            {"GROUP", TokenType::GROUP}, {"BY", TokenType::BY},
            {"SUM", TokenType::SUM}, {"COUNT", TokenType::COUNT},
            {"MIN", TokenType::MIN}, {"MAX", TokenType::MAX},
            {"AVG", TokenType::AVG}, {"ORDER", TokenType::ORDER},
            {"ASC", TokenType::ASC}, {"DESC", TokenType::DESC},
            {"LIMIT", TokenType::LIMIT}
        };
        return kw;
    }

    bool atEnd() const { return pos_ >= input_.size(); }

    char peek() const { return atEnd() ? '\0' : input_[pos_]; }

    char peekNext() const {
        return (pos_ + 1 >= input_.size()) ? '\0' : input_[pos_ + 1];
    }

    char advance() {
        char c = input_[pos_++];
        if (c == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        return c;
    }

    void skipWhitespace() {
        while (!atEnd() && std::isspace(peek())) {
            advance();
        }
    }

    Token nextToken() {
        size_t startLine = line_;
        size_t startCol = column_;
        char c = peek();

        // Single-character tokens
        if (c == '(') { advance(); return {TokenType::LPAREN, startLine, startCol}; }
        if (c == ')') { advance(); return {TokenType::RPAREN, startLine, startCol}; }
        if (c == ',') { advance(); return {TokenType::COMMA, startLine, startCol}; }
        if (c == '*') { advance(); return {TokenType::STAR, startLine, startCol}; }
        if (c == '.') { advance(); return {TokenType::DOT, startLine, startCol}; }

        // Comparison operators
        if (c == '=') { advance(); return {TokenType::EQ, startLine, startCol}; }
        if (c == '<') {
            advance();
            if (peek() == '=') { advance(); return {TokenType::LE, startLine, startCol}; }
            if (peek() == '>') { advance(); return {TokenType::NE, startLine, startCol}; }
            return {TokenType::LT, startLine, startCol};
        }
        if (c == '>') {
            advance();
            if (peek() == '=') { advance(); return {TokenType::GE, startLine, startCol}; }
            return {TokenType::GT, startLine, startCol};
        }
        if (c == '!' && peekNext() == '=') {
            advance(); advance();
            return {TokenType::NE, startLine, startCol};
        }

        // {n} column reference (Lab 4 compatibility)
        if (c == '{') {
            return scanColumnRef(startLine, startCol);
        }

        // String literal
        if (c == '\'') {
            return scanString(startLine, startCol);
        }

        // Number
        if (std::isdigit(c)) {
            return scanNumber(startLine, startCol);
        }

        // Identifier or keyword
        if (std::isalpha(c) || c == '_') {
            return scanIdentifier(startLine, startCol);
        }

        // Unknown character
        advance();
        return {TokenType::INVALID, startLine, startCol};
    }

    Token scanColumnRef(size_t startLine, size_t startCol) {
        advance();  // consume '{'

        // Check for {*}
        if (peek() == '*') {
            advance();  // consume '*'
            if (peek() != '}') {
                throw std::runtime_error("Expected '}' after '*' in column reference");
            }
            advance();  // consume '}'
            return {TokenType::STAR, startLine, startCol};
        }

        // Check if it's a number or identifier
        if (std::isdigit(peek())) {
            // Parse number: {1}, {2}, etc.
            std::string num;
            while (std::isdigit(peek())) {
                num += advance();
            }

            if (peek() != '}') {
                throw std::runtime_error("Expected '}' in column reference");
            }
            advance();  // consume '}'

            return {TokenType::COLUMN_REF, static_cast<int64_t>(std::stoll(num)), startLine, startCol};
        }

        // Parse identifier: {TABLE}, {STUDENTS}, etc. (Lab 4 compatibility)
        std::string ident;
        while (std::isalnum(peek()) || peek() == '_') {
            ident += advance();
        }

        if (peek() != '}') {
            throw std::runtime_error("Expected '}' in braced identifier");
        }
        advance();  // consume '}'

        if (ident.empty()) {
            throw std::runtime_error("Empty braced reference {}");
        }

        // Return as IDENT token (table/column name in braces)
        return {TokenType::IDENT, std::move(ident), startLine, startCol};
    }

    Token scanString(size_t startLine, size_t startCol) {
        advance();  // consume opening quote
        std::string str;

        while (!atEnd() && peek() != '\'') {
            if (peek() == '\\' && peekNext() == '\'') {
                advance();  // skip backslash
            }
            str += advance();
        }

        if (atEnd()) {
            throw std::runtime_error("Unterminated string literal");
        }
        advance();  // consume closing quote

        return {TokenType::STRING_LIT, std::move(str), startLine, startCol};
    }

    Token scanNumber(size_t startLine, size_t startCol) {
        std::string num;
        while (std::isdigit(peek())) {
            num += advance();
        }
        return {TokenType::INT_LIT, static_cast<int64_t>(std::stoll(num)), startLine, startCol};
    }

    Token scanIdentifier(size_t startLine, size_t startCol) {
        std::string ident;
        while (std::isalnum(peek()) || peek() == '_') {
            ident += advance();
        }

        // Convert to uppercase for keyword lookup
        std::string upper = ident;
        for (char& c : upper) c = std::toupper(c);

        auto it = keywords().find(upper);
        if (it != keywords().end()) {
            return {it->second, startLine, startCol};
        }

        return {TokenType::IDENT, std::move(ident), startLine, startCol};
    }
};

}  // namespace buzzdb
