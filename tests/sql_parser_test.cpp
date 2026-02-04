/**
 * @file sql_parser_test.cpp
 * @brief Tests for the new recursive descent SQL parser.
 */

#include <iostream>
#include <cassert>
#include <filesystem>

#include "execution/sql_lexer.h"
#include "execution/sql_parser.h"
#include "execution/sql_planner.h"
#include "buffer/buffer_manager.h"

using namespace buzzdb;

void cleanup_test_file() {
    std::filesystem::remove(DATABASE_FILENAME);
}

// ============================================================================
// Lexer Tests
// ============================================================================

void test_lexer_simple() {
    std::cout << "Testing lexer simple tokens..." << std::endl;

    Lexer lexer("SELECT * FROM table1");
    auto tokens = lexer.tokenize();

    assert(tokens.size() == 5);  // SELECT, *, FROM, table1, END
    assert(tokens[0].type == TokenType::SELECT);
    assert(tokens[1].type == TokenType::STAR);
    assert(tokens[2].type == TokenType::FROM);
    assert(tokens[3].type == TokenType::IDENT);
    assert(tokens[3].asString() == "table1");
    assert(tokens[4].type == TokenType::END_OF_INPUT);

    std::cout << "  Lexer simple OK" << std::endl;
}

void test_lexer_lab4_syntax() {
    std::cout << "Testing lexer Lab 4 syntax..." << std::endl;

    Lexer lexer("SELECT {*} FROM {STUDENTS} WHERE {3} > 25");
    auto tokens = lexer.tokenize();

    assert(tokens[0].type == TokenType::SELECT);
    assert(tokens[1].type == TokenType::STAR);  // {*} -> STAR
    assert(tokens[2].type == TokenType::FROM);
    assert(tokens[3].type == TokenType::IDENT);  // {STUDENTS} -> IDENT
    assert(tokens[3].asString() == "STUDENTS");
    assert(tokens[4].type == TokenType::WHERE);
    assert(tokens[5].type == TokenType::COLUMN_REF);  // {3} -> COLUMN_REF
    assert(tokens[5].asInt() == 3);
    assert(tokens[6].type == TokenType::GT);
    assert(tokens[7].type == TokenType::INT_LIT);
    assert(tokens[7].asInt() == 25);

    std::cout << "  Lexer Lab 4 syntax OK" << std::endl;
}

void test_lexer_comparisons() {
    std::cout << "Testing lexer comparison operators..." << std::endl;

    Lexer lexer("a = b < c > d <= e >= f != g <> h");
    auto tokens = lexer.tokenize();

    assert(tokens[1].type == TokenType::EQ);
    assert(tokens[3].type == TokenType::LT);
    assert(tokens[5].type == TokenType::GT);
    assert(tokens[7].type == TokenType::LE);
    assert(tokens[9].type == TokenType::GE);
    assert(tokens[11].type == TokenType::NE);
    assert(tokens[13].type == TokenType::NE);

    std::cout << "  Lexer comparisons OK" << std::endl;
}

// ============================================================================
// Parser Tests
// ============================================================================

void test_parse_simple_select() {
    std::cout << "Testing parse simple SELECT..." << std::endl;

    auto stmt = parse("SELECT * FROM users");

    assert(stmt->fromTable.name == "users");
    assert(stmt->columns.size() == 1);
    assert(dynamic_cast<StarExpr*>(stmt->columns[0].get()) != nullptr);

    std::cout << "  Parse simple SELECT OK" << std::endl;
}

void test_parse_lab4_select() {
    std::cout << "Testing parse Lab 4 SELECT..." << std::endl;

    auto stmt = parse("SELECT {*} FROM {STUDENTS}");

    assert(stmt->fromTable.name == "STUDENTS");
    assert(stmt->columns.size() == 1);
    assert(dynamic_cast<StarExpr*>(stmt->columns[0].get()) != nullptr);

    std::cout << "  Parse Lab 4 SELECT OK" << std::endl;
}

void test_parse_where_clause() {
    std::cout << "Testing parse WHERE clause..." << std::endl;

    auto stmt = parse("SELECT {*} FROM {STUDENTS} WHERE {3} > 25 AND {3} < 50");

    assert(stmt->fromTable.name == "STUDENTS");
    assert(stmt->whereClause != nullptr);

    // Should be AND of two comparisons
    auto* andExpr = dynamic_cast<BinaryExpr*>(stmt->whereClause.get());
    assert(andExpr != nullptr);
    assert(andExpr->op == BinaryExpr::Op::AND);

    std::cout << "  Parse WHERE clause OK" << std::endl;
}

void test_parse_join() {
    std::cout << "Testing parse JOIN..." << std::endl;

    auto stmt = parse("SELECT {*} FROM {STUDENTS} JOIN {GRADES} ON {1} = {1}");

    assert(stmt->fromTable.name == "STUDENTS");
    assert(stmt->join.has_value());
    assert(stmt->join->table.name == "GRADES");

    // Join condition should be {1} = {1}
    auto* eqExpr = dynamic_cast<BinaryExpr*>(stmt->join->condition.get());
    assert(eqExpr != nullptr);
    assert(eqExpr->op == BinaryExpr::Op::EQ);

    std::cout << "  Parse JOIN OK" << std::endl;
}

void test_parse_aggregation() {
    std::cout << "Testing parse aggregation..." << std::endl;

    auto stmt = parse("SELECT {*} FROM {GRADES} SUM{3} GROUP BY {1}");

    assert(stmt->fromTable.name == "GRADES");
    assert(stmt->groupBy.size() == 1);

    // Check GROUP BY column
    auto* groupCol = dynamic_cast<ColumnExpr*>(stmt->groupBy[0].get());
    assert(groupCol != nullptr);
    assert(groupCol->index.has_value());
    assert(*groupCol->index == 1);

    // Check aggregate in SELECT
    assert(stmt->columns.size() == 1);
    auto* agg = dynamic_cast<AggregateExpr*>(stmt->columns[0].get());
    assert(agg != nullptr);
    assert(agg->aggType == AggregateType::SUM);

    std::cout << "  Parse aggregation OK" << std::endl;
}

// ============================================================================
// Integration Tests (with BufferManager)
// ============================================================================

void insertSampleData(BufferManager& bm) {
    InsertOperator inserter(bm);

    // STUDENTS: (student_id, name, semester, "STUDENTS")
    std::vector<std::tuple<int, std::string, int>> students = {
        {24002, "Xenokrates", 24},
        {26120, "Fichte", 26},
        {29555, "Feuerbach", 29},
        {28000, "Schopenhauer", 46},
        {24123, "Platon", 50},
        {25198, "Aristoteles", 50},
    };

    for (const auto& [id, name, semester] : students) {
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(id));
        tuple->addField(std::make_unique<Field>(name));
        tuple->addField(std::make_unique<Field>(semester));
        tuple->addField(std::make_unique<Field>(std::string("STUDENTS")));
        inserter.setTupleToInsert(std::move(tuple));
        inserter.next();
    }

    // GRADES: (student_id, course_id, grade, "GRADES")
    std::vector<std::tuple<int, int, int>> grades = {
        {24002, 5001, 1},
        {24002, 5041, 2},
        {26120, 5001, 2},
        {26120, 5041, 3},
        {29555, 5041, 2},
        {28000, 5022, 2},
        {24123, 5041, 1},
        {25198, 5022, 1},
    };

    for (const auto& [sid, cid, grade] : grades) {
        auto tuple = std::make_unique<Tuple>();
        tuple->addField(std::make_unique<Field>(sid));
        tuple->addField(std::make_unique<Field>(cid));
        tuple->addField(std::make_unique<Field>(grade));
        tuple->addField(std::make_unique<Field>(std::string("GRADES")));
        inserter.setTupleToInsert(std::move(tuple));
        inserter.next();
    }

    inserter.close();
}

void printResults(const std::vector<std::vector<std::unique_ptr<Field>>>& results) {
    std::cout << "Results (" << results.size() << " rows):\n";
    for (const auto& row : results) {
        std::cout << "  [";
        bool first = true;
        for (const auto& field : row) {
            if (!first) std::cout << ", ";
            std::cout << field->asString();
            first = false;
        }
        std::cout << "]\n";
    }
}

void test_execute_scan() {
    std::cout << "Testing execute scan..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        auto results = executeSQL("SELECT {*} FROM {STUDENTS}", bm);

        printResults(results);
        assert(results.size() == 6);
    }

    cleanup_test_file();
    std::cout << "  Execute scan OK" << std::endl;
}

void test_execute_where() {
    std::cout << "Testing execute WHERE..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        auto results = executeSQL(
            "SELECT {*} FROM {STUDENTS} WHERE {3} > 25 AND {3} < 50", bm);

        printResults(results);
        // Fichte(26), Feuerbach(29), Schopenhauer(46)
        assert(results.size() == 3);
    }

    cleanup_test_file();
    std::cout << "  Execute WHERE OK" << std::endl;
}

void test_execute_join() {
    std::cout << "Testing execute JOIN..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        auto results = executeSQL(
            "SELECT {*} FROM {STUDENTS} JOIN {GRADES} ON {1} = {1}", bm);

        printResults(results);
        assert(results.size() == 8);
    }

    cleanup_test_file();
    std::cout << "  Execute JOIN OK" << std::endl;
}

void test_execute_aggregation() {
    std::cout << "Testing execute aggregation..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        auto results = executeSQL(
            "SELECT {*} FROM {GRADES} SUM{3} GROUP BY {1}", bm);

        printResults(results);
        // 6 unique students
        assert(results.size() == 6);
    }

    cleanup_test_file();
    std::cout << "  Execute aggregation OK" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== SQL Parser Tests (New Implementation) ===" << std::endl;

    // Lexer tests
    test_lexer_simple();
    test_lexer_lab4_syntax();
    test_lexer_comparisons();

    // Parser tests
    test_parse_simple_select();
    test_parse_lab4_select();
    test_parse_where_clause();
    test_parse_join();
    test_parse_aggregation();

    // Integration tests
    test_execute_scan();
    test_execute_where();
    test_execute_join();
    test_execute_aggregation();

    std::cout << "=== All SQL Parser tests passed ===" << std::endl;
    return 0;
}
