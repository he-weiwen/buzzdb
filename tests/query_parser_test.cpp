/**
 * @file query_parser_test.cpp
 * @brief Tests for the query parser and executor.
 *
 * Uses sample data similar to Lab 4 tests.
 */

#include <iostream>
#include <cassert>
#include <filesystem>

#include "execution/query_parser.h"
#include "buffer/buffer_manager.h"

using namespace buzzdb;

void cleanup_test_file() {
    std::filesystem::remove(DATABASE_FILENAME);
}

// Helper to create a tuple with relation tag
std::unique_ptr<Tuple> makeTaggedTuple(const std::vector<int>& ints,
                                        const std::vector<std::string>& strings,
                                        const std::string& relation) {
    auto tuple = std::make_unique<Tuple>();

    // Add int fields
    for (int val : ints) {
        tuple->addField(std::make_unique<Field>(val));
    }

    // Add string fields
    for (const auto& s : strings) {
        tuple->addField(std::make_unique<Field>(s));
    }

    // Add relation tag as last field
    tuple->addField(std::make_unique<Field>(relation));

    return tuple;
}

// Insert sample data similar to Lab 4
void insertSampleData(BufferManager& bm) {
    InsertOperator inserter(bm);

    // STUDENTS table: (id:int, name:string, age:int, "STUDENTS")
    // Stored as: id, age, name, relation_tag (to match Lab 4 format)

    // Actually, let's use a simpler format:
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

    // GRADES table: (student_id, course_id, grade, "GRADES")
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

// Print results for debugging
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

// ============================================================================
// Parser Tests
// ============================================================================

void test_parse_simple_select() {
    std::cout << "Testing parse simple SELECT..." << std::endl;

    std::string query = "SELECT {1}, {2} FROM {STUDENTS}";
    auto components = parseQuery(query);

    assert(components.relation == "STUDENTS");
    assert(components.selectAttributes.size() == 2);
    assert(components.selectAttributes[0] == 0);
    assert(components.selectAttributes[1] == 1);
    assert(!components.innerJoin);
    assert(!components.whereCondition);
    assert(!components.sumOperation);

    std::cout << "  Parse simple SELECT OK" << std::endl;
}

void test_parse_with_where() {
    std::cout << "Testing parse with WHERE..." << std::endl;

    std::string query = "SELECT {*} FROM {STUDENTS} WHERE {3} > 25 and {3} < 50";
    auto components = parseQuery(query);

    assert(components.relation == "STUDENTS");
    assert(components.whereCondition);
    assert(components.whereAttributeIndex == 2);  // {3} -> index 2
    assert(components.lowerBound == 25);
    assert(components.upperBound == 50);

    std::cout << "  Parse with WHERE OK" << std::endl;
}

void test_parse_with_join() {
    std::cout << "Testing parse with JOIN..." << std::endl;

    std::string query = "SELECT {*} FROM {STUDENTS} JOIN {GRADES} ON {1} = {1}";
    auto components = parseQuery(query);

    assert(components.relation == "STUDENTS");
    assert(components.innerJoin);
    assert(components.joinRelation == "GRADES");
    assert(components.joinAttributeIndex1 == 0);
    assert(components.joinAttributeIndex2 == 0);

    std::cout << "  Parse with JOIN OK" << std::endl;
}

void test_parse_with_aggregation() {
    std::cout << "Testing parse with aggregation..." << std::endl;

    std::string query = "SELECT {*} FROM {GRADES} SUM{3} GROUP BY {1}";
    auto components = parseQuery(query);

    assert(components.relation == "GRADES");
    assert(components.sumOperation);
    assert(components.sumAttributeIndex == 2);  // {3} -> index 2
    assert(components.groupBy);
    assert(components.groupByAttributeIndex == 0);  // {1} -> index 0

    std::cout << "  Parse with aggregation OK" << std::endl;
}

// ============================================================================
// Execution Tests
// ============================================================================

void test_execute_scan_all() {
    std::cout << "Testing execute scan all STUDENTS..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        std::string query = "SELECT {*} FROM {STUDENTS}";
        auto components = parseQuery(query);
        auto results = executeQuery(components, bm);

        // Should have 6 students
        assert(results.size() == 6);

        printResults(results);
    }

    cleanup_test_file();
    std::cout << "  Execute scan all STUDENTS OK" << std::endl;
}

void test_execute_with_where() {
    std::cout << "Testing execute with WHERE..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        // Select students with semester > 25 and < 50
        std::string query = "SELECT {*} FROM {STUDENTS} WHERE {3} > 25 and {3} < 50";
        auto components = parseQuery(query);
        prettyPrint(components);

        auto results = executeQuery(components, bm);

        // Should match: Fichte(26), Feuerbach(29), Schopenhauer(46)
        std::cout << "  Filtered students (25 < semester < 50):\n";
        printResults(results);

        assert(results.size() == 3);
    }

    cleanup_test_file();
    std::cout << "  Execute with WHERE OK" << std::endl;
}

void test_execute_with_join() {
    std::cout << "Testing execute with JOIN..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        // Join students with grades on student_id
        std::string query = "SELECT {*} FROM {STUDENTS} JOIN {GRADES} ON {1} = {1}";
        auto components = parseQuery(query);
        prettyPrint(components);

        auto results = executeQuery(components, bm);

        // Each grade row joins with its student
        // We have 8 grades, each should match one student
        std::cout << "  Joined results:\n";
        printResults(results);

        assert(results.size() == 8);
    }

    cleanup_test_file();
    std::cout << "  Execute with JOIN OK" << std::endl;
}

void test_execute_sum_grades() {
    std::cout << "Testing execute SUM grades by student..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertSampleData(bm);

        // Sum grades per student
        std::string query = "SELECT {*} FROM {GRADES} SUM{3} GROUP BY {1}";
        auto components = parseQuery(query);
        prettyPrint(components);

        auto results = executeQuery(components, bm);

        // Results should be: student_id, sum_of_grades
        std::cout << "  Sum of grades by student:\n";
        printResults(results);

        // 24002: 1+2=3, 26120: 2+3=5, 29555: 2, 28000: 2, 24123: 1, 25198: 1
        assert(results.size() == 6);  // 6 unique students with grades
    }

    cleanup_test_file();
    std::cout << "  Execute SUM grades OK" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Query Parser Tests ===" << std::endl;

    // Parser tests
    test_parse_simple_select();
    test_parse_with_where();
    test_parse_with_join();
    test_parse_with_aggregation();

    // Execution tests
    test_execute_scan_all();
    test_execute_with_where();
    test_execute_with_join();
    test_execute_sum_grades();

    std::cout << "=== All Query Parser tests passed ===" << std::endl;
    return 0;
}
