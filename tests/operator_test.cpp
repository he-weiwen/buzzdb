/**
 * @file operator_test.cpp
 * @brief Tests for query execution operators.
 */

#include <iostream>
#include <sstream>
#include <cassert>
#include <filesystem>
#include <map>

#include "execution/operators.h"
#include "buffer/buffer_manager.h"

using namespace buzzdb;

void cleanup_test_file() {
    std::filesystem::remove(DATABASE_FILENAME);
}

// Helper to create test tuples
std::unique_ptr<Tuple> makeTuple(int id, const std::string& name, int value) {
    auto tuple = std::make_unique<Tuple>();
    tuple->addField(std::make_unique<Field>(id));
    tuple->addField(std::make_unique<Field>(name));
    tuple->addField(std::make_unique<Field>(value));
    return tuple;
}

// Helper to insert test data
void insertTestData(BufferManager& bm) {
    InsertOperator inserter(bm);

    inserter.setTupleToInsert(makeTuple(1, "Alice", 100));
    inserter.next();
    inserter.setTupleToInsert(makeTuple(2, "Bob", 200));
    inserter.next();
    inserter.setTupleToInsert(makeTuple(3, "Charlie", 150));
    inserter.next();
    inserter.setTupleToInsert(makeTuple(4, "Alice", 50));
    inserter.next();
    inserter.setTupleToInsert(makeTuple(5, "Bob", 300));
    inserter.next();

    inserter.close();
}

// ============================================================================
// Scan Tests
// ============================================================================

void test_scan_empty() {
    std::cout << "Testing scan on empty database..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        ScanOperator scan(bm);
        scan.open();

        assert(!scan.next());

        scan.close();
    }

    cleanup_test_file();
    std::cout << "  Scan empty OK" << std::endl;
}

void test_scan_all() {
    std::cout << "Testing scan all tuples..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);

        ScanOperator scan(bm);
        scan.open();

        int count = 0;
        while (scan.next()) {
            auto output = scan.getOutput();
            assert(output.size() == 3);
            count++;
        }

        assert(count == 5);

        scan.close();
    }

    cleanup_test_file();
    std::cout << "  Scan all OK" << std::endl;
}

// ============================================================================
// Print Tests
// ============================================================================

void test_print() {
    std::cout << "Testing print operator..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);

        ScanOperator scan(bm);
        std::ostringstream oss;
        PrintOperator print(scan, oss);

        print.open();
        while (print.next()) {
            // PrintOperator handles output
        }
        print.close();

        std::string output = oss.str();
        assert(output.find("Alice") != std::string::npos);
        assert(output.find("Bob") != std::string::npos);
        assert(output.find("Charlie") != std::string::npos);

        // Should have 5 lines
        int lines = 0;
        for (char c : output) {
            if (c == '\n') lines++;
        }
        assert(lines == 5);
    }

    cleanup_test_file();
    std::cout << "  Print OK" << std::endl;
}

// ============================================================================
// Select Tests
// ============================================================================

void test_select_simple() {
    std::cout << "Testing select with simple predicate..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);

        ScanOperator scan(bm);

        // Select where value > 150 (index 2)
        auto predicate = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(size_t(2)),  // column 2
            SimplePredicate::Operand(std::make_unique<Field>(150)),  // value
            SimplePredicate::ComparisonOperator::GT
        );

        SelectOperator select(scan, std::move(predicate));
        select.open();

        int count = 0;
        while (select.next()) {
            auto output = select.getOutput();
            // Should only get Bob (200) and Bob (300)
            assert(output[2]->asInt() > 150);
            count++;
        }

        assert(count == 2);

        select.close();
    }

    cleanup_test_file();
    std::cout << "  Select simple OK" << std::endl;
}

void test_select_complex() {
    std::cout << "Testing select with complex predicate..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);

        ScanOperator scan(bm);

        // Select where 100 <= value <= 200
        auto pred1 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(size_t(2)),
            SimplePredicate::Operand(std::make_unique<Field>(100)),
            SimplePredicate::ComparisonOperator::GE
        );
        auto pred2 = std::make_unique<SimplePredicate>(
            SimplePredicate::Operand(size_t(2)),
            SimplePredicate::Operand(std::make_unique<Field>(200)),
            SimplePredicate::ComparisonOperator::LE
        );

        auto complex = std::make_unique<ComplexPredicate>(
            ComplexPredicate::LogicOperator::AND);
        complex->addPredicate(std::move(pred1));
        complex->addPredicate(std::move(pred2));

        SelectOperator select(scan, std::move(complex));
        select.open();

        int count = 0;
        while (select.next()) {
            auto output = select.getOutput();
            int val = output[2]->asInt();
            assert(val >= 100 && val <= 200);
            count++;
        }

        // Alice(100), Bob(200), Charlie(150)
        assert(count == 3);

        select.close();
    }

    cleanup_test_file();
    std::cout << "  Select complex OK" << std::endl;
}

// ============================================================================
// Project Tests
// ============================================================================

void test_project() {
    std::cout << "Testing project operator..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);

        ScanOperator scan(bm);

        // Project only name (index 1)
        ProjectOperator project(scan, {1});
        project.open();

        int count = 0;
        while (project.next()) {
            auto output = project.getOutput();
            assert(output.size() == 1);
            // Should be a string (name)
            std::string name = output[0]->asString();
            assert(name == "Alice" || name == "Bob" || name == "Charlie");
            count++;
        }

        assert(count == 5);

        project.close();
    }

    cleanup_test_file();
    std::cout << "  Project OK" << std::endl;
}

// ============================================================================
// Hash Join Tests
// ============================================================================

void test_hash_join() {
    std::cout << "Testing hash join operator..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        // Insert "left" table: (id, name)
        InsertOperator inserter(bm);
        auto t1 = std::make_unique<Tuple>();
        t1->addField(std::make_unique<Field>(1));
        t1->addField(std::make_unique<Field>(std::string("A")));
        t1->addField(std::make_unique<Field>(std::string("LEFT")));
        inserter.setTupleToInsert(std::move(t1));
        inserter.next();

        auto t2 = std::make_unique<Tuple>();
        t2->addField(std::make_unique<Field>(2));
        t2->addField(std::make_unique<Field>(std::string("B")));
        t2->addField(std::make_unique<Field>(std::string("LEFT")));
        inserter.setTupleToInsert(std::move(t2));
        inserter.next();

        // Insert "right" table: (id, value)
        auto t3 = std::make_unique<Tuple>();
        t3->addField(std::make_unique<Field>(1));
        t3->addField(std::make_unique<Field>(100));
        t3->addField(std::make_unique<Field>(std::string("RIGHT")));
        inserter.setTupleToInsert(std::move(t3));
        inserter.next();

        auto t4 = std::make_unique<Tuple>();
        t4->addField(std::make_unique<Field>(1));
        t4->addField(std::make_unique<Field>(200));
        t4->addField(std::make_unique<Field>(std::string("RIGHT")));
        inserter.setTupleToInsert(std::move(t4));
        inserter.next();

        inserter.close();

        // Join on id (column 0)
        ScanOperator left_scan(bm, "LEFT");
        ScanOperator right_scan(bm, "RIGHT");

        HashJoinOperator join(left_scan, right_scan, 0, 0);
        join.open();

        int count = 0;
        while (join.next()) {
            auto output = join.getOutput();
            // Left(id, name) + Right(id, value) = 4 fields
            // But relation field was removed by scan, so we get original fields
            count++;
        }

        // id=1 from left matches two rows from right
        assert(count == 2);

        join.close();
    }

    cleanup_test_file();
    std::cout << "  Hash join OK" << std::endl;
}

// ============================================================================
// Hash Aggregation Tests
// ============================================================================

void test_hash_aggregation_sum() {
    std::cout << "Testing hash aggregation (SUM)..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);  // Alice:100, Bob:200, Charlie:150, Alice:50, Bob:300

        ScanOperator scan(bm);

        // GROUP BY name (index 1), SUM(value) (index 2)
        HashAggregationOperator agg(
            scan,
            {1},  // group by name
            {{AggrFuncType::SUM, 2}}  // sum of value
        );

        agg.open();

        std::map<std::string, int> results;
        while (agg.next()) {
            auto output = agg.getOutput();
            // Output: name, sum
            std::string name = output[0]->asString();
            int sum = output[1]->asInt();
            results[name] = sum;
        }

        // Alice: 100 + 50 = 150
        // Bob: 200 + 300 = 500
        // Charlie: 150
        assert(results["Alice"] == 150);
        assert(results["Bob"] == 500);
        assert(results["Charlie"] == 150);

        agg.close();
    }

    cleanup_test_file();
    std::cout << "  Hash aggregation SUM OK" << std::endl;
}

void test_hash_aggregation_count() {
    std::cout << "Testing hash aggregation (COUNT)..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);
        insertTestData(bm);

        ScanOperator scan(bm);

        // GROUP BY name (index 1), COUNT
        HashAggregationOperator agg(
            scan,
            {1},  // group by name
            {{AggrFuncType::COUNT, 0}}  // count (attr_index ignored for COUNT)
        );

        agg.open();

        std::map<std::string, int> results;
        while (agg.next()) {
            auto output = agg.getOutput();
            std::string name = output[0]->asString();
            int count = output[1]->asInt();
            results[name] = count;
        }

        // Alice: 2, Bob: 2, Charlie: 1
        assert(results["Alice"] == 2);
        assert(results["Bob"] == 2);
        assert(results["Charlie"] == 1);

        agg.close();
    }

    cleanup_test_file();
    std::cout << "  Hash aggregation COUNT OK" << std::endl;
}

// ============================================================================
// Insert Tests
// ============================================================================

void test_insert() {
    std::cout << "Testing insert operator..." << std::endl;

    cleanup_test_file();

    {
        BufferManager bm(10, true);

        InsertOperator inserter(bm);

        // Insert 100 tuples
        for (int i = 0; i < 100; i++) {
            inserter.setTupleToInsert(makeTuple(i, "Test", i * 10));
            bool success = inserter.next();
            assert(success);
        }
        inserter.close();

        // Verify by scanning
        ScanOperator scan(bm);
        scan.open();

        int count = 0;
        while (scan.next()) {
            scan.getOutput();
            count++;
        }

        assert(count == 100);

        scan.close();
    }

    cleanup_test_file();
    std::cout << "  Insert OK" << std::endl;
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Operator Tests ===" << std::endl;

    // Scan tests
    test_scan_empty();
    test_scan_all();

    // Print test
    test_print();

    // Select tests
    test_select_simple();
    test_select_complex();

    // Project test
    test_project();

    // Join test
    test_hash_join();

    // Aggregation tests
    test_hash_aggregation_sum();
    test_hash_aggregation_count();

    // Insert test
    test_insert();

    std::cout << "=== All Operator tests passed ===" << std::endl;
    return 0;
}
