# BuzzDB

**An educational relational database engine built from scratch in C++17.**

---

## What Is This?

This is a rewrite of my lab submissions to Gatech CS6422 Database Implementation class. Significant refactoring and modifications has been conducted to integrate multiple stand-alone pieces into a unified, working database system capable of processing SQL queries.

The project covers:

- **Buffer pool management** with 2Q page replacement
- **Slotted page storage** for variable-length records
- **Query execution** using the iterator model
- **Hash join and aggregation** operators
- **A recursive descent SQL parser**

---

## Features

### Storage Engine
- Slotted pages with variable-length tuple support
- Automatic storage extension on demand
- Data persists across restarts
- 4KB page size (configurable)

### Buffer Management
- 2Q replacement policy (FIFO + LRU hybrid)
- Pin counting and reference management
- Shared/exclusive page locking
- Dirty page tracking and flushing

### Query Execution
- Operator pipeline: Scan → Select → Join → Aggregate → Project
- AND/OR predicate expressions
- Hash join for equi-joins
- Hash aggregation with SUM, COUNT, MIN, MAX
- Relation filtering for multi-table storage

### SQL Support
- `SELECT * FROM table`
- `SELECT columns FROM table WHERE conditions`
- `SELECT * FROM t1 JOIN t2 ON t1.col = t2.col`
- `SELECT col, SUM(val) FROM table GROUP BY col`
- WHERE clauses with AND/OR

### Performance (Approximate)
| Operation | Scale | Time |
|-----------|-------|------|
| Insert | 10,000 rows | ~8 sec |
| Full scan | 10,000 rows | ~25ms |
| Hash join | 1,000 × 1,000 | ~7ms |
| Aggregation | 10,000 → 100 groups | <1ms |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        SQL Query                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Lexer → Parser → AST → Planner → Operator Tree             │
│  (Recursive descent parser)                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Query Execution Engine (Volcano Model)         │
│  ┌──────┐   ┌────────┐   ┌──────┐   ┌───────────┐          │
│  │ Scan │ → │ Select │ → │ Join │ → │ Aggregate │          │
│  └──────┘   └────────┘   └──────┘   └───────────┘          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Buffer Manager                           │
│         2Q Policy │ Page Locking │ Dirty Tracking           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Manager                          │
│              Slotted Pages │ Variable-Length Tuples         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                        [ Disk File ]
```

---

## Project Structure

```
buzzdb/
├── src/
│   ├── common/           # Core types and configuration
│   │   ├── config.h      # System-wide constants
│   │   └── types.h       # Type definitions (PageID, FrameID, etc.)
│   │
│   ├── storage/          # Persistent storage layer
│   │   ├── field.h       # Field type (int/string polymorphism)
│   │   ├── tuple.h       # Variable-length tuple container
│   │   ├── slotted_page.h    # Page format with slot directory
│   │   └── storage_manager.h # Disk I/O abstraction
│   │
│   ├── buffer/           # Buffer pool management
│   │   ├── buffer_frame.h    # In-memory page wrapper
│   │   ├── policy.h          # Replacement policy interface
│   │   ├── two_q_policy.h    # 2Q implementation
│   │   └── buffer_manager.h  # Central buffer pool
│   │
│   └── execution/        # Query processing
│       ├── operator.h        # Base operator classes
│       ├── predicate.h       # WHERE clause predicates
│       ├── scan_operator.h   # Table scan
│       ├── select_operator.h # Filtering
│       ├── project_operator.h # Column projection
│       ├── hash_join_operator.h      # Equi-join
│       ├── hash_aggregation_operator.h # GROUP BY
│       ├── operators.h       # Convenience include
│       ├── sql_lexer.h       # Tokenizer
│       ├── sql_ast.h         # Abstract syntax tree
│       ├── sql_parser.h      # Recursive descent parser
│       └── sql_planner.h     # AST to operator tree
│
└── tests/                # Comprehensive test suite
    ├── common_test.cpp
    ├── field_test.cpp
    ├── tuple_test.cpp
    ├── slotted_page_test.cpp
    ├── storage_manager_test.cpp
    ├── policy_test.cpp
    ├── buffer_manager_test.cpp
    ├── operator_test.cpp
    ├── query_parser_test.cpp
    ├── sql_parser_test.cpp
    └── integration_test.cpp   # Stress tests & limits
```

---

## Building & Testing

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
ctest --output-on-failure
```

All 12 tests should pass.

---

## Design Notes

### Hybrid Type System for AST
The SQL parser uses `std::variant` for flat types (tokens, literals) and inheritance for recursive types (expression trees). This avoids awkward `unique_ptr` wrappers in variants while still getting exhaustive pattern matching where it helps.

### 2Q Replacement Policy
The buffer manager uses 2Q instead of plain LRU:
- Pages accessed once go to a FIFO queue (evicted first)
- Pages accessed again move to an LRU queue (kept longer)

This prevents sequential scans from evicting frequently-used pages.

### Volcano Iterator Model
Every operator implements `open()` / `next()` / `close()` / `getOutput()`. This allows operators to be composed freely and processes one tuple at a time without materializing full results.

---

## Limitations & Honest Assessment

This is an educational project. It demonstrates understanding of database concepts, not production readiness.

### ACID Compliance

| Property | Status | What's Missing |
|----------|--------|----------------|
| **Atomicity** | Not Supported | No transactions, no rollback |
| **Consistency** | Minimal | No schema catalog, no constraints |
| **Isolation** | Read Uncommitted | Page locks only, dirty reads possible |
| **Durability** | Partial | No WAL, no crash recovery |

### Feature Limitations

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT, FROM, WHERE | Supported | Integer comparisons only |
| JOIN | Single join only | No multi-way joins |
| GROUP BY + aggregates | Supported | SUM, COUNT, MIN, MAX |
| ORDER BY, LIMIT | Parsed only | Not executed |
| UPDATE, DELETE | Not supported | Append-only storage |
| Indexes | Not integrated | B+ tree exists but not wired up |
| Subqueries | Not supported | Single-level queries only |
| String comparisons | Not supported | Use integer keys |

### Known Issues

1. **No crash recovery**: If the process crashes mid-operation, data may be lost or corrupted. There's no write-ahead logging.

2. **No schema enforcement**: Any tuple structure is accepted. The application must ensure consistency.

### What Would Make It Production-Ready

1. **Write-Ahead Logging (WAL)** - For crash recovery and atomicity
2. **MVCC** - For proper transaction isolation
3. **Schema Catalog** - For constraint enforcement
4. **Query Optimizer** - For cost-based plan selection
5. **Index Integration** - B+ tree is implemented, needs wiring
6. **Proper Lock Manager** - Replace spinning with condition variables

---

## Background

This codebase started as three separate lab assignments:

- **Lab 2**: Buffer management and page replacement
- **Lab 3**: B+ tree indexing (implemented, not yet integrated)
- **Lab 4**: Query execution operators

The labs were refactored into a modular structure with each component in its own header file, tested independently, and integrated with CMake. The SQL parser was rewritten using recursive descent to replace the original regex-based approach. Concurrent read/write access is supported with proper synchronization.

---

## Running Example Queries

```cpp
#include "execution/sql_planner.h"
#include "buffer/buffer_manager.h"

using namespace buzzdb;

int main() {
    BufferManager bm(100, true);

    // Insert some data...

    // Query it!
    auto results = executeSQL(
        "SELECT {*} FROM {STUDENTS} WHERE {3} > 25 AND {3} < 50",
        bm
    );

    for (const auto& row : results) {
        for (const auto& field : row) {
            std::cout << field->asString() << " ";
        }
        std::cout << "\n";
    }
}
```

---
