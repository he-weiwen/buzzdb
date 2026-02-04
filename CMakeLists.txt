cmake_minimum_required(VERSION 3.14)
project(buzzdb
    VERSION 0.1.0
    DESCRIPTION "A simple database engine for learning"
    LANGUAGES CXX
)

# Require C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Header-only library interface
add_library(buzzdb INTERFACE)
target_include_directories(buzzdb INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)

# Threading support (required for buffer manager)
find_package(Threads REQUIRED)
target_link_libraries(buzzdb INTERFACE Threads::Threads)

# Enable testing
enable_testing()

# Test executable helper function
function(add_buzzdb_test TEST_NAME)
    add_executable(${TEST_NAME} tests/${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} PRIVATE buzzdb)
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

# Phase 1: Common types test
add_buzzdb_test(common_test)

# Phase 2: Storage core types
add_buzzdb_test(field_test)
add_buzzdb_test(tuple_test)

# Phase 2.5: Variant-based Field (proposed replacement)
add_buzzdb_test(field_variant_test)

# Phase 3: Page management and storage
add_buzzdb_test(slotted_page_test)
add_buzzdb_test(storage_manager_test)

# Phase 4: Buffer management
add_buzzdb_test(policy_test)
add_buzzdb_test(buffer_manager_test)

# Phase 5-6: Query execution
add_buzzdb_test(operator_test)

# Phase 7: Query parsing and integration
add_buzzdb_test(query_parser_test)
add_buzzdb_test(sql_parser_test)

# Phase 8: Integration and stress tests
add_buzzdb_test(integration_test)
