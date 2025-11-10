cmake_minimum_required(VERSION 3.16)

project(AIPipe3rdparty)

# Create include directory if it doesn't exist
file(MAKE_DIRECTORY ${PROJECT_INCLUDE_DIR})

# Build logger library
if(NOT TARGET logger)
    message(STATUS "Adding logger subdirectory...")
    add_subdirectory(logger EXCLUDE_FROM_ALL)
else()
    message(STATUS "logger target already exists. Skipping.")
endif()

# Build Google Test library (for tests)
set(OLD_BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS}")
set(BUILD_SHARED_LIBS OFF)

if(BUILD_AI_PIPE_TESTS)
    if(NOT TARGET gtest)
        message(STATUS "Adding googletest subdirectory...")
        add_subdirectory(googletest EXCLUDE_FROM_ALL)
    else()
        message(STATUS "gtest target already exists. Skipping.")
    endif()
endif()

# Build Google Benchmark library (for benchmarks)
if(BUILD_AI_PIPE_BENCHMARKS)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Disable test for benchmark" FORCE)
    if(NOT TARGET benchmark)
        message(STATUS "Adding benchmark subdirectory...")
        add_subdirectory(benchmark EXCLUDE_FROM_ALL)
    else()
        message(STATUS "benchmark target already exists. Skipping.")
    endif()
endif()

set(BUILD_SHARED_LIBS "${OLD_BUILD_SHARED_LIBS}")

# Copy nlohmann json header library for tests and examples
if(BUILD_AI_PIPE_TESTS OR BUILD_AI_PIPE_EXAMPLES)
    file(COPY nlohmann DESTINATION ${PROJECT_INCLUDE_DIR})
    message(STATUS "nlohmann json headers copied to ${PROJECT_INCLUDE_DIR}")
endif()
