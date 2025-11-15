cmake_minimum_required(VERSION 3.16)

project(Benchmarks)

# Benchmark source files
set(BENCHMARK_SOURCES
    main.cc
    through_pass.cpp
)

# Prefix paths
list(TRANSFORM BENCHMARK_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")

# Dependency configuration
set(DEPENDENCY_INCLUDES
    ${OpenCV_INCLUDE_DIRS}
    ${PROJECT_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/api
    ${CMAKE_SOURCE_DIR}/src/api/ai_pipe
)

set(DEPENDENCY_LIBS
    ai_pipe::ai_pipe
    logger::logger
    benchmark::benchmark
)

# Configure benchmark output directory
file(MAKE_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/benchmarks)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/benchmarks)

# Build benchmark executable
add_executable(ai_pipe_benchmarks ${BENCHMARK_SOURCES})

# Set target properties
set_target_properties(ai_pipe_benchmarks PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Configure include directories
target_include_directories(ai_pipe_benchmarks PRIVATE ${DEPENDENCY_INCLUDES})

# Link dependencies
target_link_libraries(ai_pipe_benchmarks PRIVATE ${DEPENDENCY_LIBS})

# Install benchmark executable
install(TARGETS ai_pipe_benchmarks
    DESTINATION benchmarks
    COMPONENT benchmark
)
