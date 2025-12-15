cmake_minimum_required(VERSION 3.16)

project(Tests)

# Test source files
set(TEST_SOURCES
    main.cc
    test_through_pass.cpp
)

# Prefix paths
list(TRANSFORM TEST_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")

# Dependency configuration
set(DEPENDENCY_INCLUDES
    ${OpenCV_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${CMAKE_CURRENT_SOURCE_DIR}/../examples
    ${PROJECT_INCLUDE_DIR}
)

set(DEPENDENCY_LIBS
    ai_pipe::ai_pipe_examples
    ai_pipe::ai_pipe
    GTest::gtest
    GTest::gtest_main
    ${OpenCV_LIBS}
)

# Configure test output directory
file(MAKE_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/tests)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/tests)

# Build test executable
add_executable(ai_pipe_tests ${TEST_SOURCES})

# Set target properties
set_target_properties(ai_pipe_tests PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Configure include directories
target_include_directories(ai_pipe_tests PRIVATE ${DEPENDENCY_INCLUDES})

# Link dependencies
target_link_libraries(ai_pipe_tests PRIVATE ${DEPENDENCY_LIBS})

# Install test executable
install(TARGETS ai_pipe_tests
    DESTINATION tests
    COMPONENT test
)
