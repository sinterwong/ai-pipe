cmake_minimum_required(VERSION 3.16)

project(Tests)

# Test source files
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "*.cpp" "*.cc")

# Dependency configuration
set(DEPENDENCY_INCLUDES
    ${OpenCV_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${CMAKE_CURRENT_SOURCE_DIR}/../examples
    ${PROJECT_INCLUDE_DIR}
)

set(DEPENDENCY_LIBS
    ai_pipe::ai_pipe
    GTest::gtest
    GTest::gtest_main
    ${OpenCV_LIBS}
)

# Configure test output directory
file(MAKE_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/tests)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/tests)

# Test plugins (F8): built into plugins/, dlopen'ed by the loader tests
if(UNIX)
    add_subdirectory(plugins)
endif()

# Build test executable
add_executable(ai_pipe_tests ${TEST_SOURCES})

if(UNIX)
    # Build-tree fallback for locating the test plugins; when running
    # from the install tree the tests find them at ./tests/plugins.
    target_compile_definitions(ai_pipe_tests PRIVATE
        AI_PIPE_TEST_PLUGIN_DIR="${CMAKE_CURRENT_BINARY_DIR}/plugins"
    )
endif()

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
