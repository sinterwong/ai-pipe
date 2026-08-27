cmake_minimum_required(VERSION 3.16)

project(Tests)

# Test source files
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "*.cpp" "*.cc")
if(NOT TARGET ai_pipe::config)
    list(REMOVE_ITEM TEST_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/test_graph_loader.cpp")
endif()

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
if(TARGET ai_pipe::config)
    list(APPEND DEPENDENCY_LIBS ai_pipe::config)
endif()

# Configure test output directory
file(MAKE_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/tests)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin/tests)

# Test plugins (F8): a static Windows host cannot satisfy DLL imports from its
# executable, while POSIX hosts can export their symbols for this test setup.
set(AI_PIPE_CAN_TEST_PLUGINS FALSE)
if(UNIX OR BUILD_SHARED_LIBS)
    set(AI_PIPE_CAN_TEST_PLUGINS TRUE)
    add_subdirectory(plugins)
else()
    list(REMOVE_ITEM TEST_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/test_plugin_loader.cpp")
endif()

# Public headers must be self-contained: compile one TU per header so a
# header that includes a removed file or misses its own dependencies
# fails the build instead of rotting unnoticed.
file(GLOB PUBLIC_API_HEADERS CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/../include/ai_pipe/*.hpp"
)
set(HEADER_CHECK_SOURCES "")
foreach(header ${PUBLIC_API_HEADERS})
    get_filename_component(header_name ${header} NAME_WE)
    set(check_src "${CMAKE_CURRENT_BINARY_DIR}/header_checks/check_${header_name}.cpp")
    if(NOT EXISTS ${check_src})
        file(WRITE ${check_src} "#include \"ai_pipe/${header_name}.hpp\"\n")
    endif()
    list(APPEND HEADER_CHECK_SOURCES ${check_src})
endforeach()

add_library(ai_pipe_header_checks OBJECT ${HEADER_CHECK_SOURCES})
target_link_libraries(ai_pipe_header_checks PRIVATE ai_pipe::ai_pipe)
set_target_properties(ai_pipe_header_checks PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# Build test executable
add_executable(ai_pipe_tests ${TEST_SOURCES})

if(AI_PIPE_CAN_TEST_PLUGINS)
    # Build-tree fallback for locating the test plugins; when running
    # from the install tree the tests find them at ./tests/plugins.
    target_compile_definitions(ai_pipe_tests PRIVATE
        AI_PIPE_TEST_PLUGIN_DIR="${CMAKE_CURRENT_BINARY_DIR}/plugins"
        AI_PIPE_TEST_PLUGIN_FILENAME="$<TARGET_FILE_NAME:ai_pipe_test_plugin>"
        AI_PIPE_TEST_PLUGIN_NOENTRY_FILENAME="$<TARGET_FILE_NAME:ai_pipe_test_plugin_noentry>"
        AI_PIPE_TEST_PLUGIN_BADVERSION_FILENAME="$<TARGET_FILE_NAME:ai_pipe_test_plugin_badversion>"
        AI_PIPE_TEST_PLUGIN_REJECT_FILENAME="$<TARGET_FILE_NAME:ai_pipe_test_plugin_reject>"
    )
    add_dependencies(ai_pipe_tests
        ai_pipe_test_plugin
        ai_pipe_test_plugin_noentry
        ai_pipe_test_plugin_badversion
        ai_pipe_test_plugin_reject)
endif()

# Set target properties
set_target_properties(ai_pipe_tests PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    # Export the executable's dynamic symbols (-rdynamic). In a static
    # build the dlopen'ed test plugins do not link their own copy of the
    # core library, so they resolve ai_pipe symbols (NodeRegistry
    # singleton, node base classes) against this host at load time. Harmless
    # for shared builds. See tests/plugins/CMakeLists.txt.
    ENABLE_EXPORTS ON
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
