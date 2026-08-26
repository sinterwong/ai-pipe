cmake_minimum_required(VERSION 3.16)

project(ai_pipe_external_plugin_example LANGUAGES CXX)

find_package(ai_pipe REQUIRED CONFIG)

get_target_property(AI_PIPE_LIBRARY_TYPE ai_pipe::ai_pipe TYPE)
if(AI_PIPE_LIBRARY_TYPE STREQUAL "STATIC_LIBRARY")
    message(FATAL_ERROR
        "The external plugin must use a shared ai_pipe library so the host "
        "and plugin share one NodeRegistry instance.")
endif()

add_library(example_echo_node MODULE echo_node_plugin.cpp)
target_link_libraries(example_echo_node PRIVATE ai_pipe::ai_pipe)
target_compile_features(example_echo_node PRIVATE cxx_std_20)

set_target_properties(example_echo_node PROPERTIES
    OUTPUT_NAME ai_pipe_plugin_example_echo
)

# GCC's unique symbols can keep a plugin mapped after `dlclose`, preventing its
# static registration from running when the library is loaded again.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(example_echo_node PRIVATE -fno-gnu-unique)
endif()
