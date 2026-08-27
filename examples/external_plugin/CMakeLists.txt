cmake_minimum_required(VERSION 3.16)

project(ai_pipe_external_plugin_example LANGUAGES CXX)

find_package(ai_pipe REQUIRED CONFIG)

ai_pipe_add_plugin(example_echo_node
    SOURCES echo_node_plugin.cpp
    OUTPUT_NAME ai_pipe_plugin_example_echo
)
