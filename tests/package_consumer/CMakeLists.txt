cmake_minimum_required(VERSION 3.18)
project(ai_pipe_package_consumer LANGUAGES CXX)

find_package(ai_pipe REQUIRED CONFIG COMPONENTS config)

add_executable(ai_pipe_package_consumer main.cpp)
target_compile_features(ai_pipe_package_consumer PRIVATE cxx_std_20)
target_link_libraries(ai_pipe_package_consumer PRIVATE
    ai_pipe::ai_pipe
    ai_pipe::config)

ai_pipe_add_plugin(package_echo
    SOURCES package_echo_plugin.cpp
    NO_INSTALL)
