cmake_minimum_required(VERSION 3.16)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Project setting
include(cmake/utils.cmake)
ai_pipe_extract_version()

project(AIPipe
    VERSION ${AI_PIPE_VERSION}
    LANGUAGES CXX
    DESCRIPTION "AI Pipeline framework for building modular AI applications"
)

set(AI_PIPE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(AI_PIPE_CMAKE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Build options
option(BUILD_SHARED_LIBS "Build ai_pipe as a shared library (OFF = static)" ON)
option(BUILD_AI_PIPE_TESTS "Build with tests" OFF)
option(AI_PIPE_WERROR "Treat ai_pipe warnings as errors" OFF)
option(BUILD_AI_PIPE_EXAMPLES "Build with examples" OFF)
option(BUILD_AI_PIPE_BENCHMARKS "Build with benchmark" OFF)
option(AI_PIPE_WITH_JSON
    "Enable the JSON graph loader (vendored header-only nlohmann/json; core stays dependency-free when OFF)" OFF)

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(AI_PIPE_FORMAT_CHECK_DEFAULT ON)
else()
    set(AI_PIPE_FORMAT_CHECK_DEFAULT OFF)
endif()
option(AI_PIPE_FORMAT_CHECK_ON_BUILD
    "Run clang-format validation as part of the default build"
    ${AI_PIPE_FORMAT_CHECK_DEFAULT})
unset(AI_PIPE_FORMAT_CHECK_DEFAULT)

include("${AI_PIPE_CMAKE_DIR}/CodeQuality.cmake")
ai_pipe_configure_code_quality()

# Sanitizer support (e.g. -DAI_PIPE_SANITIZER=address / thread / undefined)
set(AI_PIPE_SANITIZER "" CACHE STRING
    "Enable a sanitizer for all targets: address, thread, or undefined")
if(AI_PIPE_SANITIZER)
    if(MSVC)
        message(FATAL_ERROR "AI_PIPE_SANITIZER is only supported with GCC/Clang")
    endif()
    add_compile_options(-fsanitize=${AI_PIPE_SANITIZER} -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=${AI_PIPE_SANITIZER})
    message(STATUS "Sanitizer enabled: ${AI_PIPE_SANITIZER}")
endif()

# Display build configuration
message(STATUS "================================")
message(STATUS "Build AIPipe: ${AI_PIPE_VERSION}")
message(STATUS "Build with tests: ${BUILD_AI_PIPE_TESTS}")
message(STATUS "Build with examples: ${BUILD_AI_PIPE_EXAMPLES}")
message(STATUS "Build with benchmarks: ${BUILD_AI_PIPE_BENCHMARKS}")
message(STATUS "Build with JSON graph loader: ${AI_PIPE_WITH_JSON}")
message(STATUS "Format check on build: ${AI_PIPE_FORMAT_CHECK_ON_BUILD}")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")

if(MSVC)
    add_compile_options(/utf-8)
endif()

if(CMAKE_TOOLCHAIN_FILE)
    message(STATUS "CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
endif()

# Set default build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
        "Choose Release, Debug, RelWithDebInfo or MinSizeRel" FORCE)
endif()

if(NOT TARGET_OS)
    set(TARGET_OS ${CMAKE_SYSTEM_NAME})
endif()

if(NOT TARGET_ARCH)
    set(TARGET_ARCH ${CMAKE_SYSTEM_PROCESSOR})
endif()

message(STATUS "Target OS: ${TARGET_OS}")
message(STATUS "Target architecture: ${TARGET_ARCH}")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Load 3rdparty dependencies
message(STATUS "================================")
message(STATUS "Loading 3rdparty libraries...")
include(cmake/load_3rdparty.cmake)

# Configure output directories
set(PROJECT_OUTPUT_DIR ${PROJECT_BINARY_DIR}/${TARGET_ARCH})
set(PROJECT_INCLUDE_DIR ${PROJECT_OUTPUT_DIR}/include)

file(MAKE_DIRECTORY ${PROJECT_INCLUDE_DIR})
file(MAKE_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin)

message(STATUS "System architecture: ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "Output path: ${PROJECT_OUTPUT_DIR}")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_OUTPUT_DIR}/lib)

# Build subdirectories
message(STATUS "================================")
message(STATUS "Building 3rdparty libraries...")
add_subdirectory(3rdparty)

message(STATUS "================================")
message(STATUS "Building AIPipe library...")
add_subdirectory(src)
if(AI_PIPE_FORMAT_CHECK_ON_BUILD AND TARGET ai_pipe_format_check)
    add_dependencies(ai_pipe ai_pipe_format_check)
endif()

if(BUILD_AI_PIPE_TESTS)
    message(STATUS "================================")
    message(STATUS "Building tests...")
    add_subdirectory(tests)
endif()

if(BUILD_AI_PIPE_EXAMPLES)
    message(STATUS "================================")
    message(STATUS "Building examples...")
    add_subdirectory(examples)
endif()

if(BUILD_AI_PIPE_BENCHMARKS)
    message(STATUS "================================")
    message(STATUS "Building benchmarks...")
    add_subdirectory(benchmarks)
endif()

message(STATUS "================================")
message(STATUS "CMake configuration completed successfully!")
