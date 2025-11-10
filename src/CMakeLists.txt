cmake_minimum_required(VERSION 3.16)

project(AIPipeSrc VERSION ${AI_PIPE_VERSION})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Public headers (API)
set(PUBLIC_HEADERS
    api/ai_pipe/ai_pipe_version.hpp
    api/ai_pipe/config.hpp
    api/ai_pipe/context.hpp
    api/ai_pipe/data_packet.hpp
    api/ai_pipe/edge.hpp
    api/ai_pipe/graph.hpp
    api/ai_pipe/node_base.hpp
    api/ai_pipe/pipeline.hpp
    api/ai_pipe/type_safe_factory.hpp
    api/ai_pipe/types.hpp
)

# Private headers (implementation)
set(PRIVATE_HEADERS
    default_execution_engine.hpp
    execution_engine_base.hpp
    execution_engine_factory.hpp
    pipeline_impl.hpp
    thread_pool.hpp
    thread_safe_queue.hpp
)

# Source files
set(SOURCE_FILES
    default_execution_engine.cpp
    graph.cpp
    pipeline.cpp
    pipeline_impl.cpp
)

# Prefix paths for organization
list(TRANSFORM PUBLIC_HEADERS PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
list(TRANSFORM PRIVATE_HEADERS PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
list(TRANSFORM SOURCE_FILES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")

# Dependency configuration
set(DEPENDENCY_INCLUDES
    ${PROJECT_INCLUDE_DIR}
    ${PROJECT_INCLUDE_DIR}/ai_pipe
)

set(DEPENDENCY_LIBS
    logger::logger
)

# Build shared library
add_library(ai_pipe SHARED ${SOURCE_FILES})

add_library(ai_pipe::ai_pipe ALIAS ai_pipe)

# Set target properties
set_target_properties(ai_pipe PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${AI_PIPE_VERSION_MAJOR}
)

# Configure include directories
target_include_directories(ai_pipe
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/api>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/api>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/api/ai_pipe>
        ${DEPENDENCY_INCLUDES}
)

# Link dependencies
target_link_libraries(ai_pipe PRIVATE ${DEPENDENCY_LIBS})

# Generate package configuration files
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ai_pipe-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-config.cmake"
    @ONLY
)

# Install public headers
install(FILES ${PUBLIC_HEADERS}
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ai_pipe
    COMPONENT dev
)

# Install library
install(TARGETS ai_pipe
    EXPORT ai_pipe_export_set
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT lib
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT lib
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT bin
)

# Install package configuration files
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-config-version.cmake"
    DESTINATION ${CMAKE_INSTALL_DATADIR}
    COMPONENT dev
)

# Install export set
install(EXPORT ai_pipe_export_set
    FILE ai_pipe-targets.cmake
    NAMESPACE ai_pipe::
    DESTINATION ${CMAKE_INSTALL_DATADIR}
    COMPONENT dev
)

# Export for build tree
export(EXPORT ai_pipe_export_set
    FILE "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-targets.cmake"
    NAMESPACE ai_pipe::
)
