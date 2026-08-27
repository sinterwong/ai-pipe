set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Public API headers live outside the implementation tree so consumers cannot
# accidentally depend on private headers.
set(AI_PIPE_PUBLIC_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}/../include")
file(GLOB_RECURSE AI_PIPE_PUBLIC_HEADERS CONFIGURE_DEPENDS
    "${AI_PIPE_PUBLIC_INCLUDE_DIR}/ai_pipe/*.hpp"
)

set(AI_PIPE_SOURCES
    context.cpp
    engine_log.cpp
    execution_engine_impl.cpp
    graph.cpp
    logger.cpp
    node_registry.cpp
    pipeline_impl.cpp
    plugin_loader.cpp
    strategies.cpp
)

set(AI_PIPE_PRIVATE_HEADERS
    execution_engine_impl.hpp
    frame_alignment.hpp
    join_aware_sync_strategy.hpp
    lock_free_queue.hpp
    logger.hpp
    pipeline_impl.hpp
    scheduler_strategies.hpp
    sync_coordinator.hpp
    sync_strategies.hpp
    work_stealing_thread_pool.hpp
)

# Build shared library
add_library(ai_pipe 
    ${AI_PIPE_SOURCES}
    ${AI_PIPE_PRIVATE_HEADERS}
    ${AI_PIPE_PUBLIC_HEADERS}
)

add_library(ai_pipe::ai_pipe ALIAS ai_pipe)

# Warnings: apply to our code only (never to 3rdparty)
if(MSVC)
    target_compile_options(ai_pipe PRIVATE /W4)
    if(AI_PIPE_WERROR)
        target_compile_options(ai_pipe PRIVATE /WX)
    endif()
else()
    target_compile_options(ai_pipe PRIVATE -Wall -Wextra -Wpedantic)
    if(AI_PIPE_WERROR)
        target_compile_options(ai_pipe PRIVATE -Werror)
    endif()
endif()

# Set target properties
set_target_properties(ai_pipe PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${AI_PIPE_VERSION_MAJOR}
)

# Consumers see the same `ai_pipe/...` paths in the build and install trees.
target_include_directories(ai_pipe
    PUBLIC
        $<BUILD_INTERFACE:${AI_PIPE_PUBLIC_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link dependencies (CMAKE_DL_LIBS: dlopen for the plugin loader)
target_link_libraries(ai_pipe PRIVATE ${CMAKE_DL_LIBS})

# Generate package configuration files. The config/targets files go to
# lib/cmake/ai_pipe, which find_package() searches by default (a flat
# share/ needed a manual ai_pipe_DIR). configure_package_config_file is
# required for @PACKAGE_INIT@ to expand; plain configure_file leaves it
# empty.
set(AI_PIPE_CMAKE_INSTALL_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/ai_pipe)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/ai_pipe-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-config.cmake"
    INSTALL_DESTINATION ${AI_PIPE_CMAKE_INSTALL_DIR}
)

# Install public headers
install(FILES ${AI_PIPE_PUBLIC_HEADERS}
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
    "${PROJECT_SOURCE_DIR}/cmake/AIPipePlugin.cmake"
    DESTINATION ${AI_PIPE_CMAKE_INSTALL_DIR}
    COMPONENT dev
)

# Install export set
install(EXPORT ai_pipe_export_set
    FILE ai_pipe-targets.cmake
    NAMESPACE ai_pipe::
    DESTINATION ${AI_PIPE_CMAKE_INSTALL_DIR}
    COMPONENT dev
)

# Export for build tree
export(EXPORT ai_pipe_export_set
    FILE "${CMAKE_CURRENT_BINARY_DIR}/ai_pipe-targets.cmake"
    NAMESPACE ai_pipe::
)
