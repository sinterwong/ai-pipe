cmake_minimum_required(VERSION 3.16)

project(AIPipeSrc VERSION ${AI_PIPE_VERSION})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Public headers (API)
file(GLOB_RECURSE PUBLIC_HEADERS CONFIGURE_DEPENDS 
    "api/ai_pipe/*.hpp"
)

file(GLOB AI_PIPE_ROOT_HDRS CONFIGURE_DEPENDS "*.hpp")
file(GLOB AI_PIPE_ROOT_SRCS CONFIGURE_DEPENDS "*.cpp")

set(AI_PIPE_SOURCES
    ${AI_PIPE_ROOT_SRCS}
)

set(AI_PIPE_PRIVATE_HEADERS
    ${AI_PIPE_ROOT_HDRS}
)

# Build shared library
add_library(ai_pipe 
    ${AI_PIPE_SOURCES}
    ${AI_PIPE_PRIVATE_HEADERS}
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

# Configure include directories
target_include_directories(ai_pipe
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/api>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
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
