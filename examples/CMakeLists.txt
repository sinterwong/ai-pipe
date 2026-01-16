cmake_minimum_required(VERSION 3.16)

project(AIPipeExamples VERSION ${AI_PIPE_VERSION})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Source files
file(GLOB AI_PIPE_EXAMPLES_SRCS CONFIGURE_DEPENDS "*.cpp")

# Dependency configuration
set(DEPENDENCY_INCLUDES
    ${PROJECT_INCLUDE_DIR}
)

set(DEPENDENCY_LIBS
    ai_pipe::ai_pipe
)

# Build shared library
add_library(ai_pipe_examples SHARED ${AI_PIPE_EXAMPLES_SRCS})

add_library(ai_pipe::ai_pipe_examples ALIAS ai_pipe_examples)

# Set target properties
set_target_properties(ai_pipe_examples PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${AI_PIPE_VERSION_MAJOR}
)

# Configure include directories
target_include_directories(ai_pipe_examples
    PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        ${DEPENDENCY_INCLUDES}
)

# Link dependencies
target_link_libraries(ai_pipe_examples PRIVATE ${DEPENDENCY_LIBS})

# Install library
install(TARGETS ai_pipe_examples
    EXPORT ai_pipe_export_set
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT lib
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT lib
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT bin
)
