# Extract version information from header file
function(ai_pipe_extract_version)
    file(READ "${CMAKE_CURRENT_LIST_DIR}/src/api/ai_pipe/ai_pipe_version.hpp" file_contents)

    string(REGEX MATCH "AI_PIPE_VER_MAJOR ([0-9]+)" _ "${file_contents}")
    if(NOT CMAKE_MATCH_COUNT EQUAL 1)
        message(FATAL_ERROR "Could not extract major version number from ai_pipe_version.hpp")
    endif()
    set(ver_major ${CMAKE_MATCH_1})

    string(REGEX MATCH "AI_PIPE_VER_MINOR ([0-9]+)" _ "${file_contents}")
    if(NOT CMAKE_MATCH_COUNT EQUAL 1)
        message(FATAL_ERROR "Could not extract minor version number from ai_pipe_version.hpp")
    endif()
    set(ver_minor ${CMAKE_MATCH_1})

    string(REGEX MATCH "AI_PIPE_VER_PATCH ([0-9]+)" _ "${file_contents}")
    if(NOT CMAKE_MATCH_COUNT EQUAL 1)
        message(FATAL_ERROR "Could not extract patch version number from ai_pipe_version.hpp")
    endif()
    set(ver_patch ${CMAKE_MATCH_1})

    set(AI_PIPE_VERSION_MAJOR ${ver_major} PARENT_SCOPE)
    set(AI_PIPE_VERSION "${ver_major}.${ver_minor}.${ver_patch}" PARENT_SCOPE)

    message(STATUS "Extracted version: ${ver_major}.${ver_minor}.${ver_patch}")
endfunction()
