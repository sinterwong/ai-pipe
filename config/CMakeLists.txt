add_library(ai_pipe_config graph_loader.cpp)
add_library(ai_pipe::config ALIAS ai_pipe_config)

target_compile_features(ai_pipe_config PUBLIC cxx_std_20)
target_include_directories(ai_pipe_config
    PUBLIC
        $<BUILD_INTERFACE:${AI_PIPE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(ai_pipe_config SYSTEM PRIVATE
    ${AI_PIPE_SOURCE_DIR}/3rdparty)
target_link_libraries(ai_pipe_config PUBLIC ai_pipe::ai_pipe)

if(MSVC)
    target_compile_options(ai_pipe_config PRIVATE /W4)
    if(AI_PIPE_WERROR)
        target_compile_options(ai_pipe_config PRIVATE /WX)
    endif()
else()
    target_compile_options(ai_pipe_config PRIVATE
        -Wall -Wextra -Wpedantic)
    if(AI_PIPE_WERROR)
        target_compile_options(ai_pipe_config PRIVATE -Werror)
    endif()
endif()

set_target_properties(ai_pipe_config PROPERTIES
    EXPORT_NAME config
    VERSION ${AI_PIPE_VERSION}
    SOVERSION ${AI_PIPE_VERSION_MAJOR}
)

install(TARGETS ai_pipe_config
    EXPORT ai_pipe_export_set
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT config
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT config
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT config
)
