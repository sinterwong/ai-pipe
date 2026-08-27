# Test plugins for the PluginLoader tests (F8), dynamically loaded at test
# runtime. Layout mirrors what the tests expect:
#   <plugin dir>/            ai_pipe_plugin_test.<module>       (valid)
#   <plugin dir>/bad/        ai_pipe_test_plugin_noentry.<module>
#                            ai_pipe_test_plugin_badversion.<module>
# The valid plugin sits alone in the top directory so loadDirectory()
# tests scan a directory of loadable plugins only.

function(ai_pipe_add_test_plugin target source subdir)
    add_library(${target} MODULE ${source})
    if(BUILD_SHARED_LIBS)
        # Shared core: the plugin references the one libai_pipe.so, so it
        # shares the host's NodeRegistry singleton automatically.
        target_link_libraries(${target} PRIVATE ai_pipe::ai_pipe)
    else()
        # Static core: linking the archive would pull a private NodeRegistry
        # singleton into the plugin. Compile against headers and resolve the
        # explicit registration call against the exported host symbols.
        target_include_directories(${target} PRIVATE
            $<TARGET_PROPERTY:ai_pipe::ai_pipe,INTERFACE_INCLUDE_DIRECTORIES>)
    endif()
    set_target_properties(${target} PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${subdir}
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${subdir}
    )
    install(TARGETS ${target}
        LIBRARY DESTINATION tests/plugins/${subdir}
        RUNTIME DESTINATION tests/plugins/${subdir}
        COMPONENT test
    )
endfunction()

ai_pipe_add_test_plugin(ai_pipe_test_plugin test_plugin.cpp "")
set_target_properties(ai_pipe_test_plugin PROPERTIES
    OUTPUT_NAME ai_pipe_plugin_test)
ai_pipe_add_test_plugin(ai_pipe_test_plugin_noentry test_plugin_noentry.cpp bad)
ai_pipe_add_test_plugin(ai_pipe_test_plugin_badversion test_plugin_badversion.cpp bad)
ai_pipe_add_test_plugin(ai_pipe_test_plugin_reject test_plugin_reject.cpp bad)
