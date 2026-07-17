# Test plugins for the PluginLoader tests (F8), dlopen'ed at test
# runtime. Layout mirrors what the tests expect:
#   <plugin dir>/            libai_pipe_test_plugin.so       (valid)
#   <plugin dir>/bad/        libai_pipe_test_plugin_noentry.so
#                            libai_pipe_test_plugin_badversion.so
# The valid plugin sits alone in the top directory so loadDirectory()
# tests scan a directory of loadable plugins only.

function(ai_pipe_add_test_plugin target source subdir)
    add_library(${target} MODULE ${source})
    if(BUILD_SHARED_LIBS)
        # Shared core: the plugin references the one libai_pipe.so, so it
        # shares the host's NodeRegistry singleton automatically.
        target_link_libraries(${target} PRIVATE ai_pipe::ai_pipe)
    else()
        # Static core: linking the archive would pull a *private* copy of
        # the NodeRegistry singleton into the plugin, so AI_PIPE_REGISTER_NODE
        # would populate an instance the host never sees ("0 node types").
        # Compile against the headers only and leave the ai_pipe symbols
        # undefined; RTLD_NOW resolves them against the host executable's
        # exported symbols (ai_pipe_tests is built with ENABLE_EXPORTS),
        # giving a single shared registry.
        target_include_directories(${target} PRIVATE
            $<TARGET_PROPERTY:ai_pipe::ai_pipe,INTERFACE_INCLUDE_DIRECTORIES>)
    endif()
    set_target_properties(${target} PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${subdir}
    )
    # Without this, GCC emits STB_GNU_UNIQUE for the registry
    # singleton's inline-static machinery, which makes glibc mark the
    # library NODELETE: dlclose would never actually unload it and
    # re-loading would skip static-init registration.
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(${target} PRIVATE -fno-gnu-unique)
    endif()
    install(TARGETS ${target}
        LIBRARY DESTINATION tests/plugins/${subdir}
        COMPONENT test
    )
endfunction()

ai_pipe_add_test_plugin(ai_pipe_test_plugin test_plugin.cpp "")
ai_pipe_add_test_plugin(ai_pipe_test_plugin_noentry test_plugin_noentry.cpp bad)
ai_pipe_add_test_plugin(ai_pipe_test_plugin_badversion test_plugin_badversion.cpp bad)
