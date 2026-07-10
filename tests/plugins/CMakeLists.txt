# Test plugins for the PluginLoader tests (F8), dlopen'ed at test
# runtime. Layout mirrors what the tests expect:
#   <plugin dir>/            libai_pipe_test_plugin.so       (valid)
#   <plugin dir>/bad/        libai_pipe_test_plugin_noentry.so
#                            libai_pipe_test_plugin_badversion.so
# The valid plugin sits alone in the top directory so loadDirectory()
# tests scan a directory of loadable plugins only.

function(ai_pipe_add_test_plugin target source subdir)
    add_library(${target} MODULE ${source})
    target_link_libraries(${target} PRIVATE ai_pipe::ai_pipe)
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
