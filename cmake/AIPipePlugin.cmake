include(CMakeParseArguments)
include(GNUInstallDirs)

function(ai_pipe_add_plugin target)
    set(options NO_INSTALL)
    set(one_value_args OUTPUT_NAME INSTALL_DESTINATION)
    set(multi_value_args SOURCES DEPENDENCIES)
    cmake_parse_arguments(AIPP "${options}" "${one_value_args}"
                          "${multi_value_args}" ${ARGN})

    if(NOT AIPP_SOURCES)
        message(FATAL_ERROR
            "ai_pipe_add_plugin(${target}): SOURCES is required")
    endif()

    get_target_property(ai_pipe_library_type ai_pipe::ai_pipe TYPE)
    if(ai_pipe_library_type STREQUAL "STATIC_LIBRARY")
        message(FATAL_ERROR
            "ai_pipe_add_plugin requires a shared ai_pipe library")
    endif()

    add_library(${target} MODULE ${AIPP_SOURCES})
    target_compile_features(${target} PRIVATE cxx_std_20)
    target_link_libraries(${target} PRIVATE ai_pipe::ai_pipe
                          ${AIPP_DEPENDENCIES})

    if(AIPP_OUTPUT_NAME)
        set(plugin_output_name "${AIPP_OUTPUT_NAME}")
    else()
        set(plugin_output_name "ai_pipe_plugin_${target}")
    endif()
    set_target_properties(${target} PROPERTIES OUTPUT_NAME "${plugin_output_name}")

    if(NOT AIPP_NO_INSTALL)
        if(AIPP_INSTALL_DESTINATION)
            set(plugin_library_install_dir "${AIPP_INSTALL_DESTINATION}")
            set(plugin_runtime_install_dir "${AIPP_INSTALL_DESTINATION}")
        else()
            set(plugin_library_install_dir
                "${CMAKE_INSTALL_LIBDIR}/ai_pipe/plugins")
            set(plugin_runtime_install_dir
                "${CMAKE_INSTALL_BINDIR}/ai_pipe/plugins")
        endif()
        install(TARGETS ${target}
            LIBRARY DESTINATION "${plugin_library_install_dir}"
            RUNTIME DESTINATION "${plugin_runtime_install_dir}")
    endif()
endfunction()
