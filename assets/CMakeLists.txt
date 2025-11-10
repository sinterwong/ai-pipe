cmake_minimum_required(VERSION 3.16)

project(Resources)

# Install configuration files if they exist
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/conf")
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/conf"
        DESTINATION assets
        COMPONENT resources
    )
    message(STATUS "Configuration files will be installed to assets/conf")
endif()
