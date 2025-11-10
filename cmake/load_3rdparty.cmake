# Modern 3rdparty library loading functions
# Specialized libraries can be compiled separately, softlinked to the 3RDPARTY_DIR,
# and then handled independently.

set(3RDPARTY_ROOT ${PROJECT_SOURCE_DIR}/3rdparty)
set(3RDPARTY_DIR ${PROJECT_SOURCE_DIR}/3rdparty/target/${TARGET_OS}_${TARGET_ARCH})
message(STATUS "3RDPARTY_DIR: ${3RDPARTY_DIR}")

# Load OpenCV library
function(load_opencv)
    set(OPENCV_HOME ${3RDPARTY_DIR}/opencv)

    if(TARGET_OS STREQUAL "Android")
        set(OpenCV_INCLUDE_DIRS ${OPENCV_HOME}/jni/include PARENT_SCOPE)
        set(OpenCV_LIBRARY_DIRS ${OPENCV_HOME}/staticlibs/${ANDROID_ABI})
        set(OpenCV_3RDPARTY_LIBRARY_DIRS ${OPENCV_HOME}/3rdparty/libs/${ANDROID_ABI})

        file(GLOB opencv_libs
            "${OpenCV_LIBRARY_DIRS}/*.a"
            "${OpenCV_3RDPARTY_LIBRARY_DIRS}/*.a"
        )
        set(OpenCV_LIBS ${opencv_libs} PARENT_SCOPE)
        message(STATUS "OpenCV libraries: ${opencv_libs}")

    elseif(TARGET_OS STREQUAL "Windows")
        set(OpenCV_LIBRARY_DIR ${OPENCV_HOME}/build)
        list(APPEND CMAKE_PREFIX_PATH ${OpenCV_LIBRARY_DIR})
        find_package(OpenCV REQUIRED)

        if(OpenCV_FOUND)
            message(STATUS "OpenCV library status:")
            message(STATUS "  Include path: ${OpenCV_INCLUDE_DIRS}")
            message(STATUS "  Libraries dir: ${OpenCV_LIBRARY_DIR}")
            message(STATUS "  Libraries: ${OpenCV_LIBS}")

            set(OpenCV_INCLUDE_DIRS ${OpenCV_INCLUDE_DIRS} PARENT_SCOPE)
            set(OpenCV_LIBS ${OpenCV_LIBS} PARENT_SCOPE)
        else()
            message(FATAL_ERROR "OpenCV not found!")
        endif()

    else()
        # Linux and other platforms
        set(OpenCV_LIBRARY_DIR ${OPENCV_HOME}/lib)
        list(APPEND CMAKE_PREFIX_PATH ${OpenCV_LIBRARY_DIR}/cmake)
        find_package(OpenCV CONFIG REQUIRED
            COMPONENTS core imgproc highgui video videoio imgcodecs calib3d)

        if(OpenCV_FOUND)
            message(STATUS "OpenCV library status:")
            message(STATUS "  Include path: ${OpenCV_INCLUDE_DIRS}")
            message(STATUS "  Libraries dir: ${OpenCV_LIBRARY_DIR}")
            message(STATUS "  Libraries: ${OpenCV_LIBS}")

            set(OpenCV_INCLUDE_DIRS ${OpenCV_INCLUDE_DIRS} PARENT_SCOPE)
            set(OpenCV_LIBS ${OpenCV_LIBS} PARENT_SCOPE)
        else()
            message(FATAL_ERROR "OpenCV not found!")
        endif()
    endif()
endfunction()

# Load Google Test library
function(load_gtest)
    set(GTEST_HOME ${3RDPARTY_DIR}/gtest)
    message(STATUS "GTEST_HOME: ${GTEST_HOME}")

    set(GTEST_INCLUDE_DIRS "${GTEST_HOME}/include" PARENT_SCOPE)
    set(GTEST_LIB_DIR "${GTEST_HOME}/lib")

    # Create imported targets if they don't exist
    if(NOT TARGET GTest::gtest)
        add_library(GTest::gtest UNKNOWN IMPORTED)
        set_target_properties(GTest::gtest PROPERTIES
            IMPORTED_LOCATION "${GTEST_LIB_DIR}/libgtest.a"
            INTERFACE_INCLUDE_DIRECTORIES "${GTEST_HOME}/include"
        )
    endif()

    if(NOT TARGET GTest::gmock)
        add_library(GTest::gmock UNKNOWN IMPORTED)
        set_target_properties(GTest::gmock PROPERTIES
            IMPORTED_LOCATION "${GTEST_LIB_DIR}/libgmock.a"
            INTERFACE_INCLUDE_DIRECTORIES "${GTEST_HOME}/include"
        )
    endif()
endfunction()

# Load yaml-cpp library
function(load_yaml)
    set(YAML_HOME ${3RDPARTY_DIR}/yaml-cpp)

    if(TARGET_OS STREQUAL "Android")
        list(APPEND CMAKE_FIND_ROOT_PATH ${YAML_HOME}/lib/cmake)
    else()
        list(APPEND CMAKE_PREFIX_PATH ${YAML_HOME}/lib/cmake)
    endif()

    find_package(yaml-cpp REQUIRED)

    if(NOT yaml-cpp_FOUND)
        message(FATAL_ERROR "yaml-cpp not found!")
    endif()

    message(STATUS "yaml-cpp found successfully")
endfunction()

# Load logger library
function(load_logger)
    set(LOGGER_HOME ${3RDPARTY_DIR}/logger)

    if(TARGET_OS STREQUAL "Android")
        list(APPEND CMAKE_FIND_ROOT_PATH ${LOGGER_HOME}/share/logger)
    else()
        list(APPEND CMAKE_PREFIX_PATH ${LOGGER_HOME}/share/logger)
    endif()

    find_package(logger REQUIRED)

    if(NOT logger_FOUND)
        message(FATAL_ERROR "logger not found!")
    endif()

    message(STATUS "logger found successfully")
endfunction()

# Load ai_core library
function(load_ai_core)
    set(AI_CORE_HOME ${3RDPARTY_DIR}/ai_core)

    if(TARGET_OS STREQUAL "Android")
        list(APPEND CMAKE_FIND_ROOT_PATH ${AI_CORE_HOME}/share)
    else()
        list(APPEND CMAKE_PREFIX_PATH ${AI_CORE_HOME}/share)
    endif()

    find_package(ai_core REQUIRED)

    if(NOT ai_core_FOUND)
        message(FATAL_ERROR "ai_core not found!")
    endif()

    message(STATUS "ai_core found successfully")
endfunction()

# Load Android environment dependencies
function(load_android_env)
    set(ANDROID_JNI_INCLUDE_DIR
        "${ANDROID_NDK}/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/include"
        PARENT_SCOPE)
    set(ANDROID_JNI_LIBS_DIR
        "${ANDROID_NDK}/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/lib/${TARGET_ARCH}-linux-android/24"
        PARENT_SCOPE)

    # Create imported targets for Android system libraries
    set(android_libs android log z dl)
    set(ANDROID_JNI_LIBS ${android_libs} PARENT_SCOPE)

    message(STATUS "Android environment configured")
endfunction()
