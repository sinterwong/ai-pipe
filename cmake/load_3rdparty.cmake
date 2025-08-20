# Specialized libraries can be compiled separately, softinked to the 3RDPARTY_DIR, and then handled independently.
SET(3RDPARTY_ROOT ${PROJECT_SOURCE_DIR}/3rdparty)
SET(3RDPARTY_DIR ${PROJECT_SOURCE_DIR}/3rdparty/target/${TARGET_OS}_${TARGET_ARCH})
MESSAGE(STATUS "3RDPARTY_DIR: ${3RDPARTY_DIR}")

MACRO(LOAD_OPENCV)
    SET(OPENCV_HOME ${3RDPARTY_DIR}/opencv)
    
    IF (TARGET_OS STREQUAL "Android")
        SET(OpenCV_INCLUDE_DIRS ${OPENCV_HOME}/jni/include)
        SET(OpenCV_LIBRARY_DIRS ${OPENCV_HOME}/staticlibs/${ANDROID_ABI})
        SET(OpenCV_3RDPARTY_LIBRARY_DIRS ${OPENCV_HOME}/3rdparty/libs/${ANDROID_ABI})

        FILE(GLOB OpenCV_LIBS
            "${OpenCV_LIBRARY_DIRS}/*.a"
            "${OpenCV_3RDPARTY_LIBRARY_DIRS}/*.a"
        )
        MESSAGE(STATUS "Opencv libraries: ${OpenCV_LIBS}")
    ELSEIF(TARGET_OS STREQUAL "Windows")
        SET(OpenCV_LIBRARY_DIR ${OPENCV_HOME}/build)
        LIST(APPEND CMAKE_PREFIX_PATH ${OpenCV_LIBRARY_DIR})
        FIND_PACKAGE(OpenCV)

        IF(OpenCV_INCLUDE_DIRS)
            MESSAGE(STATUS "Opencv library status:")
            MESSAGE(STATUS "Opencv include path: ${OpenCV_INCLUDE_DIRS}")
            MESSAGE(STATUS "Opencv libraries dir: ${OpenCV_LIBRARY_DIR}")
            MESSAGE(STATUS "Opencv libraries: ${OpenCV_LIBS}")
        ELSE()
            MESSAGE(FATAL_ERROR "OpenCV not found!")
        ENDIF()
    
        LINK_DIRECTORIES(
            ${OpenCV_LIBRARY_DIR}
        )

    ELSE()
        SET(OpenCV_LIBRARY_DIR ${OPENCV_HOME}/lib)
        LIST(APPEND CMAKE_PREFIX_PATH ${OpenCV_LIBRARY_DIR}/cmake)
        FIND_PACKAGE(OpenCV CONFIG REQUIRED COMPONENTS core imgproc highgui video videoio imgcodecs calib3d)
        
        IF(OpenCV_INCLUDE_DIRS)
            MESSAGE(STATUS "Opencv library status:")
            MESSAGE(STATUS "    include path: ${OpenCV_INCLUDE_DIRS}")
            MESSAGE(STATUS "    libraries dir: ${OpenCV_LIBRARY_DIR}")
            MESSAGE(STATUS "    libraries: ${OpenCV_LIBS}")
        ELSE()
            MESSAGE(FATAL_ERROR "OpenCV not found!")
        ENDIF()
    
        LINK_DIRECTORIES(
            ${OpenCV_LIBRARY_DIR}
        )
    ENDIF()
ENDMACRO()

MACRO(LOAD_GTEST)
    SET(GTEST_HOME ${3RDPARTY_DIR}/gtest)
    MESSAGE(STATUS "GTEST_HOME: ${GTEST_HOME}")

    SET(GTEST_INCLUDE_DIRS "${GTEST_HOME}/include")
    SET(GTEST_LIB_DIR "${GTEST_HOME}/lib")

    SET(GTEST_LIBS
        gtest
        gmock
    )
    LINK_DIRECTORIES(${GTEST_LIB_DIR})
ENDMACRO()

MACRO(LOAD_YAML)
    SET(YAML_HOME ${3RDPARTY_DIR}/yaml-cpp)
    IF (TARGET_OS STREQUAL "Android")
        SET(CMAKE_FIND_ROOT_PATH ${CMAKE_FIND_ROOT_PATH} ${YAML_HOME}/lib/cmake)
    ELSE()
        LIST(APPEND CMAKE_PREFIX_PATH ${YAML_HOME}/lib/cmake)
    ENDIF()
    FIND_PACKAGE(yaml-cpp)

    IF(NOT yaml-cpp_FOUND)
        MESSAGE(FATAL_ERROR "yaml-cpp not found!")
    ENDIF()
ENDMACRO()

MACRO(LOAD_LOGGER)
    SET(LOGGER_HOME ${3RDPARTY_DIR}/logger)
    IF (TARGET_OS STREQUAL "Android")
        SET(CMAKE_FIND_ROOT_PATH ${CMAKE_FIND_ROOT_PATH} ${LOGGER_HOME}/share/logger)
    ELSE()
        LIST(APPEND CMAKE_PREFIX_PATH ${LOGGER_HOME}/share/logger)
    ENDIF()

    FIND_PACKAGE(logger)

    IF(NOT logger_FOUND)
        MESSAGE(FATAL_ERROR "logger not found!")
    ENDIF()
ENDMACRO()

MACRO(LOAD_AI_CORE)
    SET(AI_CORE_HOME ${3RDPARTY_DIR}/ai_core)
    IF (TARGET_OS STREQUAL "Android")
        SET(CMAKE_FIND_ROOT_PATH ${CMAKE_FIND_ROOT_PATH} ${AI_CORE_HOME}/share)
    ELSE()
        LIST(APPEND CMAKE_PREFIX_PATH ${AI_CORE_HOME}/share)
    ENDIF()
    FIND_PACKAGE(ai_core)

    IF(NOT ai_core_FOUND)
        MESSAGE(FATAL_ERROR "ai_core not found!")
    ENDIF()
ENDMACRO()

MACRO(LOAD_ANDROID_ENV)
    SET(ANDROID_JIN_INCLUDE_DIR "${ANDROID_NDK}/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/include")
    SET(ANDROID_JIN_LIBS_DIR "${ANDROID_NDK}/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/lib/${TARGET_ARCH}-linux-android/24")
    SET(ANDROID_JIN_LIBS 
        android
        log
        z
        dl
    )
    LINK_DIRECTORIES(${ANDROID_JIN_LIBS_DIR})
ENDMACRO()
