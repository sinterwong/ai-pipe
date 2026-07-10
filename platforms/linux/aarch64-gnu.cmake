# Generic aarch64 cross toolchain using the distro-packaged GNU compiler
# (apt install g++-aarch64-linux-gnu). Unlike aarch64.cmake - which targets
# a vendor NDK-style clang via TOOLCHAIN_ROOTDIR - this file has no external
# inputs, so anyone (and CI) can cross-build the dependency-free core:
#
#   cmake -B build-aarch64 \
#     -DCMAKE_TOOLCHAIN_FILE=platforms/linux/aarch64-gnu.cmake \
#     -DCMAKE_BUILD_TYPE=Release
#
# No -march/-mcpu tuning is forced here: the triple's default (armv8-a)
# keeps the binary portable across aarch64 devices; pass CMAKE_CXX_FLAGS
# for device-specific tuning.

set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

set(TARGET_OS ${CMAKE_SYSTEM_NAME})
set(TARGET_ARCH ${CMAKE_SYSTEM_PROCESSOR})

set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)

# Search headers/libraries only in the target sysroot; programs on the host
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
