# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Builds paglets-wamrc, WAMR's ahead-of-time compiler, from the WAMR checkout
# (PagletsWamr.cmake) against an LLVM installed on this machine (18 to 23;
# patch-wamr.cmake adapts it to LLVM 21 and later). Hosts use it to compile
# the modules of compute jobs for their own CPU; without it, jobs run in the
# interpreter. It is built with the system compiler that matches LLVM's C++
# library (Apple clang with Homebrew's LLVM, the distribution's c++ with
# Debian's llvm-NN-dev), not with the compiler of the rest of the tree.
#
#   PAGLETS_BUILD_WAMRC  AUTO (default: when an LLVM is found), ON, OFF
#   PAGLETS_LLVM_DIR     the directory with LLVMConfig.cmake (found if empty)

set(PAGLETS_BUILD_WAMRC "AUTO" CACHE STRING "Build the AOT compiler paglets-wamrc (AUTO, ON, OFF)")
set(PAGLETS_LLVM_DIR "" CACHE PATH "LLVM's CMake directory for paglets-wamrc (default: searched)")
unset(PAGLETS_WAMRC CACHE)

if(PAGLETS_BUILD_WAMRC STREQUAL "OFF" OR NOT PAGLETS_WAMR_AOT)
    return()
endif()

set(_llvm_dir "${PAGLETS_LLVM_DIR}")
if(NOT _llvm_dir)
    set(_candidates)
    foreach(v 23 22 21 20 19 18)
        list(APPEND _candidates /opt/homebrew/opt/llvm@${v}/lib/cmake/llvm /usr/local/opt/llvm@${v}/lib/cmake/llvm
             /usr/lib/llvm-${v}/lib/cmake/llvm)
    endforeach()
    list(PREPEND _candidates /opt/homebrew/opt/llvm/lib/cmake/llvm /usr/local/opt/llvm/lib/cmake/llvm)
    foreach(c IN LISTS _candidates)
        if(EXISTS "${c}/LLVMConfig.cmake")
            set(_llvm_dir "${c}")
            break()
        endif()
    endforeach()
endif()
if(NOT _llvm_dir)
    if(PAGLETS_BUILD_WAMRC STREQUAL "ON")
        message(FATAL_ERROR "paglets: no LLVM found for paglets-wamrc (set PAGLETS_LLVM_DIR)")
    endif()
    message(STATUS "paglets: no LLVM found; compute jobs run in the interpreter (no paglets-wamrc)")
    return()
endif()
message(STATUS "paglets: paglets-wamrc with LLVM from ${_llvm_dir}")

include(ExternalProject)
set(_wamrc_out "${PROJECT_BINARY_DIR}/host/paglets-wamrc${CMAKE_EXECUTABLE_SUFFIX}")
ExternalProject_Add(paglets_wamrc_build
    SOURCE_DIR "${WAMR_ROOT_DIR}/wamr-compiler"
    BINARY_DIR "${PROJECT_BINARY_DIR}/wamrc"
    CMAKE_ARGS -DWAMR_BUILD_WITH_CUSTOM_LLVM=1 -DLLVM_DIR=${_llvm_dir} -DCMAKE_BUILD_TYPE=Release
               -DCMAKE_C_COMPILER=cc -DCMAKE_CXX_COMPILER=c++
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target wamrc
    BUILD_ALWAYS TRUE
    INSTALL_COMMAND ${CMAKE_COMMAND} -E copy_if_different <BINARY_DIR>/wamrc${CMAKE_EXECUTABLE_SUFFIX} ${_wamrc_out}
    BUILD_BYPRODUCTS ${_wamrc_out}
    USES_TERMINAL_BUILD TRUE)
add_custom_target(paglets-wamrc ALL DEPENDS paglets_wamrc_build)
set(PAGLETS_WAMRC "${_wamrc_out}" CACHE INTERNAL "paglets-wamrc in the build tree")
