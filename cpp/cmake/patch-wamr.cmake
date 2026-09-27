# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Applied to the WAMR checkout by FetchContent (PATCH_COMMAND). Idempotent.
#
# win_file.c has line comments that end in a backslash followed by a space
# ("// Starts with \??\ "). GCC treats backslash-whitespace-newline as a line
# continuation, so MinGW-w64 GCC swallows the next source line into the
# comment; MSVC does not. Drop the trailing backslash from those comments.

set(_file "${CMAKE_CURRENT_SOURCE_DIR}/core/shared/platform/windows/win_file.c")
if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "patch-wamr: ${_file} not found")
endif()
file(READ "${_file}" _text)
string(REGEX REPLACE "(//[^\n]*)\\\\[ \t]+\n" "\\1\n" _patched "${_text}")
if(NOT _patched STREQUAL _text)
    file(WRITE "${_file}" "${_patched}")
    message(STATUS "patch-wamr: removed trailing backslashes in win_file.c comments")
endif()
