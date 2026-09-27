# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Applied to the WAMR checkout on every configure (cmake/PagletsWamr.cmake),
# with the WAMR source directory as working directory. Idempotent.
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

# wasm_exec_env_destroy removes the execution environment from its cluster
# without cluster->lock, while wasm_clusters_search_exec_env (called when an
# exception is set or cleared) reads the lists of all clusters under their
# locks. paglets runs many instances on several threads, so the removal must
# hold the lock (found by ThreadSanitizer). The cluster is destroyed after
# unlocking; wasm_cluster_destroy unlinks it under cluster_list_lock first.
set(_file "${CMAKE_CURRENT_SOURCE_DIR}/core/iwasm/common/wasm_exec_env.c")
file(READ "${_file}" _text)
set(_old "        wasm_cluster_del_exec_env(cluster, exec_env);\n    }\n#endif /* end of WASM_ENABLE_THREAD_MGR */")
set(_new "        /* paglets: unlink under cluster->lock */\n        os_mutex_lock(&cluster->lock);\n        bh_list_remove(&cluster->exec_env_list, exec_env);\n        bool paglets_cluster_empty = cluster->exec_env_list.len == 0;\n        os_mutex_unlock(&cluster->lock);\n        if (paglets_cluster_empty)\n            wasm_cluster_destroy(cluster);\n    }\n#endif /* end of WASM_ENABLE_THREAD_MGR */")
string(FIND "${_text}" "${_old}" _at)
if(_at GREATER_EQUAL 0)
    string(REPLACE "${_old}" "${_new}" _patched "${_text}")
    file(WRITE "${_file}" "${_patched}")
    message(STATUS "patch-wamr: wasm_exec_env_destroy unlinks under the cluster lock")
elseif(NOT _text MATCHES "paglets: unlink under cluster->lock")
    message(FATAL_ERROR "patch-wamr: wasm_exec_env_destroy has changed; review the patch")
endif()

# The Windows platform declares thread-local variables with
# __declspec(thread), which MinGW-w64 GCC ignores ("'thread' attribute
# directive ignored"): the variables become process-wide. Among them are the
# cached native stack boundary (the first thread to compute it sets it for
# every thread, so another thread's instances fail with "native stack
# overflow" depending on where the stacks lie) and the current execution
# environment. GCC and clang need __thread.
set(_file "${CMAKE_CURRENT_SOURCE_DIR}/core/shared/platform/windows/platform_internal.h")
file(READ "${_file}" _text)
set(_old "#define os_thread_local_attribute __declspec(thread)\n")
set(_new "/* paglets: MinGW-w64 GCC ignores __declspec(thread) */\n#if defined(_MSC_VER)\n#define os_thread_local_attribute __declspec(thread)\n#else\n#define os_thread_local_attribute __thread\n#endif\n")
string(FIND "${_text}" "${_old}" _at)
if(_at GREATER_EQUAL 0 AND NOT _text MATCHES "paglets: MinGW-w64 GCC ignores")
    string(REPLACE "${_old}" "${_new}" _patched "${_text}")
    file(WRITE "${_file}" "${_patched}")
    message(STATUS "patch-wamr: thread-local variables on Windows with GCC")
elseif(NOT _text MATCHES "paglets: MinGW-w64 GCC ignores")
    message(FATAL_ERROR "patch-wamr: windows/platform_internal.h has changed; review the patch")
endif()
