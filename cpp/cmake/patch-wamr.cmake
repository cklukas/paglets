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

# wamrc (the AOT compiler, built by PagletsWamrc.cmake against the system's
# LLVM) follows LLVM 18/19. LLVM 21 changed a few interfaces it uses: the
# ORC thread-safe context no longer hands out its LLVMContext, the target
# machine returns its subtarget by reference, PGOOptions lost its file-system
# argument, the stack-usage option was renamed and createTargetMachine takes
# a Triple. The replacements are guarded by LLVM_VERSION_MAJOR, so older
# LLVMs build the code as before.
function(paglets_patch_once file old new marker what)
    file(READ "${file}" _text)
    if(_text MATCHES "${marker}")
        return()
    endif()
    string(FIND "${_text}" "${old}" _at)
    if(_at LESS 0)
        message(FATAL_ERROR "patch-wamr: ${file} has changed (${what}); review the patch")
    endif()
    string(REPLACE "${old}" "${new}" _patched "${_text}")
    file(WRITE "${file}" "${_patched}")
    message(STATUS "patch-wamr: ${what}")
endfunction()

# WAMR's POSIX cache flush handles macOS but is empty on Linux. AOT code
# is relocated on a compiler thread and first runs on another core: ARM
# must publish those instruction bytes after the relocations, otherwise a
# stale ADRP may load instructions as a function's stack-size metadata.
paglets_patch_once(
    "${CMAKE_CURRENT_SOURCE_DIR}/core/shared/platform/common/posix/posix_memmap.c"
    "    sys_icache_invalidate(start, len);\n#else\n    (void)start;\n    (void)len;\n#endif\n}"
    "    sys_icache_invalidate(start, len);\n#elif defined(__GNUC__) || defined(__clang__)\n    /* paglets: publish relocated code on all POSIX CPU architectures */\n    __builtin___clear_cache((char *)start, (char *)start + len);\n#else\n    (void)start;\n    (void)len;\n#endif\n}"
    "paglets: publish relocated code"
    "synchronize the instruction cache after AOT relocation")

# Job instances have private linear memory, but the host must interrupt
# them. WAMR otherwise emits termination polls only for shared memories,
# even with --enable-multi-thread; an AOT job's endless loop cannot stop.
paglets_patch_once(
    "${CMAKE_CURRENT_SOURCE_DIR}/core/iwasm/compilation/aot_emit_control.c"
    "    if (!is_shared_memory) {\n        return true;\n    }"
    "    /* paglets: poll host cancellation for private-memory jobs too */\n    if (!is_shared_memory && !comp_ctx->enable_thread_mgr) {\n        return true;\n    }"
    "paglets: poll host cancellation"
    "AOT cancellation for private-memory jobs")

set(_dir "${CMAKE_CURRENT_SOURCE_DIR}/core/iwasm/compilation")
paglets_patch_once("${_dir}/aot_llvm.c"
    "    comp_ctx->orc_thread_safe_context = LLVMOrcCreateNewThreadSafeContext();\n"
    "#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 thread-safe context */\n    comp_ctx->context = LLVMContextCreate();\n    comp_ctx->orc_thread_safe_context =\n        LLVMOrcCreateNewThreadSafeContextFromLLVMContext(comp_ctx->context);\n#else\n    comp_ctx->orc_thread_safe_context = LLVMOrcCreateNewThreadSafeContext();\n#endif\n"
    "paglets: LLVM 21 thread-safe context" "wamrc: LLVM 21 thread-safe context")
paglets_patch_once("${_dir}/aot_llvm.c"
    "    if (!(comp_ctx->context = LLVMOrcThreadSafeContextGetContext(\n              comp_ctx->orc_thread_safe_context))) {"
    "#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 context getter */\n    if (!comp_ctx->context) {\n#else\n    if (!(comp_ctx->context = LLVMOrcThreadSafeContextGetContext(\n              comp_ctx->orc_thread_safe_context))) {\n#endif"
    "paglets: LLVM 21 context getter" "wamrc: LLVM 21 context getter")
paglets_patch_once("${_dir}/aot_llvm_extra.cpp"
    "    const llvm::MCSubtargetInfo *subTargetInfo =\n        targetMachine->getMCSubtargetInfo();"
    "    const llvm::MCSubtargetInfo *subTargetInfo =\n#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 subtarget */\n        &targetMachine->getMCSubtargetInfo();\n#else\n        targetMachine->getMCSubtargetInfo();\n#endif"
    "paglets: LLVM 21 subtarget" "wamrc: LLVM 21 subtarget info")
paglets_patch_once("${_dir}/aot_llvm_extra.cpp"
    "        PGO = PGOOptions(\"\", \"\", \"\", \"\", FS, PGOOptions::IRInstr);"
    "#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 PGOOptions */\n        (void)FS;\n        PGO = PGOOptions(\"\", \"\", \"\", \"\", PGOOptions::IRInstr);\n#else\n        PGO = PGOOptions(\"\", \"\", \"\", \"\", FS, PGOOptions::IRInstr);\n#endif"
    "paglets: LLVM 21 PGOOptions" "wamrc: LLVM 21 PGOOptions (instrumentation)")
paglets_patch_once("${_dir}/aot_llvm_extra.cpp"
    "        PGO = PGOOptions(comp_ctx->use_prof_file, \"\", \"\", \"\", FS,\n                         PGOOptions::IRUse);"
    "#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 PGOOptions use */\n        (void)FS;\n        PGO = PGOOptions(comp_ctx->use_prof_file, \"\", \"\", \"\", PGOOptions::IRUse);\n#else\n        PGO = PGOOptions(comp_ctx->use_prof_file, \"\", \"\", \"\", FS,\n                         PGOOptions::IRUse);\n#endif"
    "paglets: LLVM 21 PGOOptions use" "wamrc: LLVM 21 PGOOptions (profile use)")
paglets_patch_once("${_dir}/aot_llvm_extra2.cpp"
    "        opts.StackUsageOutput = StackUsageOutput;"
    "#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 stack usage */\n        opts.StackUsageFile = StackUsageOutput;\n#else\n        opts.StackUsageOutput = StackUsageOutput;\n#endif"
    "paglets: LLVM 21 stack usage" "wamrc: LLVM 21 stack-usage option")
paglets_patch_once("${_dir}/aot_llvm_extra2.cpp"
    "    auto targetmachine = target->createTargetMachine(triple, cpu, features,"
    "#if LLVM_VERSION_MAJOR >= 21 /* paglets: LLVM 21 triple */\n    auto targetmachine = target->createTargetMachine(llvm::Triple(triple), cpu, features,\n#else\n    auto targetmachine = target->createTargetMachine(triple, cpu, features,\n#endif"
    "paglets: LLVM 21 triple" "wamrc: LLVM 21 createTargetMachine")
