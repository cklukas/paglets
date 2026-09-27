# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Building guest paglets (wasm32-wasip1) inside the host build.
#
# Guests are compiled by a clang with a WASI sysroot (wasi-sdk, or Homebrew
# llvm + lld + wasi-libc + wasi-runtimes). The host toolchain (GCC 16) is not
# involved, so the guest compiler is invoked through custom commands.
#
# paglets_add_guest(<name>
#     SOURCES <files...>
#     [LANGUAGE C|CXX]                        (default CXX)
#     [SCHEMA_HEADER <header> SCHEMA_NAMESPACE <ns>]
#     [NO_SDK])
#
# Produces ${PAGLETS_GUEST_OUTPUT_DIR}/<name>.wasm and, with a schema, the
# generated <name>.schema.gen.hpp and <name>.schema.json.

set(PAGLETS_GUEST_OUTPUT_DIR "${PROJECT_BINARY_DIR}/guests" CACHE INTERNAL "")

set(_paglets_wasi_candidates_clang "")
set(_paglets_wasi_candidates_sysroot "")
if(DEFINED ENV{WASI_SDK_PATH})
    list(APPEND _paglets_wasi_candidates_clang "$ENV{WASI_SDK_PATH}/bin/clang")
    list(APPEND _paglets_wasi_candidates_sysroot "$ENV{WASI_SDK_PATH}/share/wasi-sysroot")
endif()
list(APPEND _paglets_wasi_candidates_clang /opt/wasi-sdk/bin/clang /opt/homebrew/opt/llvm/bin/clang /usr/local/opt/llvm/bin/clang)
list(APPEND _paglets_wasi_candidates_sysroot /opt/wasi-sdk/share/wasi-sysroot /opt/homebrew/share/wasi-sysroot
     /usr/local/share/wasi-sysroot /usr/share/wasi-sysroot)

if(NOT PAGLETS_WASI_CLANG)
    foreach(candidate IN LISTS _paglets_wasi_candidates_clang)
        if(EXISTS "${candidate}")
            set(PAGLETS_WASI_CLANG "${candidate}" CACHE FILEPATH "clang used for wasm32-wasi guests")
            break()
        endif()
    endforeach()
endif()
if(NOT PAGLETS_WASI_SYSROOT)
    foreach(candidate IN LISTS _paglets_wasi_candidates_sysroot)
        if(EXISTS "${candidate}")
            set(PAGLETS_WASI_SYSROOT "${candidate}" CACHE PATH "WASI sysroot for guests")
            break()
        endif()
    endforeach()
endif()

set(PAGLETS_GUESTS_AVAILABLE OFF)
if(PAGLETS_BUILD_GUESTS AND PAGLETS_WASI_CLANG AND PAGLETS_WASI_SYSROOT)
    get_filename_component(_paglets_clang_dir "${PAGLETS_WASI_CLANG}" DIRECTORY)
    set(PAGLETS_WASI_CLANGXX "${_paglets_clang_dir}/clang++")
    # Smoke test: compile and link a C++ reactor module.
    set(_probe_dir "${PROJECT_BINARY_DIR}/wasi-probe")
    file(MAKE_DIRECTORY "${_probe_dir}")
    file(WRITE "${_probe_dir}/probe.cpp" "#include <vector>\nextern \"C\" __attribute__((export_name(\"f\"))) int f() { std::vector<int> v{1,2}; return (int)v.size(); }\n")
    execute_process(
        COMMAND "${PAGLETS_WASI_CLANGXX}" --target=wasm32-wasip1 "--sysroot=${PAGLETS_WASI_SYSROOT}" -O2
                -mexec-model=reactor -fno-exceptions "${_probe_dir}/probe.cpp" -o "${_probe_dir}/probe.wasm"
        RESULT_VARIABLE _probe_result
        OUTPUT_QUIET ERROR_VARIABLE _probe_error)
    if(_probe_result EQUAL 0)
        set(PAGLETS_GUESTS_AVAILABLE ON)
        message(STATUS "paglets: guest toolchain ${PAGLETS_WASI_CLANGXX}, sysroot ${PAGLETS_WASI_SYSROOT}")
    else()
        message(WARNING "paglets: WASI toolchain probe failed, guests are not built:\n${_probe_error}")
    endif()
elseif(PAGLETS_BUILD_GUESTS)
    message(WARNING "paglets: no WASI clang/sysroot found (set PAGLETS_WASI_CLANG and PAGLETS_WASI_SYSROOT); guests are not built")
endif()

set(PAGLETS_GUEST_COMMON_FLAGS
    --target=wasm32-wasip1
    "--sysroot=${PAGLETS_WASI_SYSROOT}"
    -O2
    -mexec-model=reactor
    # Memory images need every mutable global; C/C++ guests have exactly one.
    -Wl,--export=__stack_pointer
    -Wl,--strip-debug)

function(paglets_add_guest name)
    if(NOT PAGLETS_GUESTS_AVAILABLE)
        return()
    endif()
    cmake_parse_arguments(G "NO_SDK" "LANGUAGE;SCHEMA_HEADER;SCHEMA_NAMESPACE" "SOURCES" ${ARGN})
    if(NOT G_LANGUAGE)
        set(G_LANGUAGE CXX)
    endif()

    set(out "${PAGLETS_GUEST_OUTPUT_DIR}/${name}.wasm")
    set(gen_dir "${PROJECT_BINARY_DIR}/generated/${name}")
    set(sources "")
    foreach(src IN LISTS G_SOURCES)
        get_filename_component(abs "${src}" ABSOLUTE)
        list(APPEND sources "${abs}")
    endforeach()
    set(depends ${sources} "${PROJECT_SOURCE_DIR}/common/include/paglets/msgpack.hpp")
    set(include_flags "-I${PROJECT_SOURCE_DIR}/common/include" "-I${gen_dir}")

    if(G_LANGUAGE STREQUAL "CXX")
        set(compiler "${PAGLETS_WASI_CLANGXX}")
        set(lang_flags -std=c++23 -fno-exceptions -fno-rtti)
        if(NOT G_NO_SDK)
            list(APPEND sources "${PROJECT_SOURCE_DIR}/sdk/src/guest.cpp")
            list(APPEND include_flags "-I${PROJECT_SOURCE_DIR}/sdk/include")
            list(APPEND depends "${PROJECT_SOURCE_DIR}/sdk/src/guest.cpp"
                 "${PROJECT_SOURCE_DIR}/sdk/include/paglets/guest.hpp")
        endif()
    else()
        set(compiler "${PAGLETS_WASI_CLANG}")
        set(lang_flags -std=c17)
    endif()

    if(G_SCHEMA_HEADER)
        if(NOT PAGLETS_ENABLE_REFLECTION)
            message(WARNING "paglets: guest ${name} needs the schema generator (C++26 reflection); skipped")
            return()
        endif()
        get_filename_component(schema_abs "${G_SCHEMA_HEADER}" ABSOLUTE)
        get_filename_component(schema_dir "${schema_abs}" DIRECTORY)
        get_filename_component(schema_file "${schema_abs}" NAME)
        set(gen_target "${name}_schema_gen")
        add_executable(${gen_target} "${PROJECT_SOURCE_DIR}/tools/schema_gen/schema_gen.cpp")
        target_link_libraries(${gen_target} PRIVATE paglets_host)
        target_compile_definitions(${gen_target} PRIVATE
            "PAGLETS_SCHEMA_HEADER=\"${schema_abs}\"" "PAGLETS_SCHEMA_NAMESPACE=${G_SCHEMA_NAMESPACE}")
        set(gen_header "${gen_dir}/${name}.schema.gen.hpp")
        set(gen_json "${PAGLETS_GUEST_OUTPUT_DIR}/${name}.schema.json")
        add_custom_command(
            OUTPUT "${gen_header}" "${gen_json}"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${gen_dir}" "${PAGLETS_GUEST_OUTPUT_DIR}"
            COMMAND ${gen_target} "${gen_header}" "${gen_json}" "${schema_file}"
            DEPENDS ${gen_target} "${schema_abs}"
            COMMENT "Generating guest schema code for ${name}"
            VERBATIM)
        list(APPEND depends "${gen_header}" "${schema_abs}")
        list(APPEND include_flags "-I${schema_dir}")
    endif()

    add_custom_command(
        OUTPUT "${out}"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${PAGLETS_GUEST_OUTPUT_DIR}"
        COMMAND "${compiler}" ${PAGLETS_GUEST_COMMON_FLAGS} ${lang_flags} ${include_flags} ${sources} -o "${out}"
        DEPENDS ${depends}
        COMMENT "Building guest paglet ${name}.wasm"
        VERBATIM)
    add_custom_target(${name}_wasm ALL DEPENDS "${out}")
endfunction()
