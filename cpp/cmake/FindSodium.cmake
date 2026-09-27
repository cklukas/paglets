# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Finds libsodium (https://libsodium.org), the cryptography library of the
# host: Ed25519 signatures, X25519, ChaCha20-Poly1305, Argon2id.
#
# Uses pkg-config when available, otherwise searches the usual prefixes
# (CMAKE_PREFIX_PATH, Homebrew, MSYS2). Defines the imported target
# Sodium::Sodium and Sodium_VERSION.
#
# Packages: apt libsodium-dev, Homebrew libsodium, MSYS2
# mingw-w64-ucrt-x86_64-libsodium.

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_SODIUM QUIET libsodium)
endif()

find_path(Sodium_INCLUDE_DIR sodium.h
    HINTS ${PC_SODIUM_INCLUDEDIR} ${PC_SODIUM_INCLUDE_DIRS}
    PATHS /opt/homebrew/include /usr/local/include)
find_library(Sodium_LIBRARY NAMES sodium libsodium
    HINTS ${PC_SODIUM_LIBDIR} ${PC_SODIUM_LIBRARY_DIRS}
    PATHS /opt/homebrew/lib /usr/local/lib)

if(Sodium_INCLUDE_DIR AND EXISTS "${Sodium_INCLUDE_DIR}/sodium/version.h")
    file(STRINGS "${Sodium_INCLUDE_DIR}/sodium/version.h" _sodium_version_line
         REGEX "^#define SODIUM_VERSION_STRING \"[0-9.]+\"")
    string(REGEX REPLACE ".*\"([0-9.]+)\".*" "\\1" Sodium_VERSION "${_sodium_version_line}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sodium
    REQUIRED_VARS Sodium_LIBRARY Sodium_INCLUDE_DIR
    VERSION_VAR Sodium_VERSION)

if(Sodium_FOUND AND NOT TARGET Sodium::Sodium)
    add_library(Sodium::Sodium UNKNOWN IMPORTED)
    set_target_properties(Sodium::Sodium PROPERTIES
        IMPORTED_LOCATION "${Sodium_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${Sodium_INCLUDE_DIR}")
endif()
mark_as_advanced(Sodium_INCLUDE_DIR Sodium_LIBRARY)
