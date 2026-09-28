# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Finds zstd (https://facebook.github.io/zstd/), which compresses memory
# pages on the wire when paglets move.
#
# Uses pkg-config when available, otherwise searches the usual prefixes
# (CMAKE_PREFIX_PATH, Homebrew, MSYS2). Defines the imported target
# Zstd::Zstd and Zstd_VERSION.
#
# Packages: apt libzstd-dev, Homebrew zstd, MSYS2 mingw-w64-ucrt-x86_64-zstd.

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_ZSTD QUIET libzstd)
endif()

find_path(Zstd_INCLUDE_DIR zstd.h
    HINTS ${PC_ZSTD_INCLUDEDIR} ${PC_ZSTD_INCLUDE_DIRS}
    PATHS /opt/homebrew/include /usr/local/include)
find_library(Zstd_LIBRARY NAMES zstd libzstd
    HINTS ${PC_ZSTD_LIBDIR} ${PC_ZSTD_LIBRARY_DIRS}
    PATHS /opt/homebrew/lib /usr/local/lib)

if(Zstd_INCLUDE_DIR AND EXISTS "${Zstd_INCLUDE_DIR}/zstd.h")
    file(STRINGS "${Zstd_INCLUDE_DIR}/zstd.h" _zstd_version_lines REGEX "^#define ZSTD_VERSION_(MAJOR|MINOR|RELEASE) +[0-9]+")
    string(REGEX REPLACE ".*ZSTD_VERSION_MAJOR +([0-9]+).*" "\\1" _zstd_major "${_zstd_version_lines}")
    string(REGEX REPLACE ".*ZSTD_VERSION_MINOR +([0-9]+).*" "\\1" _zstd_minor "${_zstd_version_lines}")
    string(REGEX REPLACE ".*ZSTD_VERSION_RELEASE +([0-9]+).*" "\\1" _zstd_release "${_zstd_version_lines}")
    set(Zstd_VERSION "${_zstd_major}.${_zstd_minor}.${_zstd_release}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Zstd
    REQUIRED_VARS Zstd_LIBRARY Zstd_INCLUDE_DIR
    VERSION_VAR Zstd_VERSION)

if(Zstd_FOUND AND NOT TARGET Zstd::Zstd)
    add_library(Zstd::Zstd UNKNOWN IMPORTED)
    set_target_properties(Zstd::Zstd PROPERTIES
        IMPORTED_LOCATION "${Zstd_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${Zstd_INCLUDE_DIR}")
endif()

mark_as_advanced(Zstd_INCLUDE_DIR Zstd_LIBRARY)
