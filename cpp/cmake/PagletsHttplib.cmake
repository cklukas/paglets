# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Fetches cpp-httplib (MIT, header only) at a pinned release for the HTTPS
# transport (planning/cpp-networking.md). TLS comes from OpenSSL 3.

include(FetchContent)

set(PAGLETS_HTTPLIB_TAG "v0.58.0" CACHE STRING "cpp-httplib release tag")

FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG ${PAGLETS_HTTPLIB_TAG}
    GIT_SHALLOW TRUE
    # Only the header is used.
    SOURCE_SUBDIR paglets-no-cmake-project)
FetchContent_MakeAvailable(httplib)
set(PAGLETS_HTTPLIB_INCLUDE_DIR "${httplib_SOURCE_DIR}")
