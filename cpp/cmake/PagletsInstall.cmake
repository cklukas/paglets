# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Installation as a CMake package: `cmake --install` provides paglets-host
# and paglets-worker, the guest SDK (headers, paglet.cpp, the service
# contracts with their generated clients), the schema generator with the
# library it links (paglets::wire), and pagletsConfig.cmake, so projects
# outside this repository build guest paglets with
#
#   find_package(paglets REQUIRED)
#   paglets_add_module(<name> SOURCES ... [SCHEMA_HEADER ...] [SERVICES ...])
#
# Layout below the prefix:
#   bin/                          paglets-host, paglets-worker
#   include/paglets/              guest SDK, ABI, MessagePack, services/,
#                                 wire/ (schema generator only)
#   lib/                          libpaglets_wire.a
#   lib/cmake/paglets/            package configuration, PagletsGuest.cmake
#   share/paglets/                paglet.cpp, sha256.cpp, schema_gen.cpp,
#                                 service schemas

include(CMakePackageConfigHelpers)

set(PAGLETS_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/paglets" CACHE STRING
    "Installation directory of the paglets CMake package files, relative to the prefix")
set(PAGLETS_INSTALL_DATADIR "${CMAKE_INSTALL_DATADIR}/paglets")

install(TARGETS paglets-host paglets-worker EXPORT pagletsTargets
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(TARGETS paglets_wire EXPORT pagletsTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(EXPORT pagletsTargets NAMESPACE paglets:: DESTINATION "${PAGLETS_INSTALL_CMAKEDIR}")

# Headers guests include, and the wire headers the schema generator includes.
install(DIRECTORY "${PROJECT_SOURCE_DIR}/common/include/" "${PROJECT_SOURCE_DIR}/sdk/include/"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}" FILES_MATCHING PATTERN "*.hpp")
install(FILES "${PROJECT_SOURCE_DIR}/host/include/paglets/wire/json.hpp"
              "${PROJECT_SOURCE_DIR}/host/include/paglets/wire/reflect.hpp"
              "${PROJECT_SOURCE_DIR}/host/include/paglets/wire/schema.hpp"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/paglets/wire")
install(FILES "${PROJECT_SOURCE_DIR}/sdk/src/paglet.cpp" "${PROJECT_SOURCE_DIR}/host/src/sha256.cpp"
        DESTINATION "${PAGLETS_INSTALL_DATADIR}/sdk")
install(FILES "${PROJECT_SOURCE_DIR}/tools/schema_gen/schema_gen.cpp" DESTINATION "${PAGLETS_INSTALL_DATADIR}/schema_gen")

# The typed clients of all system services are generated here, so projects
# using the package need C++26 reflection only for their own schema headers.
set(_paglets_services_prebuilt OFF)
if(PAGLETS_ENABLE_REFLECTION)
    set(_paglets_services_prebuilt ON)
    file(GLOB _paglets_service_headers "${PROJECT_SOURCE_DIR}/common/include/paglets/services/*.hpp")
    set(_paglets_service_gen "")
    set(_paglets_service_json "")
    foreach(header IN LISTS _paglets_service_headers)
        get_filename_component(service "${header}" NAME_WE)
        paglets_service_schema(${service} gen)
        list(APPEND _paglets_service_gen "${gen}")
        list(APPEND _paglets_service_json "${PAGLETS_SERVICE_SCHEMA_DIR}/paglets/services/${service}.schema.json")
    endforeach()
    add_custom_target(paglets_service_schemas ALL DEPENDS ${_paglets_service_gen})
    install(FILES ${_paglets_service_gen} DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/paglets/services")
    install(FILES ${_paglets_service_json} DESTINATION "${PAGLETS_INSTALL_DATADIR}/services")
endif()

install(FILES "${PROJECT_SOURCE_DIR}/cmake/PagletsGuest.cmake" DESTINATION "${PAGLETS_INSTALL_CMAKEDIR}")
configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/pagletsConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/pagletsConfig.cmake"
    INSTALL_DESTINATION "${PAGLETS_INSTALL_CMAKEDIR}"
    PATH_VARS CMAKE_INSTALL_INCLUDEDIR PAGLETS_INSTALL_DATADIR)
write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/pagletsConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMinorVersion)
install(FILES "${PROJECT_BINARY_DIR}/pagletsConfig.cmake" "${PROJECT_BINARY_DIR}/pagletsConfigVersion.cmake"
        DESTINATION "${PAGLETS_INSTALL_CMAKEDIR}")
