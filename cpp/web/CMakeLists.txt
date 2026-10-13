# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The local web UI (planning/cpp-web-ui.md): a small HTTP server of its own
# (WP34) and paglets-web (WP35).
add_library(paglets_webui STATIC src/http.cpp src/server.cpp)
target_include_directories(paglets_webui PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_options(paglets_webui PRIVATE ${PAGLETS_WARNINGS})
if(WIN32)
    target_link_libraries(paglets_webui PRIVATE ws2_32 shell32)
endif()
