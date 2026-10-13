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

# paglets-web (WP35): the UI's files are compiled in.
set(_paglets_web_assets "${CMAKE_CURRENT_SOURCE_DIR}/assets")
set(_paglets_web_generated "${CMAKE_CURRENT_BINARY_DIR}/assets.cpp")
add_custom_command(
    OUTPUT "${_paglets_web_generated}"
    COMMAND ${CMAKE_COMMAND} "-DOUT=${_paglets_web_generated}" "-DASSETS=${_paglets_web_assets}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/embed_assets.cmake"
    DEPENDS "${_paglets_web_assets}/index.html" "${_paglets_web_assets}/app.css" "${_paglets_web_assets}/app.js"
            "${CMAKE_CURRENT_SOURCE_DIR}/cmake/embed_assets.cmake"
    COMMENT "Embedding the paglets-web UI"
    VERBATIM)
add_library(paglets_webapp STATIC src/app.cpp "${_paglets_web_generated}")
target_include_directories(paglets_webapp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(paglets_webapp PUBLIC paglets_webui paglets_client paglets_host)
target_compile_options(paglets_webapp PRIVATE ${PAGLETS_WARNINGS})
add_executable(paglets-web src/web_main.cpp)
target_link_libraries(paglets-web PRIVATE paglets_webapp)
target_compile_options(paglets-web PRIVATE ${PAGLETS_WARNINGS})
