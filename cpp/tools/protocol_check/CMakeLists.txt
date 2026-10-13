# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# paglets-protocol-check: the protocols of the mesh against their properties
# (exhaustive model checking and timed random simulation).
add_executable(paglets-protocol-check protocol_check.cpp)
target_include_directories(paglets-protocol-check PRIVATE ${PROJECT_SOURCE_DIR}/common/include)
target_compile_options(paglets-protocol-check PRIVATE ${PAGLETS_WARNINGS})
