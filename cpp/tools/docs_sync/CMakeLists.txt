# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# paglets-docs-sync: code excerpts and recorded results in docs/*.md.
add_executable(paglets-docs-sync docs_sync.cpp)
target_link_libraries(paglets-docs-sync PRIVATE paglets_wire)
target_compile_options(paglets-docs-sync PRIVATE ${PAGLETS_WARNINGS})
