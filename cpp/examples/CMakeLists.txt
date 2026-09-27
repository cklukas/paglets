# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

paglets_add_guest(counter
    SOURCES counter/counter.cpp
    SCHEMA_HEADER counter/messages.hpp
    SCHEMA_NAMESPACE counter_msgs)
