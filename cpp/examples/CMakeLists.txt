# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

paglets_add_module(hello SOURCES hello/hello.cpp)
paglets_add_module(ping_pong SOURCES ping_pong/ping_pong.cpp)
paglets_add_module(counter
    SOURCES counter/counter.cpp
    SCHEMA_HEADER counter/messages.hpp
    SCHEMA_NAMESPACE counter_msgs)
paglets_add_module(pi
    SOURCES pi/pi.cpp
    SCHEMA_HEADER pi/pi_msgs.hpp
    SCHEMA_NAMESPACE pi_msgs
    SERVICES mesh_info compute_slots)
