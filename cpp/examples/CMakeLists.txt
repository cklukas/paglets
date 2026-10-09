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
paglets_add_module(courier
    SOURCES courier/courier.cpp
    SCHEMA_HEADER courier/courier_msgs.hpp
    SCHEMA_NAMESPACE courier_msgs
    SERVICES artifacts web
    PATTERNS)
paglets_add_module(digest
    SOURCES digest/digest.cpp
    SCHEMA_HEADER digest/digest_msgs.hpp
    SCHEMA_NAMESPACE digest_msgs
    SERVICES ai
    PATTERNS)
paglets_add_module(journey
    SOURCES journey/journey.cpp
    SCHEMA_HEADER journey/journey_msgs.hpp
    SCHEMA_NAMESPACE journey_msgs
    PATTERNS)
paglets_add_module(finder
    SOURCES finder/finder.cpp
    SCHEMA_HEADER finder/finder_msgs.hpp
    SCHEMA_NAMESPACE finder_msgs
    PATTERNS)
paglets_add_module(latency
    SOURCES latency/latency.cpp
    SCHEMA_HEADER latency/latency_msgs.hpp
    SCHEMA_NAMESPACE latency_msgs
    PATTERNS)
paglets_add_module(inventory
    SOURCES inventory/inventory.cpp
    SCHEMA_HEADER inventory/inventory_msgs.hpp
    SCHEMA_NAMESPACE inventory_msgs
    PATTERNS)
paglets_add_module(seek
    SOURCES seek/seek.cpp
    SCHEMA_HEADER seek/seek_msgs.hpp
    SCHEMA_NAMESPACE seek_msgs
    PATTERNS)
paglets_add_module(guard
    SOURCES guard/guard.cpp
    SCHEMA_HEADER guard/guard_msgs.hpp
    SCHEMA_NAMESPACE guard_msgs
    PATTERNS)
paglets_add_module(dupes
    SOURCES dupes/dupes.cpp ../host/src/sha256.cpp
    SCHEMA_HEADER dupes/dupes_msgs.hpp
    SCHEMA_NAMESPACE dupes_msgs
    PATTERNS)
paglets_add_module(semantic
    SOURCES semantic/semantic.cpp
    SCHEMA_HEADER semantic/semantic_msgs.hpp
    SCHEMA_NAMESPACE semantic_msgs
    SERVICES ai
    PATTERNS)
paglets_add_module(researcher
    SOURCES researcher/researcher.cpp
    SCHEMA_HEADER researcher/researcher_msgs.hpp
    SCHEMA_NAMESPACE researcher_msgs
    SERVICES ai web
    PATTERNS)
