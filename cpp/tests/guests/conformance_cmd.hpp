// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Command document of the ABI conformance guest, shared by the guest and the
// host tests. Every field is optional.

#pragma once

#include <paglets/abi.hpp>

namespace conformance {

struct Cmd {
    std::int32_t handle = 0;
    std::string name;
    std::vector<std::uint8_t> payload;
    std::vector<std::int32_t> caps;
    std::int32_t code = 0;
    std::int64_t ms = 0;
    std::int32_t priority = paglets::abi::default_priority;
    std::vector<std::uint8_t> spec;  // an encoded abi document
    std::vector<std::int32_t> lend;  // handles lent to a system paglet
};

inline void paglets_encode(paglets::msgpack::Writer& w, const Cmd& c) {
    using paglets::abi::detail::put;
    w.write_map_header(9);
    put(w, "handle", c.handle);
    put(w, "name", c.name);
    put(w, "payload", c.payload);
    put(w, "caps", c.caps);
    put(w, "code", c.code);
    put(w, "ms", c.ms);
    put(w, "priority", c.priority);
    put(w, "spec", c.spec);
    put(w, "lend", c.lend);
}

inline bool paglets_decode(paglets::msgpack::Reader& r, Cmd& c) {
    namespace mp = paglets::msgpack;
    return paglets::abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "handle") return mp::read_value(r, c.handle);
        if (k == "name") return mp::read_value(r, c.name);
        if (k == "payload") return mp::read_value(r, c.payload);
        if (k == "caps") return mp::read_value(r, c.caps);
        if (k == "code") return mp::read_value(r, c.code);
        if (k == "ms") return mp::read_value(r, c.ms);
        if (k == "priority") return mp::read_value(r, c.priority);
        if (k == "spec") return mp::read_value(r, c.spec);
        if (k == "lend") return mp::read_value(r, c.lend);
        return r.skip();
    });
}

}  // namespace conformance
