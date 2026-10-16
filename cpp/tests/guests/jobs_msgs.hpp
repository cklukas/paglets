// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Documents of the compute-jobs test guest, shared by the guest and the
// host tests.

#pragma once

#include <paglets/abi.hpp>

namespace jobs_test {

// `run`, `coro`: submit a job and answer with its Outcome when it ends.
struct Run {
    std::string function;
    std::vector<std::uint8_t> input;
    std::int64_t max_ms = 0;
    std::int64_t cores = 1;
    std::vector<std::string> hosts;
    bool finish = false;            // finish_on_departure
    std::int32_t artifact = 0;      // the input as an artifact made by the guest first (size in bytes)
    std::int64_t memory_mb = 0;
};

struct Outcome {
    std::int32_t status = 0;
    std::vector<std::uint8_t> output;
    std::string error;
    std::string host;
    bool aot = false;
    bool recovered_before_reply = false;
    bool artifact = false;        // the output came as an artifact
    std::int64_t artifact_size = 0;
    std::int64_t elapsed_ms = 0;
};

inline void paglets_encode(paglets::msgpack::Writer& w, const Run& c) {
    using paglets::abi::detail::put;
    w.write_map_header(8);
    put(w, "function", c.function);
    put(w, "input", c.input);
    put(w, "max_ms", c.max_ms);
    put(w, "cores", c.cores);
    put(w, "hosts", c.hosts);
    put(w, "finish", c.finish);
    put(w, "artifact", c.artifact);
    put(w, "memory_mb", c.memory_mb);
}

inline bool paglets_decode(paglets::msgpack::Reader& r, Run& c) {
    namespace mp = paglets::msgpack;
    return paglets::abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "function") return mp::read_value(r, c.function);
        if (k == "input") return mp::read_value(r, c.input);
        if (k == "max_ms") return mp::read_value(r, c.max_ms);
        if (k == "cores") return mp::read_value(r, c.cores);
        if (k == "hosts") return mp::read_value(r, c.hosts);
        if (k == "finish") return mp::read_value(r, c.finish);
        if (k == "artifact") return mp::read_value(r, c.artifact);
        if (k == "memory_mb") return mp::read_value(r, c.memory_mb);
        return r.skip();
    });
}

inline void paglets_encode(paglets::msgpack::Writer& w, const Outcome& o) {
    using paglets::abi::detail::put;
    w.write_map_header(9);
    put(w, "recovered_before_reply", o.recovered_before_reply);
    put(w, "status", o.status);
    put(w, "output", o.output);
    put(w, "error", o.error);
    put(w, "host", o.host);
    put(w, "aot", o.aot);
    put(w, "artifact", o.artifact);
    put(w, "artifact_size", o.artifact_size);
    put(w, "elapsed_ms", o.elapsed_ms);
}

inline bool paglets_decode(paglets::msgpack::Reader& r, Outcome& o) {
    namespace mp = paglets::msgpack;
    return paglets::abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "recovered_before_reply") return mp::read_value(r, o.recovered_before_reply);
        if (k == "status") return mp::read_value(r, o.status);
        if (k == "output") return mp::read_value(r, o.output);
        if (k == "error") return mp::read_value(r, o.error);
        if (k == "host") return mp::read_value(r, o.host);
        if (k == "aot") return mp::read_value(r, o.aot);
        if (k == "artifact") return mp::read_value(r, o.artifact);
        if (k == "artifact_size") return mp::read_value(r, o.artifact_size);
        if (k == "elapsed_ms") return mp::read_value(r, o.elapsed_ms);
        return r.skip();
    });
}

}  // namespace jobs_test
