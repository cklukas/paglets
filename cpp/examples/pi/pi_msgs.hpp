// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the pi compute example (planning/cpp-compute-jobs.md, section 4).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pi_msgs {

// --8<-- [start:messages]
// `start`: compute `digits` hex digits of pi in chunks of `chunk` digits,
// each a compute job (planning/cpp-compute-jobs.md) on one of `hosts`
// ({"any"}: the hosts of the mesh with free slots; empty: this host), at
// most `max_in_flight` at a time. A chunk whose job fails (its host went
// away, it ran longer than `chunk_timeout_ms`) is submitted again.
// `work_ms` makes each chunk take at least that long (for demonstrations
// and tests).
//
// With `output_root`, the digits go, in order as they come in, to files
// below that named root of the job's host (a grant for `read`, `write` and
// `create`): `<output_path>.hex` and, with `decimal`, `<output_path>.txt`.
// The paglet then keeps only the chunks that came early, not all digits.
// With `decimal`, one long compute job converts the hex digits into decimal
// digits at the end (about 1.2 per hex digit; the conversion needs every
// digit, so it cannot run per chunk). Without `output_root` it converts at
// most 800 000 hex digits (its input and output are messages).
struct Start {
    std::int64_t digits = 1000;
    std::int64_t chunk = 1000;
    std::int64_t max_in_flight = 8;
    std::int64_t chunk_timeout_ms = 0;  // wall time of a chunk's job; 0: the host's default
    std::int64_t work_ms = 0;
    bool decimal = true;
    std::string output_root;  // empty: the digits stay in the paglet's memory
    std::string output_path = "pi";
    std::vector<std::string> hosts = {"any"};
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// The input of a chunk's job.
struct Task {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t work_ms = 0;
};
// --8<-- [end:messages]

struct StatusRequest {};

// `status`: progress. `hex` and `decimal` show at most the first `shown`
// digits (the files have all of them).
struct Status {
    std::string state;        // idle, computing, writing, converting, done, failed
    std::int64_t digits = 0;  // hex digits asked for
    std::int64_t done = 0;    // hex digits contiguous from the start
    std::int64_t chunks = 0;
    std::int64_t chunks_done = 0;
    std::int64_t in_flight = 0;
    std::int64_t resent = 0;  // chunks submitted again (their job failed)
    bool finished = false;    // every hex digit is there
    std::string hex;          // "3." followed by hex digits
    std::int64_t decimal_digits = 0;  // decimal digits that hex `digits` give
    std::int64_t decimal_done = 0;    // converted so far
    std::string decimal;              // "3." followed by decimal digits
    std::int64_t shown = 0;
    std::vector<std::string> files;  // the files written, below `output_root`
    std::vector<std::string> hosts;  // names of the hosts that computed chunks
    std::int64_t chunks_aot = 0;     // chunks computed by ahead-of-time compiled code
    std::int64_t compute_ms = 0;     // running time of all chunk jobs together
    std::int64_t convert_ms = 0;     // running time of the conversion job
    std::string error;
};

}  // namespace pi_msgs
