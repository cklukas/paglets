// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the pi compute example (planning/cpp-compute.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace pi_msgs {

// `start`: compute `digits` hex digits of pi in chunks of `chunk` digits,
// at most `max_in_flight` chunks at a time on hosts mesh-info selects. A
// chunk not answered within `chunk_timeout_ms` (its host may be gone) is
// sent again elsewhere. `work_ms` makes each chunk take at least that long
// (for demonstrations and tests).
//
// With `output_root`, the digits go, in order as they come in, to files
// below that named root of the job's host (a grant for `write` and
// `create`): `<output_path>.hex` and, with `decimal`, `<output_path>.txt`.
// The job then keeps only the chunks that came early, not all digits.
// With `decimal`, the job converts the hex digits into decimal digits at the
// end (about 1.2 per hex digit; the conversion needs every digit, so it
// cannot run per chunk).
struct Start {
    std::int64_t digits = 1000;
    std::int64_t chunk = 60;
    std::int64_t max_in_flight = 8;
    std::int64_t chunk_timeout_ms = 30'000;
    std::int64_t work_ms = 0;
    bool decimal = true;
    std::string output_root;  // empty: the digits stay in the job's memory
    std::string output_path = "pi";
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// The arguments of a worker (a clone of the job paglet on another host).
struct Task {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t attempt = 0;
    std::int64_t work_ms = 0;
};

// `result`: a worker's digits.
struct Result {
    std::int64_t start = 0;
    std::int64_t count = 0;
    std::int64_t attempt = 0;
    std::string digits;
    std::string host;  // key ID of the host that computed it
    std::string host_name;
};

struct StatusRequest {};

// `status`: progress. `hex` and `decimal` show at most the first `shown`
// digits (the files have all of them).
struct Status {
    std::string state;        // idle, computing, converting, done, failed
    std::int64_t digits = 0;  // hex digits asked for
    std::int64_t done = 0;    // hex digits contiguous from the start
    std::int64_t chunks = 0;
    std::int64_t chunks_done = 0;
    std::int64_t in_flight = 0;
    std::int64_t resent = 0;  // chunks sent again (no answer in time)
    bool finished = false;    // every hex digit is there
    std::string hex;          // "3." followed by hex digits
    std::int64_t decimal_digits = 0;  // decimal digits that hex `digits` give
    std::int64_t decimal_done = 0;    // converted so far
    std::string decimal;              // "3." followed by decimal digits
    std::int64_t shown = 0;
    std::vector<std::string> files;  // the files written, below `output_root`
    std::vector<std::string> hosts;  // names of the hosts that computed chunks
    std::string error;
};

}  // namespace pi_msgs
