// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Service contract of the adder paglet. The schema generator turns it into
// MessagePack codecs, a typed adder::Client and adder::serve().

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace adder {

struct AddRequest {
    std::int64_t a = 0;
    std::int64_t b = 0;
};

struct AddReply {
    std::int64_t sum = 0;
};

struct HostRequest {};

struct HostReply {
    std::string os;  // of the host the paglet runs on
};

struct Contract {
    static constexpr std::string_view service = "adder";
    AddReply add(const AddRequest&);
    HostReply host(const HostRequest&);
};

}  // namespace adder
