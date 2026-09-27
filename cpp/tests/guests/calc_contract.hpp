// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A test service contract: served natively by the host and by the calc_user
// guest, called by both (tests/test_contract.cpp).

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace calc {

struct AddRequest {
    std::int64_t a = 0;
    std::int64_t b = 0;
};

struct AddReply {
    std::int64_t sum = 0;
};

struct DivRequest {
    std::int64_t a = 0;
    std::int64_t b = 0;
};

struct DivReply {
    std::int64_t quotient = 0;
    std::string served_by;
};

struct Contract {
    static constexpr std::string_view service = "calc";
    AddReply add(const AddRequest&);
    // invalid_argument for b == 0.
    DivReply divide(const DivRequest&);
};

}  // namespace calc
