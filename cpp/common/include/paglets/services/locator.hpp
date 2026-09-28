// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `locator` system paglet (planning/cpp-location.md): where
// a paglet is, and pins that keep it there for a while.
//
// - `locate`: the caller lends an endpoint to the paglet (with the `locate`
//   right, or `*`); the answer names the host that holds it and when it
//   last moved.
// - `locate_and_pin`: the same with the `pin` right (or `*`); the host that
//   holds the paglet pins it, following it if it moved in between. The
//   reply's first capability is the `pin` capability; the pin lasts at most
//   what the mesh policy allows (a rule for service `locator`, operation
//   `pin`). While pinned the paglet cannot leave its host: its dispatch
//   returns `pinned`.
// - `release`: the caller lends the `pin` capability; the pin ends at once.
//   Pins also end when they expire, and admins can end them.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace paglets::services::locator {

struct LocateRequest {};

struct Location {
    std::string paglet;
    std::string host;           // key ID of the host that holds it
    std::string host_name;      // its name in the ledger
    std::int64_t moved_ms = 0;  // when it arrived there (or was created), Unix milliseconds
    std::int64_t moves = 0;     // how often it moved
};

struct PinRequest {
    std::int64_t duration_ms = 60'000;
    std::string reason;
};

struct Pin {
    Location location;
    std::string pin;  // ID of the pin
    std::int64_t expires_ms = 0;
};

struct ReleaseRequest {};

struct ReleaseReply {
    bool released = false;  // false: the pin had ended already
};

struct Contract {
    static constexpr std::string_view service = "locator";
    Location locate(const LocateRequest&);
    Pin locate_and_pin(const PinRequest&);
    ReleaseReply release(const ReleaseRequest&);
};

}  // namespace paglets::services::locator
