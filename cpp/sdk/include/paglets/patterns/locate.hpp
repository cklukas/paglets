// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): locate and pin paglets through the `locator` system
// paglet (planning/cpp-location.md). A pin keeps the paglet on its host;
// with_pinned() pins, runs some work, and releases the pin when the work
// says it is done.

#pragma once

#include <paglets/patterns/services.hpp>
#include <paglets/services/locator.gen.hpp>

#include <functional>
#include <memory>
#include <string>

namespace paglets::patterns {

using Location = services::locator::Location;

// Where the paglet behind `target` is (the endpoint is lent to the locator).
inline void locate(const Endpoint& target, std::function<void(Result<Location>)> next) {
    auto loc = service("locator");
    if (!loc) return next(std::unexpected(loc.error()));
    services::locator::Client client{*loc};
    auto sent = client.locate({}, [next](Result<Location> r, Message&) { next(std::move(r)); }, lending(target));
    if (!sent) next(std::unexpected(sent.error()));
}

struct Pinned {
    services::locator::Pin pin;
    Capability capability;  // lend it to release()
};

// Pins the paglet where it is for `duration_ms` (at most what the policy
// allows).
inline void pin(const Endpoint& target, std::int64_t duration_ms, std::string reason,
                std::function<void(Result<Pinned>)> next) {
    auto loc = service("locator");
    if (!loc) return next(std::unexpected(loc.error()));
    services::locator::Client client{*loc};
    auto sent = client.locate_and_pin(
        services::locator::PinRequest{duration_ms, std::move(reason)},
        [next](Result<services::locator::Pin> r, Message& m) {
            if (!r) return next(std::unexpected(r.error()));
            if (m.cap_count() == 0) return next(std::unexpected(abi::internal));
            next(Pinned{std::move(*r), m.take_cap(0)});
        },
        lending(target));
    if (!sent) next(std::unexpected(sent.error()));
}

inline void release(Capability pin_cap, std::function<void(Result<bool>)> next = {}) {
    auto loc = service("locator");
    if (!loc) {
        if (next) next(std::unexpected(loc.error()));
        return;
    }
    services::locator::Client client{*loc};
    auto keep = std::make_shared<Capability>(pin_cap);
    auto sent = client.release(
        {},
        [next, keep](Result<services::locator::ReleaseReply> r, Message&) {
            (void)keep->drop();
            if (!next) return;
            if (!r) return next(std::unexpected(r.error()));
            next(r->released);
        },
        lending(*keep));
    if (!sent && next) next(std::unexpected(sent.error()));
}

// Pins `target`, runs `work` with its location and a `done` callback, and
// releases the pin when `done` is called (or when pinning failed, `work`
// gets the error and no pin is held).
inline void with_pinned(const Endpoint& target, std::int64_t duration_ms, std::string reason,
                        std::function<void(Result<Location>, std::function<void()> done)> work) {
    pin(target, duration_ms, std::move(reason), [work](Result<Pinned> p) {
        if (!p) return work(std::unexpected(p.error()), [] {});
        auto cap = std::make_shared<Capability>(p->capability);
        work(p->pin.location, [cap] { release(*cap); });
    });
}

}  // namespace paglets::patterns
