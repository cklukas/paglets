// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Ping-pong: a "run" request creates a child paglet from the same module and
// bounces a counter between the two with request/reply until it reaches the
// requested number of rounds. The run request is answered from a later
// handler (deferred reply); then the child is told to dispose itself.

#include <paglets/paglet.hpp>

#include <cstdint>
#include <string>

namespace {

class PingPong : public paglets::Paglet {
public:
    PingPong() {
        // Parent role.
        router().on<std::uint32_t>("run", [this](std::uint32_t rounds, paglets::Message& m) -> std::int32_t {
            if (pending_run_) return paglets::abi::bad_state;
            auto child = paglets::create_child(std::uint32_t{0});
            if (!child) return child.error();
            pong_ = *child;
            target_ = rounds;
            pending_run_ = m.defer_reply();
            bounce(0);
            return paglets::abi::handled;
        });
        // Child role.
        router().on<std::uint32_t>("ping", [](std::uint32_t n, paglets::Message& m) { m.reply(n + 1); });
        router().on("stop", [](paglets::Message&) { (void)paglets::dispose(); });
    }

private:
    void bounce(std::uint32_t n) {
        if (n >= target_) {
            (void)paglets::reply_to(pending_run_, n);
            (void)pong_.send("stop");
            return;
        }
        auto sent = pong_.request("ping", n, [this](paglets::Message& reply) {
            std::uint32_t value = 0;
            if (!reply.ok() || !reply.decode(value)) {
                paglets::log(paglets::abi::LogLevel::error,
                             "ping failed: " + std::string(paglets::abi::error_name(reply.status())));
                (void)pending_run_.drop();
                return;
            }
            bounce(value);
        });
        if (!sent) (void)pending_run_.drop();
    }

    paglets::Endpoint pong_;
    paglets::Capability pending_run_;
    std::uint32_t target_ = 0;
};

}  // namespace

PAGLETS_PAGLET(PingPong)
