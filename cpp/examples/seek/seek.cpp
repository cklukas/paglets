// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Hide and Seek (planning/cpp-demo-paglets.md, demo 15): the seeker
// creates a hider, which keeps moving to other hosts. The seeker finds it
// with the locator wherever it is, pins it (its moves are refused while the
// pin lasts), and releases it again: locate_and_pin under continuous
// movement.

#include "seek.schema.gen.hpp"

#include <paglets/patterns/locate.hpp>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace seek_msgs;
namespace pt = paglets::patterns;
using paglets::Endpoint;
using paglets::Message;

class Seek : public paglets::Paglet {
public:
    Seek() {
        // The seeker.
        router().on<Game>("start", [this](const Game& g, Message& m) { start(g, m); });
        router().on("score", [this](Message& m) { (void)m.reply(score_); });
        router().on("next-round", [this](Message&) { round(); });
        // The hider.
        router().on("hop", [this](Message&) { hop(); });
        router().on("moves", [this](Message& m) { (void)m.reply(moves_); });
        router().on("stop", [this](Message&) { hiding_ = false; });
    }

    void on_created(paglets::Started& s) override {
        // Created by a seeker: hide (args: the hop interval).
        if (!s.decode_args(hop_ms_) || hop_ms_ <= 0) return;
        hiding_ = true;
        (void)paglets::after_raw(hop_ms_, "hop", {});
    }

    void on_arrived(const paglets::abi::ArrivedEvent&) override {
        if (!hiding_) return;
        ++moves_;
        (void)paglets::after_raw(hop_ms_, "hop", {});
    }

    void on_move_failed(const paglets::abi::MoveFailed&) override {
        if (hiding_) (void)paglets::after_raw(hop_ms_, "hop", {});  // pinned, or nowhere to go: later
    }

private:
    // -- the hider ----------------------------------------------------------------------

    void hop() {
        if (!hiding_) return;
        // While pinned, dispatch says so at once: try again later.
        if (!paglets::dispatch("any")) (void)paglets::after_raw(hop_ms_, "hop", {});
    }

    // -- the seeker ---------------------------------------------------------------------

    void start(const Game& g, Message& m) {
        game_ = g;
        score_ = Score{"seeking", {}, 0};
        auto hider = paglets::create_child(g.hop_ms);
        if (!hider) {
            score_.state = "done";
            (void)m.reply(score_);
            return;
        }
        hider_ = *hider;
        (void)m.reply(score_);
        (void)paglets::after_raw(g.pause_ms, "next-round", {});
    }

    // The next round starts once the hider has moved since the last catch,
    // however slow the hosts are.
    void round() {
        if (static_cast<std::int64_t>(score_.catches.size()) >= game_.rounds) return finish();
        hider_moves([this](std::int64_t n) {
            if (!score_.catches.empty() && n >= 0 && n <= caught_at_moves_) {
                (void)paglets::after_raw(game_.pause_ms, "next-round", {});
                return;
            }
            seek();
        });
    }

    void seek() {
        pt::with_pinned(hider_, 10'000, "hide and seek",
                        [this](paglets::Result<pt::Location> where, std::function<void()> done) {
                            Catch c;
                            if (where) {
                                c.host_name = where->host_name;
                                c.moves = where->moves;
                            } else {
                                c.error = std::string(paglets::abi::error_name(where.error()));
                            }
                            score_.catches.push_back(std::move(c));
                            // Pinned, it stays where it is while it counts.
                            hider_moves([this, done](std::int64_t n) {
                                caught_at_moves_ = n;
                                done();  // let it go
                                (void)paglets::after_raw(game_.pause_ms, "next-round", {});
                            });
                        });
    }

    // How often the hider has moved (-1: no answer).
    void hider_moves(std::function<void(std::int64_t)> next) {
        paglets::RequestOptions o;
        o.timeout_ms = 5000;
        auto sent = hider_.request(
            "moves",
            [next](Message& reply) {
                std::int64_t n = -1;
                if (!reply.ok() || !reply.decode(n)) n = -1;
                next(n);
            },
            std::move(o));
        if (!sent) next(-1);
    }

    void finish() {
        paglets::RequestOptions o;
        o.timeout_ms = 5000;
        auto sent = hider_.request(
            "moves",
            [this](Message& reply) {
                std::int64_t n = 0;
                if (reply.ok() && reply.decode(n)) score_.hider_moves = n;
                (void)hider_.send("stop");
                score_.state = "done";
            },
            std::move(o));
        if (!sent) score_.state = "done";
    }

    // The seeker.
    Game game_;
    Score score_{"idle", {}, 0};
    Endpoint hider_;
    std::int64_t caught_at_moves_ = -1;
    // The hider.
    bool hiding_ = false;
    std::int64_t hop_ms_ = 0;
    std::int64_t moves_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Seek)
