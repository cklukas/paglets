// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Exercises the patterns library of the guest SDK (WP18): a task, a typed
// operation, mesh fan-out, single-file mobility, locate and pin, and
// notifications.

#include "patterns_demo.schema.gen.hpp"

#include <paglets/patterns.hpp>

#include <memory>
#include <string>

namespace {

using namespace patterns_msgs;
namespace pt = paglets::patterns;
using paglets::Message;

class Demo : public pt::Task<CountRequest, CountResult> {
public:
    Demo() {
        pt::serve<AddRequest, AddReply>(router(), "add", [](const AddRequest& q) -> paglets::Result<AddReply> {
            return AddReply{q.a + q.b};
        });
        router().on("count-done", [this](Message&) { finish_count(); });

        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            std::int64_t v = 0;
            if (c.ok && paglets::decode(c.result, v)) {
                spread_.results.push_back(c.host_name + "=" + std::to_string(v));
            } else {
                spread_.results.push_back(c.destination + "!" + c.error);
            }
            spread_.finished = fan_.finished();
            spread_.succeeded = static_cast<std::int64_t>(fan_.succeeded());
        });
        router().on<SpreadRequest>("spread", [this](const SpreadRequest& q, Message& m) {
            spread_ = SpreadStatus{};
            fan_.start(q.hosts, paglets::encode(q.value), q.timeout_ms);
            (void)m.reply(spread_);
        });
        router().on("spread_status", [this](Message& m) { (void)m.reply(spread_); });

        router().on<CarryRequest>("carry", [this](const CarryRequest& q, Message& m) {
            carry_ = q;
            carried_ = CarryStatus{"carrying", 0, {}};
            (void)m.reply(carried_);
            pt::pick_up(q.from_root, q.path, 1024 * 1024, [this](paglets::Result<pt::CarriedFile> f) {
                if (!f) return carry_failed(pt::error_text("pick up", f.error()));
                file_ = std::move(*f);
                carried_.size = static_cast<std::int64_t>(file_.data.size());
                if (auto moved = paglets::dispatch(carry_.to_host); !moved)
                    carry_failed(pt::error_text("dispatch", moved.error()));
            });
        });
        router().on("carry_status", [this](Message& m) { (void)m.reply(carried_); });

        // Locating and pinning may ask other hosts: the answers are kept
        // and read with `pin_report` and `where_report`.
        router().on("pin_self", [this](Message& m) {
            pinned_ = PinReport{};
            (void)m.reply(true);
            pt::with_pinned(paglets::self(), 60'000, "test",
                            [this](paglets::Result<pt::Location> where, std::function<void()> done) {
                                PinReport r;
                                r.done = true;
                                if (where) {
                                    r.host_name = where->host_name;
                                    r.moves = where->moves;
                                    auto moved = paglets::dispatch("nowhere");
                                    r.dispatch_while_pinned =
                                        moved ? "ok" : std::string(paglets::abi::error_name(moved.error()));
                                    r.released = true;
                                } else {
                                    r.dispatch_while_pinned = std::string(paglets::abi::error_name(where.error()));
                                }
                                done();
                                pinned_ = r;
                            });
        });
        router().on("pin_report", [this](Message& m) { (void)m.reply(pinned_); });
        router().on("where", [this](Message& m) {
            where_.clear();
            (void)m.reply(true);
            pt::locate(paglets::self(), [this](paglets::Result<pt::Location> l) {
                where_ = l ? l->host_name : pt::error_text("locate", l.error());
            });
        });
        router().on("where_report", [this](Message& m) { (void)m.reply(where_); });
        router().on<NotifyRequest>("notify", [](const NotifyRequest& q, Message& m) {
            pt::notify(pt::Level::success, q.title, "from the patterns demo");
            (void)m.reply(true);
        });
    }

    void on_cloned(paglets::Started& s) override {
        // A fan-out child: squares the value and reports to the parent.
        std::int64_t v = 0;
        if (s.caps.empty() || !s.decode_args(v)) {
            (void)paglets::dispose();
            return;
        }
        paglets::Endpoint parent(s.caps[0].release());
        pt::report(parent, paglets::encode(v * v));
        (void)paglets::after_raw(2000, "child-done", {});
        router().on("child-done", [](Message&) { (void)paglets::dispose(); });
    }

    void on_arrived(const paglets::abi::ArrivedEvent&) override {
        if (carried_.state != "carrying") return;
        pt::put_down(file_, carry_.to_root, carry_.to_path, [this](paglets::Result<void> r) {
            if (!r) return carry_failed(pt::error_text("put down", r.error()));
            carried_.state = "delivered";
            file_ = {};
        });
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) return fan_.clone_failed(f);
        if (carried_.state == "carrying") carry_failed("dispatch: " + f.reason);
    }

protected:
    void run(const CountRequest& q) override {
        request_ = q;
        if (q.n < 0) return fail("n is negative");
        if (q.delay_ms > 0) {
            (void)paglets::after_raw(q.delay_ms, "count-done", {});
        } else {
            finish_count();
        }
    }

private:
    void finish_count() {
        std::int64_t sum = 0;
        for (std::int64_t i = 1; i <= request_.n; ++i) sum += i;
        complete(CountResult{sum});
    }

    void carry_failed(std::string why) {
        carried_.state = "failed";
        carried_.error = std::move(why);
    }

    CountRequest request_;
    pt::FanOut fan_;
    SpreadStatus spread_;
    CarryRequest carry_;
    CarryStatus carried_{"idle", 0, {}};
    pt::CarriedFile file_;
    PinReport pinned_;
    std::string where_;
};

}  // namespace

PAGLETS_PAGLET(Demo)
