// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "conformance_cmd.hpp"
#include <paglets/patterns/zombie.hpp>

namespace {
// A normal C++ copy aliases this member. Step isolation must use the wire
// state, so a stale callback cannot mutate a newer lineage through it.
struct ProbeWork {
    std::shared_ptr<std::int64_t> value;
    ProbeWork(std::int64_t n = 0) : value(std::make_shared<std::int64_t>(n)) {}
};
void paglets_encode(paglets::msgpack::Writer& w, const ProbeWork& work) {
    paglets::msgpack::write_value(w, *work.value);
}
bool paglets_decode(paglets::msgpack::Reader& r, ProbeWork& work) {
    std::int64_t n = 0;
    if (!paglets::msgpack::read_value(r, n)) return false;
    work.value = std::make_shared<std::int64_t>(n);
    return true;
}
class Probe final : public paglets::patterns::Zombie<ProbeWork> {
public:
    Probe() {
        router().on<conformance::Cmd>("start", [this](const auto& q, paglets::Message& m) {
            can_finish_ = q.code == 0;
            hold_steps_ = q.code == 2;
            paglets::patterns::ZombieTiming timing;
            timing.beat_ms = 30;
            timing.checkpoint_ms = 50;
            timing.suspect_ms = 500;
            timing.jitter_ms = 0;
            timing.round_ms = 100;
            timing.step_ms = 30;
            timing.expand_ms = 0;
            start_group(q.name, q.ms, timing);
            (void)m.reply(group());
        });
        router().on("recover_step", [this](paglets::Message& m) {
            on_recovered({});
            (void)m.reply();
        });
        router().on("release_old_step", [this](paglets::Message& m) {
            if (!steps_.empty()) {
                last_step_ = std::move(steps_.front());
                steps_.erase(steps_.begin());
                last_step_();
            }
            (void)m.reply();
        });
        router().on("repeat_last_step", [this](paglets::Message& m) {
            if (last_step_) last_step_();
            (void)m.reply();
        });
        router().on("step_work", [this](paglets::Message& m) {
            (void)m.reply(std::vector<std::int64_t>{*work().value, static_cast<std::int64_t>(steps_.size())});
        });
        router().on("finish", [this](paglets::Message& m) { can_finish_ = true; (void)m.reply(); });
        router().on("probe_status", [this](paglets::Message& m) {
            (void)m.reply(std::vector<std::int64_t>{static_cast<std::int64_t>(replica().role),
                static_cast<std::int64_t>(replica().rank.term), result_calls_});
        });
        router().on<conformance::Cmd>("send", [](const auto& q, paglets::Message& m) {
            auto sent = paglets::Endpoint(paglets::Capability(q.handle)).send_raw(q.name, q.payload);
            (void)m.reply(sent ? 0 : sent.error());
        });
    }
    void step(ProbeWork& work, std::function<void(bool)> done) override {
        if (hold_steps_) {
            steps_.push_back([&work, done = std::move(done)] { *work.value += 100; done(false); });
        } else {
            done(can_finish_);
        }
    }
    std::int64_t progress(const ProbeWork&) const override { return 1; }
    paglets::Bytes result(const ProbeWork& work, const std::vector<paglets::protocols::zombie::Entry>&) override {
        ++result_calls_;
        return paglets::Bytes(static_cast<std::size_t>(*work.value), 'x');
    }
    void on_zombie_failed(const std::string& why) override { paglets::log("probe-failure:" + why); }
private:
    bool can_finish_ = true;
    bool hold_steps_ = false;
    std::vector<std::function<void()>> steps_;
    std::function<void()> last_step_;
    std::int64_t result_calls_ = 0;
};
}
PAGLETS_PAGLET(Probe)
