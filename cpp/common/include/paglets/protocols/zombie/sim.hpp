// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The zombie protocol under timed random faults: the same step() as the
// guest SDK, with virtual clocks, message latency, timers with the real
// durations, a worker that makes progress and observations, hosts that
// crash and come back, links that are cut and healed, lost and duplicated
// messages. A run has a fault phase and a quiet phase; every run is
// reproducible from its seed.
//
// Each run checks: at most one result is accepted; every replica ends; the
// delivered journal holds every observation that was durable anywhere when
// the result was delivered; every role change is in the table; and, after a
// main crashed while another replica was up, and nothing else happened
// (no fault, restart or heal) until a main worked again, that took at most
// takeover_bound().

#pragma once

#include <paglets/protocols/zombie/zombie.hpp>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

namespace paglets::protocols::zombie {

struct SimConfig {
    std::uint32_t replicas = 3;
    std::uint32_t late_hosts = 1;  // of these, hosts that join the mesh during the fault phase
    std::int64_t beat_ms = 2000;
    std::int64_t checkpoint_ms = 10000;
    std::int64_t suspect_ms = 10000;
    std::int64_t suspect_jitter_ms = 5000;
    std::int64_t round_ms = 4000;
    std::int64_t image_ms = 1000;     // the runtime's checkpoint interval
    std::int64_t gossip_ms = 2000;    // registry anti-entropy
    std::int64_t latency_min_ms = 1;
    std::int64_t latency_max_ms = 50;
    std::int64_t work_ms = 1000;      // one progress step
    std::int64_t work = 300;          // steps until done
    std::int64_t observe_every = 10;  // an observation every N steps
    std::int64_t fault_phase_ms = 180'000;
    std::int64_t fault_every_ms = 15'000;  // mean time between faults
    double loss = 0.02;
    double duplicate = 0.01;
    std::int64_t limit_ms = 1'200'000;  // a run must end before this
    bool trace = false;                 // record role changes and faults
};

// How long the group may be without a working main after its main crashed:
// the suspect timeout with jitter, the round, a second round of another
// candidate that pauses the new main, a beat and a few messages.
inline std::int64_t takeover_bound(const SimConfig& c) {
    return c.suspect_ms + c.suspect_jitter_ms + 2 * c.round_ms + c.beat_ms + 4 * c.latency_max_ms;
}

struct SimRun {
    std::uint64_t seed = 0;
    std::vector<std::string> failures;
    std::int64_t ended_ms = -1;          // when the last replica ended
    std::int64_t takeover_max_ms = 0;    // longest checked gap without a main
    std::uint64_t takeovers = 0;         // checked gaps
    std::int64_t two_mains_ms = 0;       // time with more than one working main
    std::uint64_t faults = 0;
    std::uint64_t messages = 0;
    std::vector<std::string> trace;  // with SimConfig::trace
};

class Simulation {
public:
    Simulation(SimConfig c, std::uint64_t seed) : c_(c), rng_(seed) { run_.seed = seed; }

    SimRun run() {
        const ReplicaId early = c_.replicas - std::min(c_.late_hosts, c_.replicas - 1);
        std::vector<ReplicaId> members;
        for (ReplicaId r = 0; r < early; ++r) members.push_back(r);
        for (ReplicaId r = 0; r < c_.replicas; ++r) {
            Host h;
            h.joined = r < early;
            h.state = initial(r, members);
            hosts_.push_back(std::move(h));
        }
        links_.assign(c_.replicas * c_.replicas, true);
        for (ReplicaId r = 0; r < early; ++r) apply(r, Start{r == 0, {}});
        for (ReplicaId r = early; r < c_.replicas; ++r) at(uniform(5'000, c_.fault_phase_ms), [this, r] { join(r); });
        at(c_.gossip_ms, [this] { gossip(); });
        for (ReplicaId r = 0; r < c_.replicas; ++r) at(c_.image_ms, [this, r] { image(r); });
        at(next_fault(), [this] { fault(); });
        while (!queue_.empty()) {
            auto ev = queue_.top();
            queue_.pop();
            account(ev.time);
            now_ = ev.time;
            if (now_ > c_.limit_ms) break;
            ev.action();
            if (all_ended()) break;
        }
        if (!all_ended()) fail("not every replica ended within the limit");
        if (sink_accepted_ < 0) fail("no result was delivered");
        run_.ended_ms = now_;
        return run_;
    }

private:
    struct Host {
        bool up = true;
        bool joined = true;  // false: not in the mesh yet
        State state;
        State durable;
        std::array<std::uint64_t, timer_count> timer_gen{};
        std::uint64_t work_gen = 0;
        std::uint64_t incarnation_gen = 0;  // bumped on crash: pending events die
        std::uint8_t tomb = 0;
    };
    struct Event {
        std::int64_t time;
        std::uint64_t order;
        std::function<void()> action;
        bool operator<(const Event& o) const { return std::tie(time, order) > std::tie(o.time, o.order); }
    };

    void at(std::int64_t delay, std::function<void()> f) { queue_.push({now_ + delay, order_++, std::move(f)}); }
    void note(const std::string& what) {
        if (!c_.trace) return;
        std::ostringstream os;
        os << now_ / 1000.0 << " s  " << what;
        run_.trace.push_back(os.str());
    }
    void fail(std::string what) {
        note("FAILED: " + what);
        if (std::ranges::find(run_.failures, what) == run_.failures.end()) run_.failures.push_back(std::move(what));
    }
    std::int64_t uniform(std::int64_t lo, std::int64_t hi) { return std::uniform_int_distribution<std::int64_t>(lo, hi)(rng_); }
    bool chance(double p) { return std::bernoulli_distribution(p)(rng_); }
    std::int64_t latency() { return uniform(c_.latency_min_ms, c_.latency_max_ms); }
    bool linked(ReplicaId a, ReplicaId b) const { return links_[a * c_.replicas + b]; }
    bool all_ended() const {
        return std::ranges::all_of(hosts_, [](const Host& h) { return !h.joined || (h.up && h.state.role == Role::ended); });
    }
    bool working(const Host& h) const { return h.joined && h.up && h.state.role == Role::main && !h.state.frozen; }

    // A host joins the mesh: the acting main makes a replica there (as the
    // SDK does) and adds it to its membership; without one, it waits.
    void join(ReplicaId r) {
        for (ReplicaId m = 0; m < c_.replicas; ++m) {
            if (!working(hosts_[m])) continue;
            note("host " + std::to_string(r) + " joins; replica made by the main on host " + std::to_string(m));
            ++changes_;
            Host& h = hosts_[r];
            h.joined = true;
            h.state = initial(r, hosts_[m].state.members);
            h.durable = h.state;
            apply(r, Start{false, {}});
            apply(m, AddMembers{{r}});
            return;
        }
        if (std::ranges::any_of(hosts_, [](const Host& h) { return h.joined && h.state.role != Role::ended; }))
            at(5000, [this, r] { join(r); });
    }
    std::uint32_t mains() const {
        std::uint32_t n = 0;
        for (const auto& h : hosts_) n += working(h);
        return n;
    }

    // Time accounting: gaps without a working main, and time with two.
    void account(std::int64_t t) {
        const auto m = mains();
        if (m > 1) run_.two_mains_ms += t - now_;
    }
    void observe_mains(std::optional<std::int32_t> crashed_main) {
        const auto m = mains();
        const bool done = std::ranges::any_of(hosts_, [](const Host& h) {
            return h.state.role == Role::delivering || h.state.role == Role::ended || h.state.done;
        });
        if (m == 0 && gap_start_ < 0 && !done) {
            gap_start_ = now_;
            // Checked only if another replica was there to take over.
            gap_clean_ = crashed_main.has_value() && std::ranges::any_of(hosts_, [](const Host& h) {
                return h.up && h.state.role != Role::ended;
            });
            gap_faults_ = changes_;
        } else if (m > 0 && gap_start_ >= 0) {
            if (gap_clean_ && gap_faults_ == changes_) {
                const auto gap = now_ - gap_start_;
                ++run_.takeovers;
                run_.takeover_max_ms = std::max(run_.takeover_max_ms, gap);
                const auto bound = takeover_bound(c_);
                if (gap > bound) fail("a takeover took " + std::to_string(gap) + " ms, longer than the bound");
            }
            gap_start_ = -1;
        }
    }

    std::int64_t timer_ms(Timer t) {
        switch (t) {
            case Timer::beat: return c_.beat_ms;
            case Timer::checkpoint: return c_.checkpoint_ms;
            case Timer::suspect: return c_.suspect_ms + uniform(0, c_.suspect_jitter_ms);
            case Timer::round: return c_.round_ms;
        }
        return 1000;
    }

    void apply(ReplicaId r, const Input& in) {
        Host& h = hosts_[r];
        if (!h.up || !h.joined) return;
        const Outputs out = step(h.state, in);
        for (const auto& o : out) {
            if (const auto* s = std::get_if<Send>(&o)) {
                send(r, s->to, s->message);
            } else if (std::holds_alternative<Persist>(o) || std::holds_alternative<End>(o)) {
                h.durable = h.state;
            } else if (const auto* t = std::get_if<SetTimer>(&o)) {
                const auto i = static_cast<std::size_t>(t->timer);
                const auto gen = ++h.timer_gen[i];
                const auto inc = h.incarnation_gen;
                at(timer_ms(t->timer), [this, r, i, gen, inc] {
                    Host& x = hosts_[r];
                    if (x.up && x.incarnation_gen == inc && x.timer_gen[i] == gen) apply(r, Fired{static_cast<Timer>(i)});
                });
            } else if (const auto* t2 = std::get_if<StopTimer>(&o)) {
                ++h.timer_gen[static_cast<std::size_t>(t2->timer)];
            } else if (const auto* w = std::get_if<StartWork>(&o)) {
                work_progress_[r] = w->progress;
                const auto gen = ++h.work_gen;
                schedule_work(r, gen);
            } else if (std::holds_alternative<StopWork>(o)) {
                ++h.work_gen;
            } else if (const auto* d = std::get_if<Deliver>(&o)) {
                deliver(r, *d);
            } else if (std::holds_alternative<Tombstone>(o)) {
                if (h.tomb == 0) h.tomb = 1;
            } else if (const auto* tr = std::get_if<Transition>(&o)) {
                if (!is_allowed(tr->from, tr->cause, tr->to)) fail("a role change outside the table");
                if (c_.trace) {
                    std::ostringstream os;
                    os << "replica " << r << ": " << to_string(tr->from) << " -> " << to_string(tr->to) << " ("
                       << to_string(tr->cause) << ", rank " << h.state.rank.progress << "/" << h.state.rank.term << ")";
                    note(os.str());
                }
            }
        }
        observe_mains(std::nullopt);
    }

    void schedule_work(ReplicaId r, std::uint64_t gen) {
        const auto inc = hosts_[r].incarnation_gen;
        at(c_.work_ms, [this, r, gen, inc] {
            Host& h = hosts_[r];
            if (!h.up || h.incarnation_gen != inc || h.work_gen != gen) return;
            const auto p = ++work_progress_[r];
            if (p % c_.observe_every == 0) {
                std::ostringstream os;
                os << "observation " << r << "@" << now_;
                const auto text = os.str();
                apply(r, Observed{Bytes(text.begin(), text.end())});
            }
            if (hosts_[r].work_gen != gen) return;
            apply(r, Worked{p, p >= c_.work, Bytes{static_cast<std::uint8_t>(p & 0xff)}});
            if (hosts_[r].work_gen == gen) schedule_work(r, gen);
        });
    }

    void send(ReplicaId from, ReplicaId to, const Message& m) {
        ++run_.messages;
        if (to >= hosts_.size() || !hosts_[to].joined || !linked(from, to)) return;
        const bool faulty = now_ < c_.fault_phase_ms;
        if (faulty && chance(c_.loss)) return;
        const int copies = faulty && chance(c_.duplicate) ? 2 : 1;
        for (int i = 0; i < copies; ++i) {
            const auto inc = hosts_[to].incarnation_gen;
            at(latency(), [this, from, to, m, inc] {
                Host& h = hosts_[to];
                if (h.up && h.incarnation_gen == inc && linked(from, to)) apply(to, Received{from, m});
            });
        }
    }

    void deliver(ReplicaId r, const Deliver&) {
        // The result: the main's latest work and its journal.
        const Journal journal = hosts_[r].state.journal;
        const auto inc = hosts_[r].incarnation_gen;
        at(latency(), [this, r, journal, inc] {
            const bool accepted = sink_accepted_ < 0;
            if (accepted) {
                sink_accepted_ = static_cast<std::int32_t>(r);
                for (const auto& h : hosts_) {
                    if (!h.up || h.durable.role == Role::ended) continue;
                    for (const auto& [k, d] : h.durable.journal.entries) {
                        if (!journal.entries.contains(k)) fail("an observation durable at delivery is missing in the result");
                    }
                }
            }
            at(latency(), [this, r, accepted, inc] {
                if (hosts_[r].incarnation_gen == inc) apply(r, SinkAnswer{1, accepted});
            });
        });
    }

    void image(ReplicaId r) {
        Host& h = hosts_[r];
        if (h.up) h.durable = h.state;
        at(c_.image_ms, [this, r] { image(r); });
    }

    void gossip() {
        for (ReplicaId a = 0; a < c_.replicas; ++a) {
            for (ReplicaId b = 0; b < c_.replicas; ++b) {
                if (a == b || !linked(a, b) || !hosts_[a].up || !hosts_[b].up) continue;
                const auto t = std::max(hosts_[a].tomb, hosts_[b].tomb);
                hosts_[a].tomb = hosts_[b].tomb = t;
            }
        }
        for (ReplicaId r = 0; r < c_.replicas; ++r) {
            if (hosts_[r].tomb && hosts_[r].up && hosts_[r].state.role != Role::ended) apply(r, RegistryUpdate{true, 1});
        }
        at(c_.gossip_ms, [this] { gossip(); });
    }

    std::int64_t next_fault() { return std::exponential_distribution<double>(1.0 / double(c_.fault_every_ms))(rng_) + 1; }

    void fault() {
        if (now_ >= c_.fault_phase_ms) return;
        ++run_.faults;
        ++changes_;
        const auto r = static_cast<ReplicaId>(uniform(0, c_.replicas - 1));
        if (!hosts_[r].joined) {
            at(next_fault(), [this] { fault(); });
            return;
        }
        if (chance(0.5)) {
            Host& h = hosts_[r];
            if (h.up) {
                const bool was_main = working(h);
                note("crash of host " + std::to_string(r) + (was_main ? " (the main)" : ""));
                h.up = false;
                ++h.incarnation_gen;
                observe_mains(was_main ? std::optional<std::int32_t>(r) : std::nullopt);
                at(uniform(5'000, 60'000), [this, r] {
                    Host& x = hosts_[r];
                    note("restart of host " + std::to_string(r));
                    ++changes_;
                    x.up = true;
                    x.state = x.durable;
                    x.timer_gen = {};
                    apply(r, Recovered{});
                    if (x.tomb) apply(r, RegistryUpdate{true, 1});
                });
            }
        } else {
            auto other = static_cast<ReplicaId>(uniform(0, c_.replicas - 2));
            if (other >= r) ++other;
            if (linked(r, other)) {
                links_[r * c_.replicas + other] = links_[other * c_.replicas + r] = false;
                note("cut " + std::to_string(r) + "-" + std::to_string(other));
                at(uniform(5'000, 90'000), [this, r, other] {
                    links_[r * c_.replicas + other] = links_[other * c_.replicas + r] = true;
                    note("heal " + std::to_string(r) + "-" + std::to_string(other));
                    ++changes_;
                });
            }
        }
        at(next_fault(), [this] { fault(); });
    }

    SimConfig c_;
    std::mt19937_64 rng_;
    SimRun run_;
    std::vector<Host> hosts_;
    std::vector<bool> links_;
    std::map<ReplicaId, std::int64_t> work_progress_;
    std::priority_queue<Event> queue_;
    std::int64_t now_ = 0;
    std::uint64_t order_ = 0;
    std::int32_t sink_accepted_ = -1;
    std::int64_t gap_start_ = -1;
    bool gap_clean_ = false;
    std::uint64_t gap_faults_ = 0;
    std::uint64_t changes_ = 0;  // faults, restarts and heals so far
};

struct SimSummary {
    std::uint64_t runs = 0;
    std::vector<std::pair<std::uint64_t, std::string>> failures;  // seed, what
    std::int64_t takeover_max_ms = 0;
    std::uint64_t takeovers = 0;
    std::int64_t two_mains_ms = 0;
    std::uint64_t faults = 0;
    std::uint64_t messages = 0;
    std::int64_t longest_run_ms = 0;

    std::string text() const {
        std::ostringstream os;
        os << (failures.empty() ? "ok    " : "FAIL  ") << runs << " timed runs, " << faults << " faults, " << messages
           << " messages\n"
           << "      takeovers after a main crashed: " << takeovers << ", longest " << takeover_max_ms / 1000.0
           << " s\n"
           << "      time with two working mains (partitions): " << two_mains_ms / 1000.0 << " s in total\n"
           << "      longest run until every replica ended: " << longest_run_ms / 1000.0 << " s\n";
        for (const auto& [seed, what] : failures) os << "      seed " << seed << ": " << what << "\n";
        return os.str();
    }
    std::vector<std::tuple<std::string, std::string, std::string>> values() const {
        auto fmt = [](double v) {
            std::ostringstream os;
            os.precision(1);
            os << std::fixed << v;
            return os.str();
        };
        return {{"runs", std::to_string(runs), ""},
                {"faults (crashes, cuts)", std::to_string(faults), ""},
                {"takeovers after a main crashed", std::to_string(takeovers), ""},
                {"longest takeover", fmt(takeover_max_ms / 1000.0), "s"},
                {"time with two working mains", fmt(two_mains_ms / 1000.0), "s"},
                {"failed runs", std::to_string(failures.size()), ""}};
    }
};

inline SimSummary simulate_many(const SimConfig& c, std::uint64_t seeds, std::uint64_t first = 1) {
    SimSummary s;
    for (std::uint64_t seed = first; seed < first + seeds; ++seed) {
        const auto r = Simulation(c, seed).run();
        ++s.runs;
        for (const auto& f : r.failures) s.failures.emplace_back(seed, f);
        s.takeover_max_ms = std::max(s.takeover_max_ms, r.takeover_max_ms);
        s.takeovers += r.takeovers;
        s.two_mains_ms += r.two_mains_ms;
        s.faults += r.faults;
        s.messages += r.messages;
        s.longest_run_ms = std::max(s.longest_run_ms, r.ended_ms);
    }
    return s;
}

}  // namespace paglets::protocols::zombie
