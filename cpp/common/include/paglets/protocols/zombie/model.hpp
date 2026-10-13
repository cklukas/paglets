// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The zombie protocol's world, for the model checker (paglets/verify): a
// few hosts, each with one replica, its durable state and the host's
// registry (tombstone, sink number); the sinks; the links between hosts; and
// the messages in flight. Faults are steps like any other, limited by a
// budget: hosts crash (and lose what was not durable and what was sent to
// them), links are cut, messages are lost or delivered twice, sinks go down,
// the owner cancels or redirects. Messages travel in one ordered channel
// per sender and receiver, as the transport's queues per peer do; loss can
// hit any of them. A message equal to one still queued on its channel is
// merged with it (periodic resends; double delivery is a fault of its own). Timers fire at any time: safety
// does not depend on timing.
//
// Recovery steps are not faults: restarting a host, healing a link, a sink
// coming back, the registries gossiping, timers, delivery, work. The goals
// (verify/explorer.hpp) are checked over these steps.

#pragma once

#include <paglets/protocols/zombie/zombie.hpp>
#include <paglets/verify/explorer.hpp>

#include <atomic>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <tuple>

namespace paglets::protocols::zombie {

struct ModelConfig {
    std::uint32_t replicas = 3;
    // Of these, hosts that join the mesh while the group runs: the acting
    // main makes a replica there and adds it to the membership.
    std::uint32_t late_hosts = 0;
    std::int64_t work = 1;              // progress steps until the work is done
    std::uint32_t observations = 1;     // entries the mains may observe in total
    std::uint32_t crashes = 1;
    std::uint32_t cuts = 1;
    std::uint32_t drops = 1;
    std::uint32_t duplicates = 1;
    std::uint32_t sink_downs = 0;
    std::uint32_t cancels = 0;
    std::uint32_t redirects = 0;
    // Timeouts that fire although what they detect is not true (the main is
    // alive and reachable, answers to a round are still on their way).
    std::uint32_t early_timeouts = 1;
    std::size_t in_flight = 3;          // messages in flight per channel at most (more are lost)
};

class Model {
public:
    // A packet in flight: a message between replicas, a result for a sink,
    // or a sink's answer.
    struct Packet {
        std::uint8_t kind = 0;  // 0 message, 1 result, 2 answer
        ReplicaId from = 0;
        ReplicaId to = 0;
        Message message;
        std::uint64_t sink = 1;
        bool accepted = false;
    };
    struct Host {
        bool up = true;
        zombie::State replica;
        zombie::State durable;
        std::uint8_t tomb = 0;  // 0 none, 1 delivered, 2 cancelled
        std::uint64_t sink = 1;
        bool joined = true;  // false: the host is not in the mesh yet
    };
    struct Sink {
        bool up = true;
        std::int32_t accepted = -1;  // replica that delivered, -1 none
    };
    struct World {
        std::vector<Host> hosts;
        std::vector<Sink> sinks;  // sink k is sinks[k - 1]
        std::uint32_t links = 0;  // bit set: link cut
        // Channels (sender, receiver) in order: the transport keeps one
        // ordered queue per peer. Key: channel_of(packet).
        std::map<std::uint32_t, std::vector<std::string>> net;
        std::uint32_t crashes = 0, cuts = 0, drops = 0, duplicates = 0, sink_downs = 0, cancels = 0,
                      redirects = 0, observations = 0,
                      early_timeouts = 0;
        // Ghosts (for the properties): the best durable rank and every entry
        // that was ever durable; a role change step() must not make.
        Rank best;
        std::set<EntryKey> kept;
        bool illegal = false;
    };
    using State = World;
    struct Label {
        const char* what = "";
        std::int32_t a = -1;
        std::int32_t b = -1;
        std::string detail;
    };

    explicit Model(ModelConfig config) : c_(config) {}

    State initial() const {
        State w;
        const ReplicaId early = c_.replicas - std::min(c_.late_hosts, c_.replicas - 1);
        std::vector<ReplicaId> members;
        for (ReplicaId r = 0; r < early; ++r) members.push_back(r);
        for (ReplicaId r = 0; r < c_.replicas; ++r) {
            Host h;
            h.joined = r < early;
            h.replica = zombie::initial(r, members);
            h.durable = h.replica;
            w.hosts.push_back(std::move(h));
        }
        w.sinks.resize(1 + c_.redirects);
        for (ReplicaId r = 0; r < early; ++r) apply(w, r, Start{r == 0, {0}});
        return w;
    }

    void successors(const State& w, const std::function<void(State, Label, bool)>& emit) const {
        const auto n = c_.replicas;
        for (ReplicaId r = 0; r < n; ++r) {
            const Host& h = w.hosts[r];
            if (!h.joined) {
                // The host joins the mesh: an acting main clones itself there
                // and adds the new replica to its membership.
                for (ReplicaId m = 0; m < n; ++m) {
                    const auto& mh = w.hosts[m];
                    if (!mh.joined || !mh.up || mh.replica.role != Role::main || mh.replica.frozen) continue;
                    State x = w;
                    x.hosts[r].joined = true;
                    x.hosts[r].replica = zombie::initial(r, mh.replica.members);
                    x.hosts[r].durable = x.hosts[r].replica;
                    apply(x, r, Start{false, {}});
                    apply(x, m, AddMembers{{r}});
                    emit(std::move(x), {"join", int(r), int(m)}, false);
                }
                continue;
            }
            if (!h.up) {
                State x = w;
                x.hosts[r].up = true;
                x.hosts[r].replica = x.hosts[r].durable;
                apply(x, r, Recovered{});
                emit(std::move(x), {"restart", int(r)}, false);
                continue;
            }
            // A crash loses what was not durable. The runtime also stores
            // images on its own; the crash may come right after one, so
            // nothing is lost beyond the messages to the host.
            for (const bool image : {false, true}) {
                if (w.crashes >= c_.crashes || (image && h.durable == h.replica)) continue;
                State x = w;
                ++x.crashes;
                if (image) x.hosts[r].durable = x.hosts[r].replica;
                x.hosts[r].up = false;
                std::erase_if(x.net, [&](const auto& ch) { return receiver(ch.first) == std::int32_t(r); });
                settle(x);
                emit(std::move(x), {image ? "crash after an image" : "crash", int(r)}, true);
            }
            const State_& s = h.replica;
            if (s.role == Role::ended) continue;
            for (std::size_t t = 0; t < timer_count; ++t) {
                const auto timer = static_cast<Timer>(t);
                if (!s.armed(timer)) continue;
                const bool early = premature(w, r, timer);
                if (early && w.early_timeouts >= c_.early_timeouts) continue;
                State x = w;
                if (early) ++x.early_timeouts;
                apply(x, r, Fired{timer});
                emit(std::move(x), {early ? "early timer" : "timer", int(r), int(t)}, early);
            }
            if (s.role == Role::main && !s.frozen && s.progress < c_.work) {
                State x = w;
                const auto p = s.progress + 1;
                apply(x, r, Worked{p, p >= c_.work, {static_cast<std::uint8_t>(p)}});
                emit(std::move(x), {"work", int(r)}, false);
            }
            if (s.role == Role::main && !s.frozen && w.observations < c_.observations) {
                State x = w;
                ++x.observations;
                apply(x, r, Observed{{static_cast<std::uint8_t>(w.observations)}});
                emit(std::move(x), {"observe", int(r)}, false);
            }
            if ((h.tomb != 0) || h.sink > s.sink) {
                State x = w;
                apply(x, r, RegistryUpdate{h.tomb != 0, h.sink});
                emit(std::move(x), {"registry", int(r)}, false);
            }
            if (w.cancels < c_.cancels && h.tomb == 0) {
                State x = w;
                ++x.cancels;
                x.hosts[r].tomb = 2;
                emit(std::move(x), {"cancel", int(r)}, true);
            }
            if (w.redirects < c_.redirects) {
                State x = w;
                ++x.redirects;
                x.hosts[r].sink = 1 + x.redirects;
                emit(std::move(x), {"redirect", int(r)}, true);
            }
        }
        for (ReplicaId a = 0; a < n; ++a) {
            for (ReplicaId b = a + 1; b < n; ++b) {
                if (!w.hosts[a].joined || !w.hosts[b].joined) continue;
                if (!linked(w, a, b)) {
                    State x = w;
                    x.links &= ~bit(a, b);
                    emit(std::move(x), {"heal", int(a), int(b)}, false);
                    continue;
                }
                if (w.cuts < c_.cuts) {
                    State x = w;
                    ++x.cuts;
                    x.links |= bit(a, b);
                    emit(std::move(x), {"cut", int(a), int(b)}, true);
                }
                const auto& ha = w.hosts[a];
                const auto& hb = w.hosts[b];
                if (ha.up && hb.up && (ha.tomb != hb.tomb || ha.sink != hb.sink)) gossip(w, a, b, emit);
            }
        }
        for (std::size_t k = 0; k < w.sinks.size(); ++k) {
            State x = w;
            if (w.sinks[k].up) {
                if (w.sink_downs >= c_.sink_downs) continue;
                ++x.sink_downs;
                x.sinks[k].up = false;
                emit(std::move(x), {"sink down", int(k + 1)}, true);
            } else {
                x.sinks[k].up = true;
                emit(std::move(x), {"sink up", int(k + 1)}, false);
            }
        }
        for (const auto& [channel, queue] : w.net) {
            const Packet k = decode_packet(queue.front());
            if (deliverable(w, k)) {
                State x = w;
                pop(x, channel, 0);
                deliver(x, k);
                emit(std::move(x), {"deliver", -1, -1, describe(k)}, false);
                if (w.duplicates < c_.duplicates) {
                    State y = w;
                    ++y.duplicates;
                    deliver(y, k);  // stays in flight: delivered again later
                    emit(std::move(y), {"duplicate", -1, -1, describe(k)}, true);
                }
            }
            if (w.drops < c_.drops) {
                for (std::size_t i = 0; i < queue.size(); ++i) {
                    State x = w;
                    ++x.drops;
                    pop(x, channel, i);
                    emit(std::move(x), {"lose", -1, -1, describe(decode_packet(queue[i]))}, true);
                }
            }
        }
    }

    std::string encode(const State& world) const {
        State w = world;
        canonicalize(w);
        msgpack::Writer out;
        out.write_array_header(static_cast<std::uint32_t>(w.hosts.size()));
        for (const auto& h : w.hosts) {
            out.write_array_header(6);
            out.write_bool(h.joined);
            out.write_bool(h.up);
            paglets_encode(out, h.replica);
            paglets_encode(out, h.durable);
            out.write_uint(h.tomb);
            out.write_uint(h.sink);
        }
        out.write_array_header(static_cast<std::uint32_t>(w.sinks.size()));
        for (const auto& s : w.sinks) {
            out.write_bool(s.up);
            out.write_int(s.accepted);
        }
        out.write_uint(w.links);
        out.write_array_header(static_cast<std::uint32_t>(w.net.size()));
        for (const auto& [channel, queue] : w.net) {
            out.write_uint(channel);
            out.write_array_header(static_cast<std::uint32_t>(queue.size()));
            for (const auto& p : queue) out.write_str(p);
        }
        for (auto v : {w.crashes, w.cuts, w.drops, w.duplicates, w.sink_downs, w.cancels, w.redirects, w.observations,
                       w.early_timeouts})
            out.write_uint(v);
        paglets_encode(out, w.best);
        out.write_array_header(static_cast<std::uint32_t>(w.kept.size()));
        for (const auto& k : w.kept) paglets_encode(out, Prefix{k.observer, k.incarnation, k.n});
        out.write_bool(w.illegal);
        const auto& b = out.bytes();
        return std::string(b.begin(), b.end());
    }

    State decode(const std::string& bytes) const {
        State w;
        msgpack::Reader in(std::span(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size()));
        std::uint32_t n = 0;
        (void)in.read_array_header(n);
        w.hosts.resize(n);
        for (auto& h : w.hosts) {
            std::uint32_t k = 0;
            (void)in.read_array_header(k);
            (void)msgpack::read_value(in, h.joined);
            (void)msgpack::read_value(in, h.up);
            (void)paglets_decode(in, h.replica);
            (void)paglets_decode(in, h.durable);
            (void)msgpack::read_value(in, h.tomb);
            (void)msgpack::read_value(in, h.sink);
        }
        (void)in.read_array_header(n);
        w.sinks.resize(n);
        for (auto& s : w.sinks) {
            (void)msgpack::read_value(in, s.up);
            (void)msgpack::read_value(in, s.accepted);
        }
        (void)msgpack::read_value(in, w.links);
        (void)in.read_array_header(n);
        for (std::uint32_t i = 0; i < n; ++i) {
            std::uint32_t channel = 0, m = 0;
            (void)msgpack::read_value(in, channel);
            (void)in.read_array_header(m);
            auto& queue = w.net[channel];
            for (std::uint32_t j = 0; j < m; ++j) {
                std::string p;
                (void)in.read_str(p);
                queue.push_back(std::move(p));
            }
        }
        for (auto* v : {&w.crashes, &w.cuts, &w.drops, &w.duplicates, &w.sink_downs, &w.cancels, &w.redirects,
                        &w.observations, &w.early_timeouts})
            (void)msgpack::read_value(in, *v);
        (void)paglets_decode(in, w.best);
        (void)in.read_array_header(n);
        for (std::uint32_t i = 0; i < n; ++i) {
            Prefix p;
            (void)paglets_decode(in, p);
            w.kept.insert(EntryKey{p.observer, p.incarnation, p.n});
        }
        (void)msgpack::read_value(in, w.illegal);
        return w;
    }

    std::string describe(const Label& l) const {
        std::ostringstream os;
        os << l.what;
        if (l.a >= 0) os << ' ' << (std::string_view(l.what).starts_with("sink") ? "" : "host ") << l.a;
        if (std::string_view(l.what) == "join") return os.str() + " (replica made by the main on host " + std::to_string(l.b) + ")";
        if (std::string_view(l.what).ends_with("timer")) {
            os << ' ' << to_string(static_cast<Timer>(l.b));
        } else if (l.b >= 0) {
            os << "-" << l.b;
        }
        if (!l.detail.empty()) os << ' ' << l.detail;
        return os.str();
    }

    std::vector<verify::Property<State>> invariants() const {
        return {
            {"one_result", "at most one sink holds a result (one more for every redirect)",
             [](const State& w) {
                 std::uint32_t accepted = 0;
                 for (const auto& s : w.sinks) accepted += s.accepted >= 0;
                 return accepted <= 1 + w.redirects;
             }},
            {"best_progress_kept",
             "while the group lives, some replica durably holds the best checkpoint ever made durable",
             [](const State& w) {
                 if (tombed(w)) return true;
                 for (const auto& h : w.hosts) {
                     if (h.durable.role != Role::ended && h.durable.rank >= w.best) return true;
                 }
                 return false;
             }},
            {"observations_kept",
             "while the group lives, every observation that was ever durable is durable on some replica",
             [](const State& w) {
                 if (tombed(w)) return true;
                 for (const auto& k : w.kept) {
                     bool found = false;
                     for (const auto& h : w.hosts) {
                         if (h.durable.role != Role::ended && h.durable.journal.entries.contains(k)) found = true;
                     }
                     if (!found) return false;
                 }
                 return true;
             }},
            {"legal_transitions", "every role change is in the specification's table",
             [](const State& w) { return !w.illegal; }},
        };
    }

    std::vector<verify::Property<State>> goals() const {
        return {
            {"one_main",
             "a single main that every other replica follows, with its checkpoint and journal (or the group ends)",
             [](const State& w) {
                 for (const auto& h : w.hosts) {
                     if (h.joined && h.replica.role == Role::ended) return true;
                 }
                 std::int32_t main = -1;
                 for (const auto& h : w.hosts) {
                     if (!h.joined) continue;
                     if (!h.up) return false;
                     if (h.replica.role == Role::main || h.replica.role == Role::delivering) {
                         if (main >= 0) return false;
                         main = static_cast<std::int32_t>(h.replica.self);
                     }
                 }
                 if (main < 0) return false;
                 const auto& m = w.hosts[main].replica;
                 for (const auto& h : w.hosts) {
                     const auto& s = h.replica;
                     if (!h.joined || s.self == m.self) continue;
                     if (s.members != m.members) return false;
                     if (s.role != Role::sleeping || s.leader != m.self || !(s.rank == m.rank) ||
                         !(s.journal == m.journal))
                         return false;
                 }
                 return true;
             }},
            {"group_ends", "every replica ended, after a sink accepted the result (or the owner cancelled)",
             [](const State& w) {
                 bool result = false;
                 for (const auto& s : w.sinks) result |= s.accepted >= 0;
                 for (const auto& h : w.hosts) {
                     if (!h.joined) continue;
                     if (!h.up || h.replica.role != Role::ended) return false;
                     result |= h.tomb == 2;
                 }
                 return result;
             }},
        };
    }

    // Role changes seen while exploring (the specification's table should be
    // covered completely).
    std::set<std::tuple<Role, Cause, Role>> transitions_seen() const {
        std::set<std::tuple<Role, Cause, Role>> out;
        const auto bits = seen_.load();
        for (std::size_t i = 0; i < allowed_transitions.size(); ++i) {
            if ((bits >> i) & 1) {
                const auto& a = allowed_transitions[i];
                out.emplace(a.from, a.cause, a.to);
            }
        }
        return out;
    }

    static Packet decode_packet(const std::string& bytes) {
        Packet p;
        msgpack::Reader r(std::span(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size()));
        std::uint32_t n = 0;
        (void)r.read_array_header(n);
        (void)msgpack::read_value(r, p.kind);
        (void)msgpack::read_value(r, p.from);
        (void)msgpack::read_value(r, p.to);
        if (p.kind == 0) {
            (void)paglets_decode(r, p.message);
        } else {
            (void)msgpack::read_value(r, p.sink);
            (void)msgpack::read_value(r, p.accepted);
        }
        return p;
    }
    static std::string encode_packet(const Packet& p) {
        msgpack::Writer w;
        w.write_array_header(p.kind == 0 ? 4 : 5);
        w.write_uint(p.kind);
        w.write_uint(p.from);
        w.write_uint(p.to);
        if (p.kind == 0) {
            paglets_encode(w, p.message);
        } else {
            w.write_uint(p.sink);
            w.write_bool(p.accepted);
        }
        const auto& b = w.bytes();
        return std::string(b.begin(), b.end());
    }

private:
    using State_ = zombie::State;

    static std::uint32_t bit(ReplicaId a, ReplicaId b) {
        if (a > b) std::swap(a, b);
        return 1u << (a * 8 + b);
    }
    static bool linked(const State& w, ReplicaId a, ReplicaId b) { return (w.links & bit(a, b)) == 0; }

    // Channels: replica to replica, replica to sink, sink to replica.
    static std::uint32_t channel_of(const Packet& p) {
        switch (p.kind) {
            case 0: return p.from * 256 + p.to;
            case 1: return p.from * 256 + 200 + static_cast<std::uint32_t>(p.sink);
            default: return (200 + static_cast<std::uint32_t>(p.sink)) * 256 + p.to;
        }
    }
    // The replica a channel leads to, -1 for a sink.
    static std::int32_t receiver(std::uint32_t channel) {
        const auto to = channel % 256;
        return to >= 200 ? -1 : static_cast<std::int32_t>(to);
    }
    static void pop(State& w, std::uint32_t channel, std::size_t i) {
        auto& queue = w.net[channel];
        queue.erase(queue.begin() + static_cast<std::ptrdiff_t>(i));
        if (queue.empty()) w.net.erase(channel);
    }
    static bool tombed(const State& w) {
        for (const auto& h : w.hosts) {
            if (h.tomb != 0) return true;
        }
        return false;
    }

    // A suspect timeout is early while the main it follows is up, reachable
    // and leading; a round ends early while questions or answers of it are
    // still in flight; a beat (and the resend of a result) is early while
    // the last one is still in flight.
    static bool premature(const State& w, ReplicaId r, Timer t) {
        const auto& s = w.hosts[r].replica;
        if (t == Timer::suspect) {
            const auto l = s.leader;
            if (l == r) return false;
            const auto& m = w.hosts[l];
            return m.up && linked(w, r, l) &&
                   (m.replica.role == Role::main || m.replica.role == Role::delivering);
        }
        if (t == Timer::beat) {
            // A resend while the last beat or result is still on its way.
            for (const auto& [channel, queue] : w.net) {
                for (const auto& bytes : queue) {
                    const Packet p = decode_packet(bytes);
                    if ((p.kind == 0 && p.from == r && std::holds_alternative<Beat>(p.message)) ||
                        (p.kind == 1 && p.from == r) || (p.kind == 2 && p.to == r))
                        return true;
                }
            }
            return false;
        }
        if (t == Timer::round && s.role == Role::candidate) {
            for (const auto& [channel, queue] : w.net) {
              for (const auto& bytes : queue) {
                const Packet p = decode_packet(bytes);
                if (p.kind != 0) continue;
                const bool ask = p.from == r && std::holds_alternative<StatusReq>(p.message);
                const bool answer = p.to == r && std::holds_alternative<Status>(p.message);
                if ((ask || answer) && w.hosts[p.from].up && w.hosts[p.to].up && linked(w, p.from, p.to)) return true;
              }
            }
        }
        return false;
    }

    bool deliverable(const State& w, const Packet& p) const {
        switch (p.kind) {
            case 0: return w.hosts[p.to].joined && w.hosts[p.to].up && linked(w, p.from, p.to);
            case 1: return w.sinks[p.sink - 1].up;
            default: return w.hosts[p.to].up;
        }
    }

    void put(State& w, Packet p) const {
        auto& queue = w.net[channel_of(p)];
        std::string bytes = encode_packet(p);
        // A periodic resend that catches up with the same message, still
        // queued, adds nothing (double delivery is the duplicate fault).
        if (std::ranges::find(queue, bytes) != queue.end()) return;
        if (queue.size() >= c_.in_flight) return;  // lost
        queue.push_back(std::move(bytes));
    }

    void deliver(State& w, const Packet& p) const {
        switch (p.kind) {
            case 0: apply(w, p.to, Received{p.from, p.message}); return;
            case 1: {
                auto& sink = w.sinks[p.sink - 1];
                const bool accepted = sink.accepted < 0;
                if (accepted) sink.accepted = static_cast<std::int32_t>(p.from);
                if (w.hosts[p.from].up) put(w, Packet{2, 0, p.from, {}, p.sink, accepted});
                return;
            }
            default: apply(w, p.to, SinkAnswer{p.sink, p.accepted}); return;
        }
    }

    void gossip(State x, ReplicaId a, ReplicaId b, const std::function<void(State, Label, bool)>& emit) const {
        auto& ha = x.hosts[a];
        auto& hb = x.hosts[b];
        const auto tomb = std::max(ha.tomb, hb.tomb);
        const auto sink = std::max(ha.sink, hb.sink);
        ha.tomb = hb.tomb = tomb;
        ha.sink = hb.sink = sink;
        emit(std::move(x), {"gossip", int(a), int(b)}, false);
    }

    std::string describe(const Packet& p) const {
        std::ostringstream os;
        if (p.kind == 0) {
            os << message_name(p.message) << ' ' << p.from << "->" << p.to;
        } else if (p.kind == 1) {
            os << "result " << p.from << "->sink " << p.sink;
        } else {
            os << (p.accepted ? "accepted" : "already") << " sink " << p.sink << "->" << p.to;
        }
        return os.str();
    }

    // Applies an input to a replica and carries out the outputs.
    void apply(State& w, ReplicaId r, const Input& input) const {
        Host& h = w.hosts[r];
        const Outputs out = step(h.replica, input);
        for (const auto& o : out) {
            if (const auto* send = std::get_if<Send>(&o)) {
                if (send->to < w.hosts.size() && w.hosts[send->to].joined && w.hosts[send->to].up &&
                    linked(w, r, send->to))
                    put(w, Packet{0, r, send->to, send->message});
            } else if (std::holds_alternative<Persist>(o) || std::holds_alternative<End>(o)) {
                h.durable = h.replica;
            } else if (const auto* d = std::get_if<Deliver>(&o)) {
                if (d->sink <= w.sinks.size() && w.sinks[d->sink - 1].up) put(w, Packet{1, r, 0, {}, d->sink});
            } else if (std::holds_alternative<Tombstone>(o)) {
                if (h.tomb == 0) h.tomb = 1;
            } else if (const auto* t = std::get_if<Transition>(&o)) {
                bool listed = false;
                for (std::size_t i = 0; i < allowed_transitions.size(); ++i) {
                    const auto& a = allowed_transitions[i];
                    if (a.from == t->from && a.cause == t->cause && a.to == t->to) {
                        listed = true;
                        if (!((seen_.load(std::memory_order_relaxed) >> i) & 1)) seen_.fetch_or(1ull << i);
                    }
                }
                if (!listed) w.illegal = true;
            }
        }
        settle(w);
    }

    // Once no crash can come any more, what is durable no longer matters
    // for running replicas: it is their state.
    void settle(State& w) const {
        if (w.crashes >= c_.crashes) {
            for (auto& h : w.hosts) {
                if (h.up) h.durable = h.replica;
            }
        }
        note_durable(w);
    }

    static void note_durable(State& w) {
        for (const auto& h : w.hosts) {
            if (h.durable.role == Role::ended) continue;
            w.best = std::max(w.best, h.durable.rank);
            for (const auto& [k, d] : h.durable.journal.entries) w.kept.insert(k);
        }
    }

    // Terms are only compared, raised to the maximum and incremented; renaming
    // them keeps the behavior as long as order and adjacency are kept. Gaps
    // shrink to 2, so the state space stays finite however long the run.
    static void canonicalize(State& w) {
        std::set<std::uint64_t> terms{0};
        auto collect_rank = [&](const Rank& r) { terms.insert(r.term); };
        auto collect_state = [&](const State_& s) {
            terms.insert(s.term);
            terms.insert(s.reign);
            collect_rank(s.rank);
            for (const auto& [id, st] : s.replies) {
                terms.insert(st.term);
                collect_rank(st.rank);
            }
        };
        auto visit_message = [](Message& m, auto&& f) {
            std::visit(
                [&](auto& v) {
                    f(v.term);
                    if constexpr (requires { v.rank; }) f(v.rank.term);
                },
                m);
        };
        // Messages and answers for a replica that ended can have no effect
        // any more (it never comes back); dropping them merges states that
        // differ only in such leftovers.
        std::vector<Packet> packets;  // in channel order
        for (const auto& [channel, queue] : w.net) {
            for (const auto& p : queue) {
                Packet k = decode_packet(p);
                if (k.kind != 1 && w.hosts[k.to].replica.role == Role::ended) continue;
                packets.push_back(std::move(k));
            }
        }
        for (const auto& h : w.hosts) {
            collect_state(h.replica);
            collect_state(h.durable);
        }
        for (auto& p : packets) {
            if (p.kind == 0) visit_message(p.message, [&](std::uint64_t& t) { terms.insert(t); });
        }
        collect_rank(w.best);
        std::map<std::uint64_t, std::uint64_t> to;
        std::uint64_t prev = 0, next = 0;
        for (auto t : terms) {
            if (t != 0) next += std::min<std::uint64_t>(t - prev, 2);
            to[t] = next;
            prev = t;
        }
        auto fix_state = [&](State_& s) {
            s.term = to[s.term];
            s.reign = to[s.reign];
            s.rank.term = to[s.rank.term];
            for (auto& [id, st] : s.replies) {
                st.term = to[st.term];
                st.rank.term = to[st.rank.term];
            }
        };
        for (auto& h : w.hosts) {
            fix_state(h.replica);
            fix_state(h.durable);
        }
        w.best.term = to[w.best.term];
        w.net.clear();
        for (auto& p : packets) {
            if (p.kind == 0) visit_message(p.message, [&](std::uint64_t& t) { t = to[t]; });
            w.net[channel_of(p)].push_back(encode_packet(p));
        }
    }

    ModelConfig c_;
    mutable std::atomic<std::uint64_t> seen_{0};
};

}  // namespace paglets::protocols::zombie
