// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The zombie protocol: definitions (included by zombie.hpp; header-only, so
// that guests and the host compile the same code).

#pragma once

namespace paglets::protocols::zombie {

inline std::string_view to_string(Role r) {
    switch (r) {
        case Role::recovering: return "recovering";
        case Role::sleeping: return "sleeping";
        case Role::candidate: return "candidate";
        case Role::main: return "main";
        case Role::delivering: return "delivering";
        case Role::ended: return "ended";
    }
    return "?";
}

inline std::string_view to_string(Timer t) {
    switch (t) {
        case Timer::beat: return "beat";
        case Timer::checkpoint: return "checkpoint";
        case Timer::suspect: return "suspect";
        case Timer::round: return "round";
    }
    return "?";
}

inline std::string_view to_string(Cause c) {
    switch (c) {
        case Cause::created: return "created";
        case Cause::recovered: return "recovered";
        case Cause::suspect: return "suspect";
        case Cause::beat: return "beat";
        case Cause::adopted: return "adopted";
        case Cause::worse_main: return "worse_main";
        case Cause::better_main: return "better_main";
        case Cause::round_won: return "round_won";
        case Cause::round_lost: return "round_lost";
        case Cause::takeover: return "takeover";
        case Cause::done: return "done";
        case Cause::delivered: return "delivered";
        case Cause::tomb: return "tomb";
    }
    return "?";
}

// -- journal ------------------------------------------------------------------

inline VersionVector Journal::version() const {
    VersionVector vv;
    for (const auto& [key, data] : entries) {
        if (vv.empty() || vv.back().observer != key.observer || vv.back().incarnation != key.incarnation) {
            vv.push_back({key.observer, key.incarnation, 0});
        }
        auto& p = vv.back();
        if (key.n == p.n + 1) p.n = key.n;  // only the part without a gap counts
    }
    std::erase_if(vv, [](const Prefix& p) { return p.n == 0; });
    return vv;
}

namespace detail {
inline std::uint64_t covered(const VersionVector& vv, const EntryKey& key) {
    auto it = std::lower_bound(vv.begin(), vv.end(), Prefix{key.observer, key.incarnation, 0},
                               [](const Prefix& a, const Prefix& b) {
                                   return std::pair(a.observer, a.incarnation) < std::pair(b.observer, b.incarnation);
                               });
    if (it == vv.end() || it->observer != key.observer || it->incarnation != key.incarnation) return 0;
    return it->n;
}

// True if `theirs` covers an entry that `mine` does not.
inline bool lacks(const VersionVector& mine, const VersionVector& theirs) {
    return std::ranges::any_of(theirs, [&](const Prefix& p) {
        return covered(mine, EntryKey{p.observer, p.incarnation, p.n}) < p.n;
    });
}
}  // namespace detail

inline std::vector<Entry> Journal::beyond(const VersionVector& vv) const {
    std::vector<Entry> out;
    for (const auto& [key, data] : entries) {
        if (key.n > detail::covered(vv, key)) out.push_back({key, data});
    }
    return out;
}

inline bool Journal::has_beyond(const VersionVector& vv) const {
    return std::ranges::any_of(entries, [&](const auto& e) { return e.first.n > detail::covered(vv, e.first); });
}

inline bool Journal::merge(const std::vector<Entry>& more) {
    bool changed = false;
    for (const auto& e : more) changed |= entries.emplace(e.key, e.data).second;
    return changed;
}

// -- messages -----------------------------------------------------------------

inline std::uint64_t term_of(const Message& m) {
    return std::visit([](const auto& v) { return v.term; }, m);
}

inline std::string_view message_name(const Message& m) {
    static constexpr std::array<std::string_view, 7> names = {"beat",   "ack",    "checkpoint", "entries",
                                                              "status?", "status", "takeover"};
    return names[m.index()];
}

// -- the transition function ----------------------------------------------------

inline ReplicaId replica_id(std::string_view key) {
    if (key.size() < 16) return 0;
    ReplicaId id = 0;
    for (std::size_t i = 0; i < 16; ++i) {
        const char c = key[i];
        int v = c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
        if (v < 0) return 0;
        id = id << 4 | static_cast<ReplicaId>(v);
    }
    return id;
}

inline State initial(ReplicaId self, std::vector<ReplicaId> members) {
    State s;
    s.self = self;
    members.push_back(self);
    std::ranges::sort(members);
    members.erase(std::unique(members.begin(), members.end()), members.end());
    s.members = std::move(members);
    s.leader = self;
    s.rank.author = self;
    return s;
}

namespace detail {

class Stepper {
public:
    Stepper(State& s, Outputs& out) : s_(s), out_(out) {}

    void run(const Input& input) {
        if (s_.role == Role::ended) return;  // an ended replica does nothing
        std::visit([this](const auto& in) { on(in); }, input);
    }

private:
    // -- helpers --

    void become(Role to, Cause cause) {
        const Role from = s_.role;
        s_.role = to;
        out_.push_back(Transition{from, to, cause});
    }
    void set(Timer t) {
        s_.timers |= static_cast<std::uint8_t>(1u << static_cast<unsigned>(t));
        out_.push_back(SetTimer{t});
    }
    void stop(Timer t) {
        if (!s_.armed(t)) return;
        s_.timers &= static_cast<std::uint8_t>(~(1u << static_cast<unsigned>(t)));
        out_.push_back(StopTimer{t});
    }
    void stop_all() {
        for (std::size_t t = 0; t < timer_count; ++t) stop(static_cast<Timer>(t));
    }
    void send(ReplicaId to, Message m) { out_.push_back(Send{to, std::move(m)}); }
    void broadcast(const Message& m) {
        for (ReplicaId r : s_.members) {
            if (r != s_.self) send(r, m);
        }
    }
    void persist() { out_.push_back(Persist{}); }
    void see(std::uint64_t term) { s_.term = std::max(s_.term, term); }

    Beat beat() const { return Beat{s_.rank.term, s_.rank, s_.journal.version(), s_.members}; }

    // The membership only grows.
    void add_members(const std::vector<ReplicaId>& more) {
        for (ReplicaId r : more) {
            auto it = std::ranges::lower_bound(s_.members, r);
            if (it == s_.members.end() || *it != r) s_.members.insert(it, r);
        }
    }

    // The main records its working state as a new checkpoint (durable before
    // anybody hears of it).
    void checkpoint() {
        if (!s_.dirty) return;
        s_.rank = Rank{s_.done, s_.progress, s_.reign, s_.rank.seq + 1, s_.self};
        s_.work = s_.work_now;
        s_.dirty = false;
        persist();
    }

    // --8<-- [start:become_main]
    // Takes over with a new reign above every term seen. The checkpoint gets
    // the new reign (same content), and is durable before the first beat.
    void become_main(Cause cause, std::uint64_t seen) {
        see(seen);
        see(s_.rank.term);
        s_.term += 1;
        s_.reign = s_.term;
        s_.rank = Rank{s_.rank.done, s_.rank.progress, s_.reign, 0, s_.self};
        s_.progress = s_.rank.progress;
        s_.done = s_.rank.done;
        s_.work_now = s_.work;
        s_.dirty = false;
        s_.frozen = false;
        s_.leader = s_.self;
        s_.replies.clear();
        stop(Timer::suspect);
        stop(Timer::round);
// --8<-- [start:paper_persist_order]
        become(s_.done ? Role::delivering : Role::main, cause);
        persist();
        broadcast(beat());
        set(Timer::beat);
// --8<-- [end:paper_persist_order]
        if (s_.done) {
            out_.push_back(Deliver{s_.sink});
        } else {
            set(Timer::checkpoint);
            out_.push_back(StartWork{s_.work_now, s_.progress});
        }
    }
    // --8<-- [end:become_main]

    void become_candidate(Cause cause) {
        stop(Timer::suspect);
        s_.replies.clear();
        become(Role::candidate, cause);
        broadcast(StatusReq{s_.term});
        set(Timer::round);
    }

    void follow(ReplicaId main, Role from_role, Cause cause) {
        s_.leader = main;
        if (from_role != Role::sleeping) {
            stop(Timer::round);
            s_.replies.clear();
            become(Role::sleeping, cause);
        }
        set(Timer::suspect);  // restarts the timeout
    }

    void end(Cause cause) {
        if (s_.role == Role::main) out_.push_back(StopWork{});
        stop_all();
        become(Role::ended, cause);
        out_.push_back(End{});
    }

    bool passive() const {
        return s_.role == Role::sleeping || s_.role == Role::recovering || s_.role == Role::candidate;
    }
    bool leading() const { return s_.role == Role::main || s_.role == Role::delivering; }

    // -- inputs --

    void on(const Start& in) {
        if (in.main) {
            s_.work = in.work;
            become_main(Cause::created, 0);
        } else {
            set(Timer::suspect);
            persist();
        }
    }

    // --8<-- [start:recovered]
    // After a restart the replica comes back as `recovering`, whatever it
    // was, with a new incarnation (durable before it observes anything), and
    // drops the working state beyond its checkpoint.
    void on(const Recovered&) {
        const Role from = s_.role;
        s_.incarnation += 1;
        s_.next_entry = 1;
        s_.timers = 0;
        s_.frozen = false;
        s_.replies.clear();
        s_.progress = s_.rank.progress;
        s_.done = s_.rank.done;
        s_.work_now = s_.work;
        s_.dirty = false;
        if (from == Role::main) out_.push_back(StopWork{});
        s_.role = Role::recovering;
        out_.push_back(Transition{from, Role::recovering, Cause::recovered});
        persist();
        set(Timer::suspect);
    }
    // --8<-- [end:recovered]

    void on(const Fired& in) {
        if (!s_.armed(in.timer)) return;  // stopped meanwhile
        s_.timers &= static_cast<std::uint8_t>(~(1u << static_cast<unsigned>(in.timer)));
        switch (in.timer) {
            case Timer::beat:
                if (!leading()) return;
                broadcast(beat());
                if (s_.role == Role::delivering) out_.push_back(Deliver{s_.sink});
                set(Timer::beat);
                return;
            case Timer::checkpoint:
                if (s_.role != Role::main) return;
                if (s_.dirty) {
                    checkpoint();
                    broadcast(beat());
                }
                set(Timer::checkpoint);
                return;
            case Timer::suspect:
                if (s_.role == Role::sleeping || s_.role == Role::recovering) become_candidate(Cause::suspect);
                return;
            case Timer::round:
                if (s_.role == Role::candidate) {
                    decide();
                } else if (s_.role == Role::main && s_.frozen) {
                    s_.frozen = false;
                    out_.push_back(StartWork{s_.work_now, s_.progress});
                }
                return;
        }
    }

    // --8<-- [start:decide]
    // The end of an election round: the best of the candidate and every
    // replica that answered. Equal ranks mean the same checkpoint; then a
    // replica that already leads is preferred, then the lower number.
    void decide() {
        ReplicaId best = s_.self;
        Rank best_rank = s_.rank;
        bool best_leads = false;
        std::uint64_t seen = s_.term;
        for (const auto& [r, st] : s_.replies) {
            seen = std::max(seen, st.term);
            const bool leads = st.role == Role::main || st.role == Role::delivering;
            if (st.rank > best_rank || (st.rank == best_rank && leads && !best_leads) ||
                (st.rank == best_rank && leads == best_leads && r < best)) {
                best = r;
                best_rank = st.rank;
                best_leads = leads;
            }
        }
        s_.replies.clear();
        if (best == s_.self) {
            become_main(Cause::round_won, seen);
            return;
        }
        see(seen);
        if (!best_leads) send(best, Takeover{s_.term});
        follow(best, Role::candidate, Cause::round_lost);
    }
    // --8<-- [end:decide]

    void on(const Received& in) {
        see(term_of(in.message));
        add_members({in.from});
        if (const auto* b = std::get_if<Beat>(&in.message)) add_members(b->members);
        std::visit([&](const auto& m) { receive(in.from, m); }, in.message);
    }

    // Entries are merged whatever the role: journals only grow.
    void merge(const std::vector<Entry>& entries) {
        if (s_.journal.merge(entries) && s_.role == Role::main) s_.dirty = true;
    }

    // --8<-- [start:meet]
    // Two mains met. The one behind steps down and follows; the one ahead
    // tells the other and pauses its work for a round, so that its rank
    // stays what the other compares with.
    void meet(ReplicaId from, const Rank& theirs, const VersionVector& vv) {
        if (from == s_.self) return;
        checkpoint();
        if (theirs > s_.rank) {
            out_.push_back(StopWork{});
            stop(Timer::beat);
            stop(Timer::checkpoint);
            stop(Timer::round);
            s_.frozen = false;
            s_.leader = from;
            become(Role::sleeping, Cause::better_main);
            set(Timer::suspect);
            send(from, Ack{s_.rank.term, s_.rank, s_.journal.version()});
            if (s_.journal.has_beyond(vv)) send(from, Entries{0, s_.journal.beyond(vv)});
            return;
        }
        send(from, beat());
        if (!s_.frozen) {
            s_.frozen = true;
            out_.push_back(StopWork{});
        }
        set(Timer::round);
    }
    // --8<-- [end:meet]

    // --8<-- [start:beat]
    void receive(ReplicaId from, const Beat& m) {
        if (from == s_.self) return;
        if (s_.role == Role::main) {
            meet(from, m.rank, m.vv);
            return;
        }
        if (!passive()) return;  // delivering: the sink decides
        if (m.rank < s_.rank) {
            if (s_.role != Role::candidate) become_candidate(Cause::worse_main);
            return;
        }
        const Role from_role = s_.role;
        follow(from, from_role, Cause::beat);
        // Ask for what is missing; hand over what the main lacks.
        const VersionVector mine = s_.journal.version();
        if (m.rank > s_.rank || detail::lacks(mine, m.vv)) send(from, Ack{s_.rank.term, s_.rank, mine});
        if (s_.journal.has_beyond(m.vv)) send(from, Entries{0, s_.journal.beyond(m.vv)});
    }
    // --8<-- [end:beat]

    void receive(ReplicaId from, const Ack& m) {
        if (!leading()) return;
        const bool behind = m.rank < s_.rank;
        const bool lacks = s_.journal.has_beyond(m.vv);
        if (!behind && !lacks) return;
        CheckpointMsg c{s_.rank.term, s_.rank, std::nullopt, s_.journal.beyond(m.vv)};
        if (behind) c.work = s_.work;
        send(from, std::move(c));
    }

    // --8<-- [start:adopt]
    // A checkpoint at least as far as this replica's replaces its work; its
    // entries are merged in any case. Durable before anything else happens.
    void receive(ReplicaId from, const CheckpointMsg& m) {
        const bool merged = s_.journal.merge(m.entries);
        if (s_.role == Role::main) {
            if (merged) s_.dirty = true;
            meet(from, m.rank, s_.journal.version());
            return;
        }
        if (!passive() || m.rank < s_.rank) {
            if (merged) persist();
            return;
        }
        const bool adopt = m.rank > s_.rank && m.work;
        if (adopt) {
            s_.rank = m.rank;
            s_.work = *m.work;
        }
        if (adopt || merged) persist();
        if (s_.role != Role::sleeping) {
            follow(from, s_.role, Cause::adopted);
        } else {
            s_.leader = from;
        }
    }
    // --8<-- [end:adopt]

    void receive(ReplicaId, const Entries& m) { merge(m.entries); }

    // --8<-- [start:status]
    // A main asked for its status checkpoints first and pauses its work for
    // one round, so the rank it reports stays true while the round is open.
    void receive(ReplicaId from, const StatusReq&) {
        if (s_.role == Role::main) {
            checkpoint();
            if (!s_.frozen) {
                s_.frozen = true;
                out_.push_back(StopWork{});
            }
            set(Timer::round);
        }
        send(from, Status{s_.term, s_.role, s_.rank});
    }
    // --8<-- [end:status]

    void receive(ReplicaId from, const Status& m) {
        if (s_.role == Role::candidate) s_.replies[from] = m;
    }

    void receive(ReplicaId, const Takeover& m) {
        if (passive()) become_main(Cause::takeover, m.term);
    }

    void on(const Worked& in) {
        if (s_.role != Role::main || s_.frozen) return;
        if (in.progress > s_.progress || in.work != s_.work_now || in.done) s_.dirty = true;
        s_.progress = std::max(s_.progress, in.progress);
        s_.work_now = in.work;
        if (!in.done) return;
        s_.done = true;
        s_.dirty = true;
        checkpoint();
        out_.push_back(StopWork{});
        stop(Timer::checkpoint);
        become(Role::delivering, Cause::done);
        broadcast(beat());
        out_.push_back(Deliver{s_.sink});
    }

    void on(const Observed& in) {
        if (s_.role != Role::main || s_.frozen) return;
        s_.journal.entries.emplace(EntryKey{s_.self, s_.incarnation, s_.next_entry++}, in.data);
        s_.dirty = true;
    }

    // --8<-- [start:sink]
    // Whatever the sink says, the group is done: it holds a result.
    void on(const SinkAnswer&) {
        out_.push_back(Tombstone{});
        end(Cause::delivered);
    }

    // A main that made replicas on new hosts tells everybody with its beats.
    void on(const AddMembers& in) { add_members(in.members); }

    void on(const RegistryUpdate& in) {
        s_.sink = std::max(s_.sink, in.sink);
        if (in.tomb) end(Cause::tomb);
    }
    // --8<-- [end:sink]

    State& s_;
    Outputs& out_;
};

}  // namespace detail

inline Outputs step(State& s, const Input& input) {
    Outputs out;
    detail::Stepper(s, out).run(input);
    return out;
}

// -- encoding -----------------------------------------------------------------

namespace detail {
template <class E>
void write_enum(msgpack::Writer& w, E e) {
    w.write_uint(static_cast<std::uint64_t>(e));
}
template <class E>
bool read_enum(msgpack::Reader& r, E& e, std::uint64_t limit) {
    std::uint64_t v = 0;
    if (!msgpack::read_value(r, v) || v >= limit) return false;
    e = static_cast<E>(v);
    return true;
}
inline bool header(msgpack::Reader& r, std::uint32_t expected) {
    std::uint32_t n = 0;
    return r.read_array_header(n) && n == expected;
}
}  // namespace detail

inline void paglets_encode(msgpack::Writer& w, const Rank& v) {
    w.write_array_header(5);
    w.write_bool(v.done);
    w.write_int(v.progress);
    w.write_uint(v.term);
    w.write_uint(v.seq);
    w.write_uint(v.author);
}
inline bool paglets_decode(msgpack::Reader& r, Rank& v) {
    return detail::header(r, 5) && msgpack::read_value(r, v.done) && msgpack::read_value(r, v.progress) &&
           msgpack::read_value(r, v.term) && msgpack::read_value(r, v.seq) && msgpack::read_value(r, v.author);
}

inline void paglets_encode(msgpack::Writer& w, const Entry& v) {
    w.write_array_header(4);
    w.write_uint(v.key.observer);
    w.write_uint(v.key.incarnation);
    w.write_uint(v.key.n);
    w.write_bin(v.data);
}
inline bool paglets_decode(msgpack::Reader& r, Entry& v) {
    return detail::header(r, 4) && msgpack::read_value(r, v.key.observer) &&
           msgpack::read_value(r, v.key.incarnation) && msgpack::read_value(r, v.key.n) && r.read_bin(v.data);
}

inline void paglets_encode(msgpack::Writer& w, const Prefix& v) {
    w.write_array_header(3);
    w.write_uint(v.observer);
    w.write_uint(v.incarnation);
    w.write_uint(v.n);
}
inline bool paglets_decode(msgpack::Reader& r, Prefix& v) {
    return detail::header(r, 3) && msgpack::read_value(r, v.observer) && msgpack::read_value(r, v.incarnation) &&
           msgpack::read_value(r, v.n);
}

inline void paglets_encode(msgpack::Writer& w, const Message& v) {
    std::visit(
        [&](const auto& m) {
            using M = std::decay_t<decltype(m)>;
            if constexpr (std::is_same_v<M, Beat>) {
                w.write_array_header(5);
                w.write_uint(v.index());
                w.write_uint(m.term);
                msgpack::write_value(w, m.rank);
                msgpack::write_value(w, m.vv);
                msgpack::write_value(w, m.members);
            } else if constexpr (std::is_same_v<M, Ack>) {
                w.write_array_header(4);
                w.write_uint(v.index());
                w.write_uint(m.term);
                msgpack::write_value(w, m.rank);
                msgpack::write_value(w, m.vv);
            } else if constexpr (std::is_same_v<M, CheckpointMsg>) {
                w.write_array_header(5);
                w.write_uint(v.index());
                w.write_uint(m.term);
                msgpack::write_value(w, m.rank);
                msgpack::write_value(w, m.work);
                msgpack::write_value(w, m.entries);
            } else if constexpr (std::is_same_v<M, Entries>) {
                w.write_array_header(3);
                w.write_uint(v.index());
                w.write_uint(m.term);
                msgpack::write_value(w, m.entries);
            } else if constexpr (std::is_same_v<M, Status>) {
                w.write_array_header(4);
                w.write_uint(v.index());
                w.write_uint(m.term);
                detail::write_enum(w, m.role);
                msgpack::write_value(w, m.rank);
            } else {  // StatusReq, Takeover
                w.write_array_header(2);
                w.write_uint(v.index());
                w.write_uint(m.term);
            }
        },
        v);
}

inline bool paglets_decode(msgpack::Reader& r, Message& v) {
    std::uint32_t n = 0;
    std::uint64_t index = 0;
    if (!r.read_array_header(n) || n < 2 || !msgpack::read_value(r, index)) return false;
    std::uint64_t term = 0;
    switch (index) {
        case 0:
        case 1: {
            Rank rank;
            VersionVector vv;
            std::vector<ReplicaId> members;
            if (n != (index == 0 ? 5u : 4u) || !msgpack::read_value(r, term) || !msgpack::read_value(r, rank) ||
                !msgpack::read_value(r, vv) || (index == 0 && !msgpack::read_value(r, members)))
                return false;
            if (index == 0) {
                v = Beat{term, rank, std::move(vv), std::move(members)};
            } else {
                v = Ack{term, rank, std::move(vv)};
            }
            return true;
        }
        case 2: {
            CheckpointMsg c;
            if (n != 5 || !msgpack::read_value(r, c.term) || !msgpack::read_value(r, c.rank) ||
                !msgpack::read_value(r, c.work) || !msgpack::read_value(r, c.entries))
                return false;
            v = std::move(c);
            return true;
        }
        case 3: {
            Entries e;
            if (n != 3 || !msgpack::read_value(r, e.term) || !msgpack::read_value(r, e.entries)) return false;
            v = std::move(e);
            return true;
        }
        case 4:
            if (n != 2 || !msgpack::read_value(r, term)) return false;
            v = StatusReq{term};
            return true;
        case 5: {
            Status st;
            if (n != 4 || !msgpack::read_value(r, st.term) || !detail::read_enum(r, st.role, 6) ||
                !msgpack::read_value(r, st.rank))
                return false;
            v = st;
            return true;
        }
        case 6:
            if (n != 2 || !msgpack::read_value(r, term)) return false;
            v = Takeover{term};
            return true;
        default: return false;
    }
}

inline void paglets_encode(msgpack::Writer& w, const State& v) {
    w.write_array_header(21);
    w.write_uint(v.self);
    msgpack::write_value(w, v.members);
    detail::write_enum(w, v.role);
    w.write_uint(v.incarnation);
    w.write_uint(v.term);
    w.write_uint(v.reign);
    w.write_uint(v.leader);
    msgpack::write_value(w, v.rank);
    w.write_bin(v.work);
    w.write_int(v.progress);
    w.write_bool(v.done);
    w.write_bin(v.work_now);
    w.write_bool(v.dirty);
    w.write_bool(v.frozen);
    w.write_array_header(static_cast<std::uint32_t>(v.journal.entries.size()));
    for (const auto& [key, data] : v.journal.entries) msgpack::write_value(w, Entry{key, data});
    w.write_uint(v.next_entry);
    w.write_array_header(static_cast<std::uint32_t>(v.replies.size()));
    for (const auto& [r, st] : v.replies) {
        w.write_array_header(2);
        w.write_uint(r);
        msgpack::write_value(w, Message{st});
    }
    w.write_uint(v.sink);
    w.write_uint(v.timers);
    w.write_uint(0);  // reserved
    w.write_uint(0);  // reserved
}

inline bool paglets_decode(msgpack::Reader& r, State& v) {
    if (!detail::header(r, 21)) return false;
    std::vector<Entry> entries;
    std::uint32_t n = 0;
    std::uint64_t reserved = 0;
    if (!(msgpack::read_value(r, v.self) && msgpack::read_value(r, v.members) && detail::read_enum(r, v.role, 6) &&
          msgpack::read_value(r, v.incarnation) && msgpack::read_value(r, v.term) && msgpack::read_value(r, v.reign) &&
          msgpack::read_value(r, v.leader) && msgpack::read_value(r, v.rank) && r.read_bin(v.work) &&
          msgpack::read_value(r, v.progress) && msgpack::read_value(r, v.done) && r.read_bin(v.work_now) &&
          msgpack::read_value(r, v.dirty) && msgpack::read_value(r, v.frozen) && msgpack::read_value(r, entries) &&
          msgpack::read_value(r, v.next_entry) && r.read_array_header(n)))
        return false;
    v.journal.entries.clear();
    for (auto& e : entries) v.journal.entries.emplace(e.key, std::move(e.data));
    v.replies.clear();
    if (n > r.remaining()) return false;
    for (std::uint32_t i = 0; i < n; ++i) {
        ReplicaId id = 0;
        Message m;
        if (!detail::header(r, 2) || !msgpack::read_value(r, id) || !paglets_decode(r, m) ||
            !std::holds_alternative<Status>(m))
            return false;
        v.replies[id] = std::get<Status>(m);
    }
    return msgpack::read_value(r, v.sink) && msgpack::read_value(r, v.timers) && msgpack::read_value(r, reserved) &&
           msgpack::read_value(r, reserved);
}

}  // namespace paglets::protocols::zombie
