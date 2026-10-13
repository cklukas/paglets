// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh Chronicle (docs/examples/mesh-chronicle.md): a zombie paglet that
// writes the life log of its mesh. It has a replica on every host; the main
// samples mesh-info every few seconds and records what changed: hosts that
// disappear and come back, connections between hosts that are lost and
// restored (as each host reports them), and its own takeovers. Replicas
// that ran apart during a partition merge their journals when they meet, so
// what both sides saw ends up in one log. When its lifetime is over, the
// life log goes to the sink as Markdown, the owner is notified, and every
// replica ends.

#include "chronicle.schema.gen.hpp"

#include <paglets/patterns/notify.hpp>
#include <paglets/patterns/zombie.hpp>

#include <algorithm>
#include <map>
#include <numeric>
#include <set>
#include <string>
#include <vector>

using namespace paglets;
using namespace chronicle_msgs;
namespace zp = paglets::protocols::zombie;

namespace {

std::int64_t now_ms() {
    return paglets::now_ns() / 1'000'000;
}

std::string two(std::int64_t v) {
    return (v < 10 ? "0" : "") + std::to_string(v);
}

// "2026-10-13 14:05:09" (UTC).
std::string utc(std::int64_t ms) {
    std::int64_t secs = ms / 1000;
    std::int64_t days = secs / 86400;
    std::int64_t rest = secs % 86400;
    // Days to civil date (Howard Hinnant's algorithm).
    days += 719468;
    const std::int64_t era = (days >= 0 ? days : days - 146096) / 146097;
    const std::int64_t doe = days - era * 146097;
    const std::int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    std::int64_t y = yoe + era * 400;
    const std::int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const std::int64_t mp = (5 * doy + 2) / 153;
    const std::int64_t d = doy - (153 * mp + 2) / 5 + 1;
    const std::int64_t m = mp < 10 ? mp + 3 : mp - 9;
    if (m <= 2) ++y;
    return std::to_string(y) + "-" + two(m) + "-" + two(d) + " " + two(rest / 3600) + ":" + two(rest % 3600 / 60) +
           ":" + two(rest % 60);
}

// "1 h 02 min", "3 min 05 s", "12 s".
std::string span(std::int64_t ms) {
    const std::int64_t s = std::max<std::int64_t>(0, ms) / 1000;
    if (s >= 3600) return std::to_string(s / 3600) + " h " + two(s % 3600 / 60) + " min";
    if (s >= 60) return std::to_string(s / 60) + " min " + two(s % 60) + " s";
    return std::to_string(s) + " s";
}

}  // namespace

class Chronicle final : public patterns::Zombie<Work> {
public:
    Chronicle() {
        router().on<Start>("start", [this](const Start& s, Message& m) -> std::int32_t {
            if (!group().empty()) return abi::bad_state;
            if (!s.sink.starts_with("host:") || s.lifetime_ms <= 0 || s.sample_ms < 1000) return abi::invalid_argument;
            Work w;
            w.sink = s.sink;
            w.lifetime_ms = s.lifetime_ms;
            w.sample_ms = s.sample_ms;
            w.started_ms = now_ms();
            patterns::ZombieTiming t;
            t.step_ms = s.sample_ms;
            if (s.beat_ms > 0) t.beat_ms = s.beat_ms;
            if (s.suspect_ms > 0) {
                t.suspect_ms = s.suspect_ms;
                t.jitter_ms = s.suspect_ms / 2;
            }
            if (s.round_ms > 0) t.round_ms = s.round_ms;
            if (s.expand_ms > 0) t.expand_ms = s.expand_ms;
            t.checkpoint_ms = std::max<std::int64_t>(s.sample_ms, t.beat_ms);
            start_group(s.sink, std::move(w), t);
            (void)m.reply(chronicle_msgs::Started{group(), {}});
            return abi::handled;
        });
        router().on("status", [this](Message& m) {
            const auto& r = replica();
            Status st;
            st.group = group();
            st.role = std::string(zp::to_string(r.role));
            st.replica = host_of(r.self);
            st.replicas = static_cast<std::uint32_t>(r.members.size());
            st.main = name_of(host_of(r.leader));
            st.samples = r.rank.progress;
            st.started_ms = work().started_ms;
            st.lifetime_ms = work().lifetime_ms;
            st.events = static_cast<std::int64_t>(r.journal.entries.size());
            (void)m.reply(st);
        });
        router().on("finish", [this](Message& m) {
            FinishReply f;
            f.accepted = acting();
            if (f.accepted) finish_ = true;
            const auto& r = replica();
            f.main = name_of(host_of(r.leader));
            (void)m.reply(f);
        });
    }

// --8<-- [start:step]
    void step(Work& w, std::function<void(bool done)> finished) override {
        auto mi = service("mesh-info");
        if (!mi) return finished(false);
        services::mesh_info::Client client{*mi};
        services::mesh_info::LandscapeRequest q;
        q.max_age_ms = std::max<std::int64_t>(3 * w.sample_ms, 3000);
        auto sent = client.landscape(q, [this, &w, finished](Result<services::mesh_info::Landscape> r, Message&) {
            if (!r) return finished(false);
            sample(w, r->hosts);
            const bool done = finish_ || now_ms() - w.started_ms >= w.lifetime_ms;
            if (done) note(Event{now_ms(), "end", here_, {}, std::to_string(w.samples) + " samples"});
            finished(done);
        });
        if (!sent) finished(false);
    }
// --8<-- [end:step]

    std::int64_t progress(const Work& w) const override { return w.samples; }

    std::string result_name() const override { return "chronicle-" + group() + ".md"; }

    void on_role(Role, Role to, Cause cause) override {
        if (to == Role::main) {
            takeover_ = true;
            takeover_cause_ = std::string(zp::to_string(cause));
        }
        if (to == Role::delivering) {
            patterns::notify(services::user_info::Level::info, "Mesh Chronicle " + group().substr(0, 8) + " is done",
                             "Its life log goes to " + work().sink + ".");
        }
    }

    Bytes result(const Work& w, const std::vector<zp::Entry>& journal) override {
        const std::string text = life_log(w, journal);
        return Bytes(text.begin(), text.end());
    }

private:
    void note(Event e) { (void)observe_value(e); }

    std::vector<Event> journal_events() const {
        std::vector<Event> out;
        for (const auto& [k, d] : replica().journal.entries) {
            Event e;
            if (decode(d, e)) out.push_back(std::move(e));
        }
        std::ranges::stable_sort(out, [](const Event& a, const Event& b) { return a.time_ms < b.time_ms; });
        return out;
    }

    std::string name_of(const std::string& key) const {
        const auto& w = work();
        for (std::size_t i = 0; i < w.members.size() && i < w.names.size(); ++i) {
            if (w.members[i] == key) return w.names[i];
        }
        return key.substr(0, 8);
    }

    // Compares the mesh now with the latest sample and records the changes.
    void sample(Work& w, const std::vector<services::mesh_info::Snapshot>& hosts) {
        const std::int64_t now = now_ms();
        if (!hosts.empty()) here_ = hosts.front().host;
        // Who is in the group: the replicas this main knows, with names from
        // mesh-info. Hosts that joined the mesh got a replica meanwhile.
        const bool first = w.members.empty();
        for (const auto& m : members()) {
            if (std::ranges::find(w.members, m) != w.members.end()) continue;
            w.members.push_back(m);
            w.names.emplace_back();
            if (!first) note(Event{now, "joined", m, here_});
        }
        for (const auto& h : hosts) {
            auto it = std::ranges::find(w.members, h.host);
            if (it != w.members.end() && !h.host_name.empty()) w.names[it - w.members.begin()] = h.host_name;
        }
        if (first) {
            std::string list;
            for (std::size_t i = 0; i < w.members.size(); ++i)
                list += (list.empty() ? "" : ", ") + (w.names[i].empty() ? w.members[i].substr(0, 8) : w.names[i]);
            note(Event{now, "begin", here_, {}, list});
        }
        if (takeover_) {
            takeover_ = false;
            if (w.samples > 0) {
                note(Event{now, "takeover", here_, {}, takeover_cause_, now - w.last_sample_ms, replica().reign});
            }
        }
// --8<-- [start:view]
        // What the merged journal says now: every lineage's observations,
        // the latest per host and per link.
        std::map<std::string, bool> host_up;
        std::set<std::string> down;  // "observer>other"
        for (const auto& m : w.members) host_up[m] = true;
        for (const auto& e : journal_events()) {
            if (e.kind == "host_down" || e.kind == "host_up") host_up[e.host] = e.kind == "host_up";
            if (e.kind == "link_down") down.insert(e.host + ">" + e.other);
            if (e.kind == "link_up") down.erase(e.host + ">" + e.other);
        }
        std::set<std::string> fresh;
        for (const auto& h : hosts) fresh.insert(h.host);
        for (const auto& [h, was_up] : host_up) {
            if (fresh.contains(h) && !was_up) note(Event{now, "host_up", h, here_});
            if (!fresh.contains(h) && was_up) note(Event{now, "host_down", h, here_});
        }
        // Links as each fresh host reports them (hosts that are not fresh
        // keep what they reported last).
        for (const auto& h : hosts) {
            for (const auto& l : h.links) {
                const std::string key = h.host + ">" + l.host;
                if (!l.up && !down.contains(key)) note(Event{now, "link_down", h.host, l.host, l.failure});
                if (l.up && down.contains(key)) note(Event{now, "link_up", h.host, l.host});
            }
        }
// --8<-- [end:view]
        if (w.samples > 0) w.alive_ms += std::min(now - w.last_sample_ms, 2 * w.sample_ms);
        w.last_sample_ms = now;
        ++w.samples;
    }

    // -- the life log --------------------------------------------------------------------------

    std::string life_log(const Work& w, const std::vector<zp::Entry>& journal) const {
        std::vector<Event> events;
        for (const auto& e : journal) {
            Event ev;
            if (decode(e.data, ev)) events.push_back(std::move(ev));
        }
        std::ranges::stable_sort(events, [](const Event& a, const Event& b) { return a.time_ms < b.time_ms; });
        // The same change reported by two lineages is one change.
        std::vector<Event> merged;
        for (auto& e : events) {
            const bool repeat = std::ranges::any_of(merged, [&](const Event& m) {
                if (e.kind == "joined") return m.kind == "joined" && m.host == e.host;  // a host joins once
                return m.kind == e.kind && m.host == e.host && m.other == e.other && e.time_ms - m.time_ms < 2 * w.sample_ms &&
                       e.kind != "takeover";
            });
            if (!repeat) merged.push_back(std::move(e));
        }
        auto name = [&](const std::string& key) {
            for (std::size_t i = 0; i < w.members.size() && i < w.names.size(); ++i) {
                if (w.members[i] == key && !w.names[i].empty()) return w.names[i];
            }
            return key.substr(0, 8);
        };
        const std::int64_t end = w.last_sample_ms;
        std::int64_t takeovers = 0, longest_gap = 0, dead = 0;
        for (const auto& e : merged) {
            if (e.kind != "takeover") continue;
            ++takeovers;
            longest_gap = std::max(longest_gap, e.gap_ms);
            dead += std::max<std::int64_t>(0, e.gap_ms - w.sample_ms);
        }
        std::string md;
        md += "# Mesh life log\n\n";
        md += "Written by the Mesh Chronicle, zombie group `" + group() + "`.\n\n";
        md += "| | |\n|---|---|\n";
        md += "| Began | " + utc(w.started_ms) + " UTC |\n";
        md += "| Ended | " + utc(end) + " UTC |\n";
        md += "| Lifetime | " + span(end - w.started_ms) + " |\n";
        std::string hosts;
        for (std::size_t i = 0; i < w.members.size(); ++i) hosts += (i ? ", " : "") + name(w.members[i]);
        md += "| Replicas | " + std::to_string(w.members.size()) + ": " + hosts + " |\n";
        md += "| Samples of the mesh | " + std::to_string(w.samples) + ", every " + span(w.sample_ms) + " |\n";
        md += "| Takeovers | " + std::to_string(takeovers) + " |\n";
        md += "| Time without a working main | " + span(dead) + " (longest gap " + span(longest_gap) + ") |\n\n";

        md += "## Timeline\n\n| Time (UTC) | What happened |\n|---|---|\n";
        for (const auto& e : merged) {
            std::string what;
            if (e.kind == "begin") {
                what = "The chronicle began on " + name(e.host) + ", with replicas on " + e.text + ".";
            } else if (e.kind == "host_down") {
                what = "**" + name(e.host) + "** disappeared (no fresh report seen from " + name(e.other) + ").";
            } else if (e.kind == "host_up") {
                what = "**" + name(e.host) + "** is back (seen from " + name(e.other) + ").";
            } else if (e.kind == "link_down") {
                what = name(e.host) + " lost its connection to " + name(e.other) +
                       (e.text.empty() ? "." : " (" + e.text + ").");
            } else if (e.kind == "link_up") {
                what = name(e.host) + " reaches " + name(e.other) + " again.";
            } else if (e.kind == "joined") {
                what = "**" + name(e.host) + "** joined the mesh and got a replica (seen from " + name(e.other) + ").";
            } else if (e.kind == "takeover") {
                what = "**" + name(e.host) + " took over as main** (" + e.text + ", reign " + std::to_string(e.term) +
                       ") after " + span(e.gap_ms) + " without a sample.";
            } else if (e.kind == "end") {
                what = "The work is done on " + name(e.host) + ": " + e.text + ".";
            } else {
                what = e.kind;
            }
            md += "| " + utc(e.time_ms) + " | " + what + " |\n";
        }

        // Connections: a pair is down while either end reports it down.
        md += "\n## Connections\n\n";
        struct Interval {
            std::int64_t from = 0, to = 0;
            std::string why;
        };
        std::map<std::string, std::vector<Interval>> pairs;
        std::map<std::string, std::set<std::string>> down_by;  // pair -> reporting directions
        auto pair_key = [&](const std::string& a, const std::string& b) {
            return name(std::min(a, b)) + " – " + name(std::max(a, b));
        };
        for (const auto& e : merged) {
            if (e.kind != "link_down" && e.kind != "link_up") continue;
            const auto key = pair_key(e.host, e.other);
            auto& by = down_by[key];
            const bool was = !by.empty();
            if (e.kind == "link_down") {
                by.insert(e.host);
            } else {
                by.erase(e.host);
            }
            if (!was && !by.empty()) pairs[key].push_back(Interval{e.time_ms, -1, e.text});
            if (was && by.empty() && !pairs[key].empty()) pairs[key].back().to = e.time_ms;
        }
        if (pairs.empty()) {
            md += "No connection was lost.\n";
        } else {
            md += "| Connection | Lost | Restored | For | Reason |\n|---|---|---|---|---|\n";
            for (const auto& [key, list] : pairs) {
                for (const auto& i : list) {
                    md += "| " + key + " | " + utc(i.from) + " | " + (i.to < 0 ? "not restored" : utc(i.to)) + " | " +
                          span((i.to < 0 ? end : i.to) - i.from) + " | " + (i.why.empty() ? "-" : i.why) + " |\n";
                }
            }
        }

        // Separations: hosts that could not see each other, from what either
        // side reported (a host gone from a main's view, a connection lost).
        // A side "worked" when a main there recorded something meanwhile; a
        // crashed host stays silent, a partition has work on both sides.
        md += "\n## Separations and partitions\n\n";
        std::set<std::pair<std::string, std::string>> cut;  // ordered pairs
        auto cut_pair = [&](const std::string& a, const std::string& b, bool on) {
            const auto p = std::pair{std::min(a, b), std::max(a, b)};
            if (on) {
                cut.insert(p);
            } else {
                cut.erase(p);
            }
        };
        auto components = [&]() {
            std::vector<std::string> hs = w.members;
            std::vector<std::size_t> parent(hs.size());
            std::iota(parent.begin(), parent.end(), 0);
            std::function<std::size_t(std::size_t)> find = [&](std::size_t x) {
                return parent[x] == x ? x : parent[x] = find(parent[x]);
            };
            for (std::size_t a = 0; a < hs.size(); ++a) {
                for (std::size_t b = a + 1; b < hs.size(); ++b) {
                    if (!cut.contains({std::min(hs[a], hs[b]), std::max(hs[a], hs[b])})) parent[find(a)] = find(b);
                }
            }
            std::map<std::size_t, std::vector<std::string>> groups;
            for (std::size_t i = 0; i < hs.size(); ++i) groups[find(i)].push_back(hs[i]);
            std::vector<std::vector<std::string>> out;
            for (auto& [root, g] : groups) out.push_back(std::move(g));
            return out;
        };
        struct Open {
            std::int64_t since = 0;
            std::vector<std::vector<std::string>> sides;
            std::set<std::string> active;  // observers that recorded something
        };
        std::optional<Open> open;
        std::vector<std::string> lines;
        std::int64_t partitions = 0;
        auto close = [&](std::int64_t at) {
            if (!open) return;
            if (at == open->since) {  // changes of one moment, not a separation
                open.reset();
                return;
            }
            std::string text;
            std::size_t worked = 0;
            for (const auto& side : open->sides) {
                std::string names;
                bool active = false;
                for (const auto& h : side) {
                    names += (names.empty() ? "" : ", ") + name(h);
                    active |= open->active.contains(h);
                }
                worked += active;
                text += (text.empty() ? "" : " / ") + std::string("{") + names + "} " + (active ? "worked" : "silent");
            }
            const bool partition = worked >= 2;
            partitions += partition;
            lines.push_back("| " + utc(open->since) + " | " + (at < 0 ? "not healed" : utc(at)) + " | " +
                            span((at < 0 ? end : at) - open->since) + " | " + text + " | " +
                            (partition ? "partition: both sides had a main" : "a host was gone or cut off") + " |");
            open.reset();
        };
        for (const auto& e : merged) {
            if (e.kind == "host_down") cut_pair(e.host, e.other, true);
            if (e.kind == "host_up") cut_pair(e.host, e.other, false);
            if (e.kind == "link_down") cut_pair(e.host, e.other, true);
            if (e.kind == "link_up") cut_pair(e.host, e.other, false);
            const std::string observer = e.kind == "host_down" || e.kind == "host_up" ? e.other : e.host;
            const auto parts = components();
            if (parts.size() < 2) {
                close(e.time_ms);
                continue;
            }
            if (!open || open->sides != parts) {
                close(e.time_ms);
                open = Open{e.time_ms, parts, {}};
            }
            open->active.insert(observer);
        }
        close(-1);
        if (lines.empty()) {
            md += "The hosts could always see each other.\n";
        } else {
            md += "| From | To | For | Sides | What it was |\n|---|---|---|---|---|\n";
            for (const auto& l : lines) md += l + "\n";
            md += "\n" + std::to_string(partitions) +
                  " partition(s) with a main on both sides. What both sides observed is merged into this log: the "
                  "replicas that worked apart handed over their journals when they met again; the lineage that got "
                  "furthest went on, the others forgot their work.\n";
        }
        return md;
    }

    std::string here_;
    bool takeover_ = false;
    std::string takeover_cause_;
    bool finish_ = false;
};

PAGLETS_PAGLET(Chronicle)
