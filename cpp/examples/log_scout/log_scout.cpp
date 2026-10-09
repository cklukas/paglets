// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Log Scout (planning/cpp-demo-paglets.md, demo 7): one view of problems
// across the mesh without a log collection stack. The paglet sends a scout
// (a clone) to every host. Each scout asks its host for the log root (the
// mesh policy decides through `grants`), reads only the end of each log
// file (range reads), keeps the lines with a pattern within the time
// window, and reports counts and excerpts.
//
// In live mode the scouts stay on their hosts. Between checks they are
// inactive; when the host wakes them they read only what was appended since
// the last check (the read positions are ordinary members, so they survive
// deactivation in the memory image) and send what they found home. The
// reply tells them whether to go on: after `stop`, or when home is gone,
// they dispose themselves.

#include "log_scout.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>
#include <paglets/patterns/files.hpp>
#include <paglets/patterns/operations.hpp>

#include <algorithm>
#include <cctype>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace log_scout_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

constexpr std::size_t max_line = 300;
constexpr std::size_t max_live_excerpts = 100;

std::int64_t now_ms() {
    return paglets::now_ns(paglets::abi::Clock::wall) / 1'000'000;
}

std::string lower(std::string_view s) {
    std::string out(s);
    std::ranges::transform(out, out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

// Days since 1970-01-01 of a civil date (H. Hinnant's algorithm).
std::int64_t days_from_civil(std::int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const auto yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

// The time of a line that starts with "YYYY-MM-DD[T ]HH:MM:SS" (UTC), or -1.
std::int64_t line_time(std::string_view l) {
    if (l.size() < 19 || l[4] != '-' || l[7] != '-' || (l[10] != 'T' && l[10] != ' ') || l[13] != ':' || l[16] != ':')
        return -1;
    auto num = [&](std::size_t at, std::size_t n) -> std::int64_t {
        std::int64_t v = 0;
        for (std::size_t i = at; i < at + n; ++i) {
            if (!std::isdigit(static_cast<unsigned char>(l[i]))) return -1;
            v = v * 10 + (l[i] - '0');
        }
        return v;
    };
    const auto y = num(0, 4), mo = num(5, 2), d = num(8, 2), h = num(11, 2), mi = num(14, 2), s = num(17, 2);
    if (y < 0 || mo < 1 || mo > 12 || d < 1 || d > 31 || h < 0 || h > 23 || mi < 0 || mi > 59 || s < 0 || s > 60) return -1;
    return ((days_from_civil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d)) * 24 + h) * 60 + mi) * 60'000 + s * 1000;
}

class LogScout : public paglets::Paglet {
public:
    LogScout() {
        router().on<Scout>("start", [this](const Scout& s, Message& m) { start(s, m); });
        router().on("report", [this](Message& m) { (void)m.reply(report_); });
        router().on("stop", [this](Message& m) {
            if (report_.state == "live") report_.state = "stopped";
            (void)m.reply(report_);
        });
        // Ends this paglet (live scouts end at their next check).
        router().on("dispose", [this](Message& m) {
            if (report_.state == "live") report_.state = "stopped";
            (void)m.reply(report_);
            (void)paglets::dispose();
        });
        pt::serve<Findings, LiveReply>(router(), "scout.live", [this](const Findings& f) -> paglets::Result<LiveReply> {
            if (report_.state != "live") return LiveReply{false};
            ++report_.live_batches;
            for (const auto& c : f.counts) {
                add_count(f.host_name, c.pattern, c.matches);
                report_.live_matches += c.matches;
            }
            report_.files += f.files;
            report_.bytes_read += f.bytes_read;
            for (auto e : f.excerpts) {
                e.host_name = f.host_name;
                report_.excerpts.push_back(std::move(e));
            }
            if (report_.excerpts.size() > max_live_excerpts) {
                report_.excerpts.erase(report_.excerpts.begin(),
                                       report_.excerpts.end() - static_cast<std::ptrdiff_t>(max_live_excerpts));
            }
            return LiveReply{true};
        });
        router().on("scout.check", [this](Message&) {
            if (live_) check();
        });
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            Findings f;
            const std::string name = c.host_name.empty() ? (c.destination.empty() ? "here" : c.destination) : c.host_name;
            report_.hosts.push_back(name);
            if (!c.ok || !paglets::decode(c.result, f)) {
                report_.errors.push_back(name + ": " + (c.error.empty() ? "no report" : c.error));
            } else {
                for (const auto& x : f.counts) add_count(name, x.pattern, x.matches);
                for (auto e : f.excerpts) {
                    e.host_name = name;
                    report_.excerpts.push_back(std::move(e));
                }
                report_.files += f.files;
                report_.bytes_read += f.bytes_read;
            }
            if (fan_.finished() && report_.state == "searching") report_.state = scout_.live ? "live" : "done";
        });
    }

    void on_cloned(paglets::Started& s) override {
        if (s.caps.empty() || !s.decode_args(scout_)) {
            (void)paglets::dispose();
            return;
        }
        parent_ = paglets::Endpoint(s.caps[0].release());
        scouting_ = true;
        scan(true);
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }

    // A live scout: the host wakes it on time. A message can wake it
    // earlier; then it checks on time with a timer instead.
    void on_activated() override {
        if (!live_) return;
        const std::int64_t left = next_check_ - now_ms();
        if (left > std::max<std::int64_t>(5, scout_.interval_ms / 2)) {
            (void)paglets::after_raw(left, "scout.check", {});
            return;
        }
        check();
    }

private:
    // -- the paglet at home ---------------------------------------------------------------

    void start(const Scout& s, Message& m) {
        if (s.root.empty() || s.patterns.empty()) {
            (void)m.reply(Started{false, "a root and patterns are needed", 0});
            return;
        }
        if (report_.state == "searching" || report_.state == "live") {
            (void)m.reply(Started{false, "already " + report_.state, 0});
            return;
        }
        scout_ = s;
        report_ = Report{"searching"};
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, reply](std::vector<std::string> hosts) {
            const auto sent = fan_.start(hosts, paglets::encode(scout_), scout_.timeout_ms);
            (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no scout went out",
                                                    static_cast<std::int64_t>(hosts.size())});
        };
        if (!s.hosts.empty()) return go(s.hosts);
        // Every host mesh-info knows; this one by "" (no move).
        auto mi = paglets::service("mesh-info");
        if (!mi) return go({""});
        svc::mesh_info::Client client{*mi};
        auto sent = client.landscape({}, [go](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
            std::vector<std::string> hosts{""};
            if (l) {
                for (std::size_t i = 1; i < l->hosts.size(); ++i) hosts.push_back(l->hosts[i].host);
            }
            go(std::move(hosts));
        });
        if (!sent) go({""});
    }

    void add_count(const std::string& host, const std::string& pattern, std::int64_t n) {
        for (auto& c : report_.counts) {
            if (c.host_name == host && c.pattern == pattern) {
                c.matches += n;
                return;
            }
        }
        report_.counts.push_back(Count{host, pattern, n});
    }

    // -- a scout --------------------------------------------------------------------------

    // Scans the log files: the first time the end of each file within the
    // window, later only what was appended.
    void scan(bool first) {
        found_ = Findings{};
        for (const auto& p : scout_.patterns) found_.counts.push_back(Count{{}, p, 0});
        pt::granted_dir(scout_.root, "", {"read"}, "log scout", [this, first](paglets::Result<paglets::Capability> d) {
            if (!d) return scanned_error(pt::error_text("files of " + scout_.root, d.error()));
            dir_ = std::make_shared<paglets::Capability>(*d);
            pt::endpoint("files", [this, first](paglets::Result<paglets::Endpoint> files) {
                if (!files) return scanned_error(pt::error_text("files", files.error()));
                files_ = *files;
                svc::files::Client client{files_};
                svc::files::FindRequest q;
                q.pattern = scout_.files;
                q.limit = 10'000;
                auto sent = client.find(
                    q,
                    [this, first](paglets::Result<svc::files::FindReply> r, Message&) {
                        if (!r) return scanned_error(pt::error_text("find", r.error()));
                        todo_.clear();
                        for (const auto& e : r->entries) {
                            if (e.type == svc::files::EntryType::file) todo_.push_back(e);
                        }
                        read_next(0, first);
                    },
                    pt::lending(*dir_));
                if (!sent) scanned_error(pt::error_text("find", sent.error()));
            });
        });
    }

    void read_next(std::size_t i, bool first) {
        if (i >= todo_.size()) return scanned();
        const auto& e = todo_[i];
        auto at = positions_.find(e.path);
        std::uint64_t from = 0;
        if (first && scout_.window_ms > 0 && e.modified_ms < now_ms() - scout_.window_ms) {
            // Not written within the window: nothing to read now, and a live
            // scout reads only what is appended from here on.
            positions_[e.path] = e.size;
            return read_next(i + 1, first);
        }
        if (at != positions_.end() && at->second <= e.size) {
            from = at->second;  // appended since the last check
        } else if (first && scout_.tail_bytes > 0 && e.size > static_cast<std::uint64_t>(scout_.tail_bytes)) {
            from = e.size - static_cast<std::uint64_t>(scout_.tail_bytes);  // only the end
        }
        if (from >= e.size) {
            positions_[e.path] = e.size;
            return read_next(i + 1, first);
        }
        ++found_.files;
        chunk_.clear();
        read_range(i, first, from, from);
    }

    // Reads `path` from `offset` to its end (in chunks), then matches the
    // complete lines; a line still being written is read next time.
    void read_range(std::size_t i, bool first, std::uint64_t start, std::uint64_t offset) {
        constexpr std::uint64_t chunk = 256 * 1024;
        svc::files::Client client{files_};
        auto sent = client.read(
            svc::files::ReadRequest{todo_[i].path, offset, chunk},
            [this, i, first, start, offset](paglets::Result<svc::files::ReadReply> r, Message&) {
                if (!r) return read_next(i + 1, first);  // gone or unreadable: the next file
                chunk_.append(r->data.begin(), r->data.end());
                found_.bytes_read += static_cast<std::int64_t>(r->data.size());
                if (!r->eof && !r->data.empty()) return read_range(i, first, start, offset + r->data.size());
                const auto last_newline = chunk_.rfind('\n');
                const std::size_t complete = last_newline == std::string::npos ? 0 : last_newline + 1;
                // Started in the middle of a file: its first line is partial.
                std::size_t begin = 0;
                if (start > 0 && !positions_.contains(todo_[i].path)) {
                    const auto nl = chunk_.find('\n');
                    begin = nl == std::string::npos ? complete : std::min(complete, nl + 1);
                }
                match(todo_[i], std::string_view(chunk_).substr(begin, complete - begin), first);
                positions_[todo_[i].path] = start + complete;
                read_next(i + 1, first);
            },
            pt::lending(*dir_));
        if (!sent) read_next(i + 1, first);
    }

    void match(const svc::files::Entry& e, std::string_view text, bool first) {
        const std::int64_t since = first && scout_.window_ms > 0 ? now_ms() - scout_.window_ms : -1;
        std::vector<std::string> wanted;
        for (const auto& p : scout_.patterns) wanted.push_back(scout_.ignore_case ? lower(p) : p);
        std::size_t pos = 0;
        while (pos < text.size()) {
            auto nl = text.find('\n', pos);
            if (nl == std::string_view::npos) nl = text.size();
            std::string_view line = text.substr(pos, nl - pos);
            pos = nl + 1;
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
            if (since >= 0) {
                const auto t = line_time(line);
                if (t >= 0 ? t < since : e.modified_ms < since) continue;
            }
            const std::string hay = scout_.ignore_case ? lower(line) : std::string(line);
            for (std::size_t k = 0; k < wanted.size(); ++k) {
                if (wanted[k].empty() || hay.find(wanted[k]) == std::string::npos) continue;
                ++found_.counts[k].matches;
                const auto shown = std::ranges::count_if(found_.excerpts, [&](const Excerpt& x) {
                    return lower(x.line).find(lower(scout_.patterns[k])) != std::string::npos;
                });
                if (shown < scout_.max_excerpts) {
                    found_.excerpts.push_back(Excerpt{{}, e.path, std::string(line.substr(0, max_line))});
                }
            }
        }
    }

    void drop_dir() {
        if (dir_) (void)dir_->drop();
        dir_.reset();
    }

    void scanned() {
        drop_dir();
        if (scouting_) {
            scouting_ = false;
            if (!scout_.live) return pt::report(parent_, paglets::encode(found_), {}, [] { (void)paglets::dispose(); });
            pt::report(parent_, paglets::encode(found_));
            live_ = true;
            return sleep();
        }
        // A live check: send the new lines home; the reply says whether to go on.
        found_.host_name = host_name_;
        if (host_name_.empty()) {
            pt::here([this](paglets::Result<svc::mesh_info::Snapshot> s) {
                if (s) host_name_ = s->host_name;
                found_.host_name = host_name_;
                send_home();
            });
            return;
        }
        send_home();
    }

    void send_home() {
        auto sent = pt::call<LiveReply>(parent_, "scout.live", found_, [this](paglets::Result<LiveReply> r) {
            if (!r || !r->go_on) {
                live_ = false;
                return (void)paglets::dispose();
            }
            sleep();
        });
        if (!sent) {
            live_ = false;
            (void)paglets::dispose();
        }
    }

    void scanned_error(const std::string& why) {
        drop_dir();
        if (scouting_) {
            scouting_ = false;
            return pt::report_error(parent_, why, [] { (void)paglets::dispose(); });
        }
        paglets::log("log scout: " + why);
        sleep();  // a live check failed; the next may work
    }

    void check() { scan(false); }

    // Inactive until the next check.
    void sleep() {
        const std::int64_t interval = std::max<std::int64_t>(10, scout_.interval_ms);
        next_check_ = now_ms() + interval;
        if (!paglets::deactivate(interval)) (void)paglets::after_raw(interval, "scout.check", {});
    }

    // At home.
    pt::FanOut fan_;
    Report report_{"idle"};
    // Everywhere: what to look for.
    Scout scout_;
    // A scout.
    paglets::Endpoint parent_;
    bool scouting_ = false;
    bool live_ = false;
    std::int64_t next_check_ = 0;
    std::string host_name_;
    std::map<std::string, std::uint64_t> positions_;  // path -> read up to here
    std::vector<svc::files::Entry> todo_;
    std::string chunk_;
    Findings found_;
    std::shared_ptr<paglets::Capability> dir_;
    paglets::Endpoint files_;
};

}  // namespace

PAGLETS_PAGLET(LogScout)
