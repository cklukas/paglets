// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Log Explainer (planning/cpp-demo-paglets.md, demo 19): turns many log
// lines from many machines into a short list of distinct problems. The
// paglet starts a Log Scout (demo 7) as its child and takes the lines it
// found, groups similar ones (numbers, IDs and quoted text do not count),
// moves to a host that offers `ai`, classifies each group and has it
// explained in plain language, and comes home to tell its owner.

#include "explainer.schema.gen.hpp"

#include <paglets/patterns/notify.hpp>
#include <paglets/patterns/operations.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/services/ai.gen.hpp>

#include <algorithm>
#include <cctype>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace explainer_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
namespace pt = paglets::patterns;
using paglets::Message;

bool hex_digit(char c) {
    return std::isxdigit(static_cast<unsigned char>(c)) != 0;
}

// The shape of a log line: lower case, without a leading timestamp; numbers,
// long hex IDs and quoted text become #.
std::string shape_of(std::string_view line) {
    if (line.size() >= 19 && line[4] == '-' && line[7] == '-' && (line[10] == 'T' || line[10] == ' ')) {
        std::size_t i = 19;
        while (i < line.size() && line[i] != ' ') ++i;  // fractions, zone
        line.remove_prefix(i);
    }
    std::string out;
    std::size_t i = 0;
    while (i < line.size()) {
        const char c = line[i];
        if (c == '"' || c == '\'') {
            const auto end = line.find(c, i + 1);
            if (end != std::string_view::npos) {
                out += '#';
                i = end + 1;
                continue;
            }
        }
        // A word of hex digits with a digit in it (IDs, addresses), or a number.
        std::size_t j = i;
        bool has_digit = false;
        while (j < line.size() && (hex_digit(line[j]) || line[j] == '.' || line[j] == ':')) {
            has_digit = has_digit || std::isdigit(static_cast<unsigned char>(line[j]));
            ++j;
        }
        const bool word_start = i == 0 || !std::isalnum(static_cast<unsigned char>(line[i - 1]));
        const bool word_end = j == line.size() || !std::isalnum(static_cast<unsigned char>(line[j]));
        if (j > i && has_digit && word_start && word_end) {
            out += '#';
            i = j;
            continue;
        }
        out += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        ++i;
    }
    // Trim and collapse spaces.
    std::string tidy;
    for (char c : out) {
        if (c == ' ' && (tidy.empty() || tidy.back() == ' ')) continue;
        tidy += c;
    }
    while (!tidy.empty() && tidy.back() == ' ') tidy.pop_back();
    return tidy;
}

class Explainer : public paglets::Paglet {
public:
    Explainer() {
        router().on<Explain>("start", [this](const Explain& e, Message& m) { start(e, m); });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
        router().on("poll", [this](Message&) { poll(); });
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        where([this] {
            if (status_.state == "explaining") return explain(0);
            if (status_.state == "returning") return done();
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        fail("move failed: " + f.reason);
    }

private:
    void where(std::function<void()> next) {
        pt::here([this, next](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) {
                here_ = s->host;
                status_.hosts.push_back(s->host_name.empty() ? s->host.substr(0, 8) : s->host_name);
                if (status_.state == "explaining") status_.ai_host = s->host_name;
            }
            next();
        });
    }

    void fail(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
        paglets::log("explainer: " + status_.error);
    }

    // -- at home: the Log Scout -------------------------------------------------------------

    void start(const Explain& e, Message& m) {
        if (status_.state != "idle") return (void)m.reply(Started{false, "already " + status_.state});
        if (e.root.empty() || e.scout_module.empty()) return (void)m.reply(Started{false, "a root and the scout module are needed"});
        params_ = e;
        auto child = paglets::create_child_raw({}, {}, e.scout_module);
        if (!child) return (void)m.reply(Started{false, pt::error_text("log scout", child.error())});
        scout_ = std::make_shared<paglets::Endpoint>(std::move(*child));
        status_.state = "scouting";
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        ScoutStart q{e.hosts, e.root, e.files, e.patterns, e.window_ms, 50};
        auto sent = pt::call<ScoutStarted>(*scout_, "start", q, [this, reply](paglets::Result<ScoutStarted> r) {
            if (!r || !r->accepted) {
                fail(!r ? pt::error_text("log scout", r.error()) : "log scout: " + r->reason);
                return (void)paglets::reply_to(*reply, Started{false, status_.error});
            }
            (void)paglets::reply_to(*reply, Started{true, {}});
            where([this] {
                home_ = here_;
                poll();
            });
        });
        if (!sent) {
            fail(pt::error_text("log scout", sent.error()));
            (void)paglets::reply_to(*reply, Started{false, status_.error});
        }
    }

    // Asks the scout for its report until it is done.
    void poll() {
        if (status_.state != "scouting") return;
        auto sent = pt::call<ScoutReport>(*scout_, "report", ScoutStart{}, [this](paglets::Result<ScoutReport> r) {
            if (!r) return fail(pt::error_text("log scout", r.error()));
            if (r->state != "done") {
                (void)paglets::after_raw(100, "poll", {});
                return;
            }
            group(*r);
            // The scout's work is done.
            (void)pt::call<ScoutReport>(*scout_, "dispose", ScoutStart{}, [](paglets::Result<ScoutReport>) {});
            scout_.reset();
            if (status_.groups.empty()) return done();
            status_.state = "explaining";
            if (auto moved = paglets::dispatch(params_.via); !moved) fail(pt::error_text("dispatch", moved.error()));
        });
        if (!sent) fail(pt::error_text("log scout", sent.error()));
    }

    void group(const ScoutReport& r) {
        std::map<std::string, Group> by_shape;
        for (const auto& e : r.excerpts) {
            ++status_.lines;
            auto& g = by_shape[shape_of(e.line)];
            if (g.count++ == 0) {
                g.shape = shape_of(e.line);
                g.example = e.line;
            }
            if (std::ranges::find(g.hosts, e.host_name) == g.hosts.end()) g.hosts.push_back(e.host_name);
        }
        for (auto& [shape, g] : by_shape) {
            std::ranges::sort(g.hosts);
            status_.groups.push_back(std::move(g));
        }
        std::ranges::stable_sort(status_.groups, [](const Group& a, const Group& b) { return a.count > b.count; });
        if (static_cast<std::int64_t>(status_.groups.size()) > params_.max_groups) {
            status_.groups.resize(static_cast<std::size_t>(std::max<std::int64_t>(0, params_.max_groups)));
        }
    }

    // -- on the AI host -----------------------------------------------------------------------

    void explain(std::size_t i) {
        if (i >= status_.groups.size()) {
            status_.state = "returning";
            if (here_ == home_) return done();
            if (auto moved = paglets::dispatch(home_); !moved) fail(pt::error_text("dispatch home", moved.error()));
            return;
        }
        pt::lookup("ai", [this, i](paglets::Result<paglets::Endpoint> ai) {
            if (!ai) return fail(pt::error_text("ai", ai.error()));
            auto client = std::make_shared<svc::ai::Client>(*ai);
            const auto& g = status_.groups[i];
            auto sent = client->classify(
                svc::ai::ClassifyRequest{g.example, params_.labels, {}},
                [this, i, client](paglets::Result<svc::ai::ClassifyReply> c, Message&) {
                    if (!c) return fail(pt::error_text("classify", c.error()));
                    status_.groups[i].label = c->label;
                    const auto& g = status_.groups[i];
                    svc::ai::GenerateRequest q;
                    q.system = "You explain log messages to system administrators.";
                    q.prompt = "This log line appeared " + std::to_string(g.count) + " times on " +
                               std::to_string(g.hosts.size()) + " host(s): " + g.example +
                               "\nExplain its likely cause in one or two sentences.";
                    auto asked = client->generate(q, [this, i](paglets::Result<svc::ai::TextReply> t, Message&) {
                        if (!t) return fail(pt::error_text("generate", t.error()));
                        status_.groups[i].explanation = t->text;
                        status_.model = t->model;
                        explain(i + 1);
                    });
                    if (!asked) fail(pt::error_text("generate", asked.error()));
                });
            if (!sent) fail(pt::error_text("classify", sent.error()));
        });
    }

    // -- home again ---------------------------------------------------------------------------

    void done() {
        status_.state = "explained";
        std::string text;
        for (const auto& g : status_.groups) {
            text += "[" + g.label + "] " + std::to_string(g.count) + "x " + g.example + "\n" + g.explanation + "\n\n";
        }
        pt::notify(status_.groups.empty() ? pt::Level::success : pt::Level::warning,
                   std::to_string(status_.groups.size()) + " distinct problem(s) in " + std::to_string(status_.lines) +
                       " log line(s)",
                   text.substr(0, 3900));
    }

    Explain params_;
    Status status_{"idle"};
    std::string home_;
    std::string here_;
    std::shared_ptr<paglets::Endpoint> scout_;
};

}  // namespace

PAGLETS_PAGLET(Explainer)
