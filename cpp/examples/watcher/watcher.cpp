// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Release Watcher (planning/cpp-demo-paglets.md, demo 22): keeps tool
// versions on machines without internet access up to date. The paglet
// moves to a host that offers `web.fetch` and stays there. Every interval
// it fetches the release pages or feeds, finds the version on each, and
// tells its owner about new ones (`user-info`). Between checks it is
// inactive; the versions it saw are ordinary members of its memory. On
// request (`fetch`) it starts a Download Courier for the release file,
// which brings the file to the host the watcher started on.

#include "watcher.schema.gen.hpp"

#include <paglets/patterns/notify.hpp>
#include <paglets/patterns/operations.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/services/web.gen.hpp>

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace watcher_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
namespace pt = paglets::patterns;
using paglets::Message;

std::int64_t now_ms() {
    return paglets::now_ns(abi::Clock::wall) / 1'000'000;
}

bool digit(char c) {
    return std::isdigit(static_cast<unsigned char>(c)) != 0;
}

// The first version on a page (in a feed: from its first entry on), such as
// 1.4.2 or v2.0: digits, a dot, digits, more dotted parts; with its v.
std::string version_of(std::string_view page) {
    if (auto entry = page.find("<entry"); entry != std::string_view::npos) page = page.substr(entry);
    for (std::size_t i = 0; i < page.size(); ++i) {
        if (!digit(page[i]) || (i > 0 && (digit(page[i - 1]) || page[i - 1] == '.'))) continue;
        std::size_t j = i;
        int parts = 0;
        while (j < page.size() && digit(page[j])) {
            while (j < page.size() && digit(page[j])) ++j;
            ++parts;
            if (j + 1 < page.size() && page[j] == '.' && digit(page[j + 1])) {
                ++j;
            } else {
                break;
            }
        }
        if (parts < 2) {
            i = j;
            continue;
        }
        const bool v = i > 0 && (page[i - 1] == 'v' || page[i - 1] == 'V');
        return std::string(page.substr(v ? i - 1 : i, j - i + (v ? 1 : 0)));
    }
    return {};
}

std::string replace_all(std::string s, std::string_view what, std::string_view with) {
    for (std::size_t at = s.find(what); at != std::string::npos; at = s.find(what, at + with.size())) {
        s.replace(at, what.size(), with);
    }
    return s;
}

class Watcher : public paglets::Paglet {
public:
    Watcher() {
        router().on<Watch>("start", [this](const Watch& w, Message& m) { start(w, m); });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
        router().on("stop", [this](Message& m) {
            if (status_.state == "watching") status_.state = "done";
            (void)m.reply(status_);
        });
        router().on("check", [this](Message&) {
            if (status_.state == "watching") check(0);
        });
        router().on<Fetch>("fetch", [this](const Fetch& f, Message& m) { fetch(f, m); });
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        if (status_.state != "travelling") return;
        pt::here([this](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) status_.host = s->host_name;
            status_.state = "watching";
            check(0);
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        status_.state = "failed";
        status_.error = "no web host: " + f.reason;
    }

    // The host wakes it on time. Messages wake it too (`status`, replies):
    // then it checks on time with a timer instead.
    void on_activated() override {
        if (status_.state != "watching") return;
        const std::int64_t left = next_check_ - now_ms();
        if (left > std::max<std::int64_t>(5, watch_.interval_ms / 2)) {
            (void)paglets::after_raw(left, "check", {});
            return;
        }
        check(0);
    }

private:
    void start(const Watch& w, Message& m) {
        if (status_.state != "idle") {
            (void)m.reply(Started{false, "already " + status_.state});
            return;
        }
        if (w.pages.empty()) {
            (void)m.reply(Started{false, "no pages to watch"});
            return;
        }
        watch_ = w;
        status_.seen.clear();
        for (const auto& p : w.pages) status_.seen.push_back(Seen{p.name, {}, 0, {}});
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        pt::here([this, reply](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (!s) return (void)paglets::reply_to(*reply, Started{false, pt::error_text("mesh-info", s.error())});
            home_ = s->host;
            status_.state = "travelling";
            if (auto moved = paglets::dispatch(watch_.via); !moved) {
                status_.state = "failed";
                status_.error = pt::error_text("dispatch", moved.error());
                return (void)paglets::reply_to(*reply, Started{false, status_.error});
            }
            (void)paglets::reply_to(*reply, Started{true, {}});
        });
    }

    // -- checks -----------------------------------------------------------------------------

    void check(std::size_t i) {
        if (i >= watch_.pages.size()) {
            ++status_.checks;
            return sleep();
        }
        pt::lookup("web", [this, i](paglets::Result<paglets::Endpoint> web) {
            if (!web) {
                status_.seen[i].error = pt::error_text("web", web.error());
                return check(i + 1);
            }
            svc::web::Client client{*web};
            svc::web::FetchRequest q;
            q.url = watch_.pages[i].url;
            auto sent = client.fetch(q, [this, i](paglets::Result<svc::web::FetchReply> r, Message&) {
                auto& seen = status_.seen[i];
                seen.checked_ms = now_ms();
                if (!r || r->status < 200 || r->status >= 300) {
                    seen.error = !r ? pt::error_text("fetch", r.error()) : "HTTP status " + std::to_string(r->status);
                    return check(i + 1);
                }
                const std::string version =
                    version_of(std::string_view(reinterpret_cast<const char*>(r->body.data()), r->body.size()));
                if (version.empty()) {
                    seen.error = "no version on the page";
                    return check(i + 1);
                }
                seen.error.clear();
                if (!seen.version.empty() && seen.version != version) {
                    status_.updates.push_back(Update{seen.name, seen.version, version, seen.checked_ms});
                    pt::notify(pt::Level::info, seen.name + " " + version + " is out",
                               "was " + seen.version + "; " + watch_.pages[i].url);
                }
                seen.version = version;
                check(i + 1);
            });
            if (!sent) {
                status_.seen[i].error = pt::error_text("fetch", sent.error());
                check(i + 1);
            }
        });
    }

    // Inactive until the next check.
    void sleep() {
        if (status_.state != "watching") return;
        if (watch_.checks > 0 && status_.checks >= watch_.checks) {
            status_.state = "done";
            return;
        }
        const std::int64_t interval = std::max<std::int64_t>(10, watch_.interval_ms);
        next_check_ = now_ms() + interval;
        if (paglets::deactivate(interval)) {
            ++status_.sleeps;
        } else {
            (void)paglets::after_raw(interval, "check", {});
        }
    }

    // -- a courier for the release file ------------------------------------------------------

    void fetch(const Fetch& f, Message& m) {
        const auto page = std::ranges::find_if(watch_.pages, [&](const Page& p) { return p.name == f.name; });
        if (page == watch_.pages.end()) return (void)m.reply(Fetching{false, "not watched: " + f.name, {}});
        const auto& seen = status_.seen[static_cast<std::size_t>(page - watch_.pages.begin())];
        if (seen.version.empty() || page->asset.empty()) {
            return (void)m.reply(Fetching{false, "no version or no release file known", {}});
        }
        if (watch_.courier_module.empty()) return (void)m.reply(Fetching{false, "no courier module", {}});
        const std::string bare = seen.version.front() == 'v' || seen.version.front() == 'V' ? seen.version.substr(1) : seen.version;
        const std::string url = replace_all(page->asset, "{version}", bare);
        auto child = paglets::create_child_raw({}, {}, watch_.courier_module);
        if (!child) return (void)m.reply(Fetching{false, pt::error_text("courier", child.error()), {}});
        auto courier = std::make_shared<paglets::Endpoint>(std::move(*child));
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        CourierStart q;
        q.url = url;
        q.deliver_to = home_;
        auto sent = pt::call<CourierStarted>(*courier, "start", q, [reply, courier, url](paglets::Result<CourierStarted> r) {
            (void)courier->drop();  // the courier goes its own way
            if (!r) return (void)paglets::reply_to(*reply, Fetching{false, pt::error_text("courier", r.error()), url});
            (void)paglets::reply_to(*reply, Fetching{r->accepted, r->reason, url});
        });
        if (!sent) (void)paglets::reply_to(*reply, Fetching{false, pt::error_text("courier", sent.error()), url});
    }

    Watch watch_;
    Status status_{"idle"};
    std::string home_;
    std::int64_t next_check_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Watcher)
