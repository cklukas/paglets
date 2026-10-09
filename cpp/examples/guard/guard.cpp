// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Volume Guard (planning/cpp-demo-paglets.md, demo 9): watches the volumes
// of its host. Between checks it is inactive (it takes no memory or CPU on
// the host's lanes and survives restarts as a stored image); the host wakes
// it on time. A volume low on space is reported to the owner through
// `user-info`, once until it recovers.

#include "guard.schema.gen.hpp"

#include <paglets/patterns/notify.hpp>
#include <paglets/services/server_info.gen.hpp>

#include <algorithm>
#include <set>
#include <string>

namespace {

using namespace guard_msgs;
namespace si = paglets::services::server_info;
namespace pt = paglets::patterns;
using paglets::Message;

class Guard : public paglets::Paglet {
public:
    Guard() {
        router().on<Watch>("start", [this](const Watch& w, Message& m) {
            watch_ = w;
            status_ = Status{"watching", 0, 0, {}, {}};
            low_.clear();
            (void)m.reply(status_);
            check();
        });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
        router().on("check", [this](Message&) {
            if (status_.state == "watching") check();
        });
        router().on("stop", [this](Message& m) {
            status_.state = "done";
            (void)m.reply(status_);
        });
    }

    // The host wakes it when it is time, but a message (a reply to its
    // notification, `status`) wakes it too: then it checks on time with a
    // timer instead.
    void on_activated() override {
        if (status_.state != "watching") return;
        // Timers and clocks differ by a little: a wake within half an
        // interval of the time is the host's.
        const std::int64_t left = next_check_ - now_ms();
        if (left > std::max<std::int64_t>(5, watch_.interval_ms / 2)) {
            (void)paglets::after_raw(left, "check", {});
            return;
        }
        check();
    }

private:
    static std::int64_t now_ms() { return paglets::now_ns(paglets::abi::Clock::wall) / 1'000'000; }

    void check() {
        pt::lookup("server-info", [this](paglets::Result<paglets::Endpoint> ep) {
            if (!ep) return failed(pt::error_text("server-info", ep.error()));
            si::Client c{*ep};
            auto sent = c.volumes({}, [this](paglets::Result<si::Volumes> v, Message&) {
                if (!v) return failed(pt::error_text("volumes", v.error()));
                ++status_.checks;
                for (const auto& vol : v->volumes) {
                    if (vol.total == 0) continue;
                    const double free = 100.0 * static_cast<double>(vol.available) / static_cast<double>(vol.total);
                    if (free >= watch_.min_free_percent) {
                        low_.erase(vol.mount_point);  // recovered
                        continue;
                    }
                    if (!low_.insert(vol.mount_point).second) continue;  // reported already
                    status_.alerts.push_back(Alert{vol.mount_point, free, now_ms()});
                    pt::notify(pt::Level::warning, "volume " + vol.mount_point + " is low on space",
                               std::to_string(static_cast<int>(free)) + "% free");
                }
                sleep();
            });
            if (!sent) failed(pt::error_text("volumes", sent.error()));
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
        if (paglets::deactivate(interval)) ++status_.sleeps;
    }

    void failed(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
    }

    Watch watch_;
    Status status_{"idle", 0, 0, {}, {}};
    std::set<std::string> low_;
    std::int64_t next_check_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Guard)
