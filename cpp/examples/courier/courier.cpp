// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Download Courier demo (planning/cpp-web-ai.md, section 5): a paglet
// started on a host without internet access moves to a host that offers
// `web`, downloads a file there into an artifact and comes home with the
// artifact capability. At home, the home host fetches the content from the
// web host into its own store (planning/cpp-content-exchange.md), and the
// owner hears. The content never passes through the paglet's memory, so
// its size is limited by the web host's download limit, not by the
// paglet's. Paglets never get sockets: the web host fetches on their
// behalf, checks the destination and asks the mesh policy.

#include "courier.schema.gen.hpp"

#include <paglets/patterns/notify.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/services/artifacts.gen.hpp>
#include <paglets/services/web.gen.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace courier_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
using paglets::Capability;
using paglets::Endpoint;
using paglets::Message;
using paglets::patterns::endpoint;
using paglets::patterns::error_text;
using paglets::patterns::lending;
using paglets::patterns::lookup;

class Courier : public paglets::Paglet {
public:
    Courier() {
        router().on<Start>("start", [this](const Start& s, Message& m) { start(s, m); });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
        router().on("retry-home", [this](Message&) { go_home(); });
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        note_host([this] {
            if (status_.state == "travelling") return download();
            if (status_.state == "returning") return deliver();
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        if (status_.state == "travelling") {
            fail("no host to download from: " + f.reason);
        } else if (status_.state == "returning") {
            // Stays here with the content; tries again later.
            status_.error = "cannot return home yet: " + f.reason;
            (void)paglets::after_raw(2000, "retry-home", {});
        }
    }

private:
    void start(const Start& s, Message& m) {
        if (status_.state != "" && status_.state != "idle") {
            (void)m.reply(Started{false, "already on an errand"});
            return;
        }
        if (s.url.empty()) {
            (void)m.reply(Started{false, "no URL"});
            return;
        }
        params_ = s;
        status_ = Status{};
        status_.state = "travelling";
        status_.url = s.url;
        // Home is this host, as mesh-info names it; then off to the web host.
        auto mi = paglets::service("mesh-info");
        if (!mi) {
            (void)m.reply(Started{false, "no mesh-info"});
            return;
        }
        auto reply = std::make_shared<Capability>(m.defer_reply());
        svc::mesh_info::Client client{*mi};
        auto sent = client.snapshot({}, [this, reply](paglets::Result<svc::mesh_info::Snapshot> snap, Message&) {
            if (!snap) {
                status_.state = "failed";
                return (void)paglets::reply_to(*reply, Started{false, error_text("mesh-info", snap.error())});
            }
            here_ = snap->host;
            status_.home = params_.deliver_to.empty() ? snap->host : params_.deliver_to;
            status_.hosts.push_back(snap->host_name.empty() ? snap->host.substr(0, 8) : snap->host_name);
            // Started on a web host (by a paglet staying there): it downloads here.
            const bool web_here = std::ranges::any_of(snap->offers, [](const svc::mesh_info::Offer& o) {
                return o.service == "web" && std::ranges::find(o.ops, "download") != o.ops.end();
            });
            if (web_here && params_.via == "offer:web.download") {
                (void)paglets::reply_to(*reply, Started{true, {}});
                return download();
            }
            if (auto moved = paglets::dispatch(params_.via); !moved) {
                status_.state = "failed";
                status_.error = error_text("dispatch", moved.error());
                return (void)paglets::reply_to(*reply, Started{false, status_.error});
            }
            paglets::reply_to(*reply, Started{true, {}});
        });
        if (!sent) (void)m.reply(Started{false, error_text("mesh-info", sent.error())});
    }

    // Records where it is now, then goes on.
    void note_host(std::function<void()> next) {
        auto mi = paglets::service("mesh-info");
        if (!mi) return next();
        svc::mesh_info::Client client{*mi};
        auto sent = client.snapshot({}, [this, next](paglets::Result<svc::mesh_info::Snapshot> snap, Message&) {
            if (snap) {
                here_ = snap->host;
                status_.hosts.push_back(snap->host_name.empty() ? snap->host.substr(0, 8) : snap->host_name);
            }
            next();
        });
        if (!sent) next();
    }

    void download() {
        status_.state = "downloading";
        lookup("web", [this](paglets::Result<Endpoint> web) {
            if (!web) return fail_and_return(error_text("web", web.error()));
            svc::web::Client client{*web};
            auto sent = client.download(svc::web::DownloadRequest{params_.url, params_.sha256},
                                        [this](paglets::Result<svc::web::DownloadReply> r, Message& m) {
                                            if (!r) return fail_and_return(error_text("download", r.error()));
                                            status_.http_status = r->status;
                                            if (r->status < 200 || r->status >= 300 || m.cap_count() == 0) {
                                                return fail_and_return("HTTP status " + std::to_string(r->status));
                                            }
                                            blob_ = std::make_shared<Capability>(m.take_cap(0));
                                            status_.size = r->size;
                                            status_.state = "returning";
                                            go_home();
                                        });
            if (!sent) fail_and_return(error_text("download", sent.error()));
        });
    }

    void go_home() {
        if (here_ == status_.home) return deliver();
        if (auto moved = paglets::dispatch(status_.home); !moved) {
            status_.error = error_text("dispatch home", moved.error());
            (void)paglets::after_raw(2000, "retry-home", {});
        }
    }

    // Home again: the home host fetches the content into its store, the
    // owner hears.
    void deliver() {
        if (status_.state == "failed") return notify(svc::user_info::Level::error, "courier failed", status_.error);
        endpoint("artifacts", [this](paglets::Result<Endpoint> store) {
            if (!store) return fail(error_text("artifacts", store.error()));
            store_at_home(*store);
        });
    }

    void store_at_home(Endpoint store) {
        svc::artifacts::Client client{store};
        paglets::RequestOptions options = lending(*blob_);
        options.timeout_ms = 3600 * 1000;  // a large download takes a while to arrive
        auto sent = client.stat(
            svc::artifacts::StatRequest{},
            [this](paglets::Result<svc::artifacts::Info> info, Message&) {
                if (!info) return fail(error_text("fetch home", info.error()));
                status_.artifact = info->hash;
                status_.state = "delivered";
                status_.error.clear();
                notify(svc::user_info::Level::success, "courier delivered " + status_.url,
                       "artifact " + info->hash + ", " + std::to_string(status_.size) + " bytes");
            },
            options);
        if (!sent) fail(error_text("fetch home", sent.error()));
    }

    static void notify(svc::user_info::Level level, std::string title, std::string text) {
        paglets::patterns::notify(level, std::move(title), std::move(text));
    }

    void fail(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
        paglets::log("courier: " + status_.error);
    }

    // The errand failed away from home: the courier comes back to report.
    void fail_and_return(std::string why) {
        fail(std::move(why));
        if (here_ != status_.home) {
            if (!paglets::dispatch(status_.home)) notify(svc::user_info::Level::error, "courier failed", status_.error);
        } else {
            notify(svc::user_info::Level::error, "courier failed", status_.error);
        }
    }

    Start params_;
    Status status_{"idle"};
    std::string here_;
    std::shared_ptr<Capability> blob_;
};

}  // namespace

PAGLETS_PAGLET(Courier)
