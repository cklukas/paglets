// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// File Courier (planning/cpp-demo-paglets.md, demo 5): carries files from
// one host to another without shared drives. Before it leaves home it
// checks that both hosts are in the mesh. On the source host it asks for a
// directory of the source root (the mesh policy decides through `grants`),
// finds the files and shares them: the host hashes each file and gives the
// courier an artifact capability, a few bytes. The courier carries only
// these capabilities. On the destination host it saves each file below the
// destination root: that host fetches the content directly from the source
// host and checks its SHA-256 before the file appears. Then the courier
// comes home with a receipt. Files of any size travel this way
// (planning/cpp-content-exchange.md).
//
// Data residency: listing a root marks the courier, so it does not leave a
// host-only root's host (it stays there, refused). For roots whose content
// may go to some hosts, the source host gives the content only to those;
// for others, the delivery is refused and the courier comes home with that
// answer.

#include "file_courier.schema.gen.hpp"

#include <paglets/patterns/files.hpp>
#include <paglets/patterns/services.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace file_courier_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
namespace pt = paglets::patterns;
using paglets::Capability;
using paglets::Message;

class FileCourier : public paglets::Paglet {
public:
    FileCourier() {
        router().on<Start>("start", [this](const Start& s, Message& m) { start(s, m); });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        where([this] {
            if (status_.state == "picking-up") return pick_up();
            if (status_.state == "delivering") return deliver();
            if (status_.state == "returning") arrived_home();
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        // Refused by data residency (the courier listed a root whose names
        // stay on its host), or the host is gone: it stays here.
        status_.state = f.reason.find("data residency") != std::string::npos ? "refused" : "failed";
        status_.error = f.reason;
        paglets::log("file courier: " + f.reason);
    }

private:
    // Learns which host this is, then goes on.
    void where(std::function<void()> next) {
        auto mi = paglets::service("mesh-info");
        if (!mi) return next();
        svc::mesh_info::Client client{*mi};
        auto sent = client.snapshot({}, [this, next](paglets::Result<svc::mesh_info::Snapshot> s, Message&) {
            if (s) {
                here_ = s->host;
                here_name_ = s->host_name;
                status_.hosts.push_back(s->host_name.empty() ? s->host.substr(0, 8) : s->host_name);
            }
            next();
        });
        if (!sent) next();
    }

    static bool same_host(const std::string& wanted, const std::string& key, const std::string& name) {
        return wanted == name || wanted == key || (wanted.size() >= 8 && key.starts_with(wanted));
    }

    bool is_here(const std::string& host) const { return same_host(host, here_, here_name_); }

    void go(const std::string& destination) {
        if (auto moved = paglets::dispatch(destination); !moved) fail(pt::error_text("dispatch to " + destination, moved.error()));
    }

    void fail(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
        drop_carried();
        paglets::log("file courier: " + status_.error);
    }

    void drop_carried() {
        for (auto& f : carried_) (void)f.content.drop();
        carried_.clear();
    }

    void go_home() {
        drop_carried();
        status_.state = "returning";
        if (is_here(home_)) return arrived_home();
        go(home_);
    }

    void arrived_home() {
        status_.state = refused_ ? "refused" : "delivered";
        status_.total_ms = now_ms(abi::Clock::wall) - started_ms_;
    }

    static std::int64_t now_ms(abi::Clock clock) { return paglets::now_ns(clock) / 1'000'000; }

    // -- start: checks before departure ------------------------------------------------------

    void start(const Start& s, Message& m) {
        if (status_.state != "idle" && status_.state != "delivered" && status_.state != "failed" &&
            status_.state != "refused") {
            (void)m.reply(Started{false, "busy: " + status_.state});
            return;
        }
        if (s.from_host.empty() || s.from_root.empty() || s.to_host.empty() || s.to_root.empty()) {
            (void)m.reply(Started{false, "source and destination needed"});
            return;
        }
        params_ = s;
        status_ = Status{"idle"};
        refused_ = false;
        started_ms_ = now_ms(abi::Clock::wall);
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto mi = paglets::service("mesh-info");
        if (!mi) {
            (void)paglets::reply_to(*reply, Started{false, "no mesh-info"});
            return;
        }
        // --8<-- [start:checks]
        // Both hosts must be in the mesh and online before the courier leaves.
        svc::mesh_info::Client client{*mi};
        auto sent = client.landscape(
            svc::mesh_info::LandscapeRequest{}, [this, reply](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
                if (!l || l->hosts.empty()) {
                    (void)paglets::reply_to(*reply, Started{false, "no landscape"});
                    return;
                }
                for (const auto& wanted : {params_.from_host, params_.to_host}) {
                    bool known = false;
                    for (const auto& h : l->hosts) known = known || same_host(wanted, h.host, h.host_name);
                    if (!known) {
                        (void)paglets::reply_to(*reply, Started{false, "unknown or offline host: " + wanted});
                        return;
                    }
                }
                home_ = l->hosts.front().host;  // this host comes first
                status_.state = "picking-up";
                (void)paglets::reply_to(*reply, Started{true, {}});
                where([this] {
                    if (is_here(params_.from_host)) return pick_up();
                    go(params_.from_host);
                });
            });
        if (!sent) (void)paglets::reply_to(*reply, Started{false, pt::error_text("mesh-info", sent.error())});
        // --8<-- [end:checks]
    }

    // -- the source host: find and share --------------------------------------------------

    void pick_up() {
        pt::granted_dir(params_.from_root, "", {"read"}, "file courier: pick up", [this](paglets::Result<Capability> d) {
            if (!d) return fail(pt::error_text("files of " + params_.from_root, d.error()));
            dir_ = std::make_shared<Capability>(*d);
            pt::endpoint("files", [this](paglets::Result<paglets::Endpoint> files) {
                if (!files) return fail(pt::error_text("files", files.error()));
                svc::files::FindRequest q;
                q.pattern = params_.pattern;
                q.limit = static_cast<std::uint32_t>(std::max<std::int64_t>(1, params_.max_files));
                (void)paglets::patterns::find_files(*files, *dir_, q,
                    [this](paglets::Result<svc::files::FindReply> r) {
                        if (!r) return fail(pt::error_text("find", r.error()));
                        found_.clear();
                        for (const auto& e : r->entries) {
                            if (e.type == svc::files::EntryType::file) found_.push_back(e.path);
                        }
                        share_next(0);
                    });
            });
        });
    }

    // The host hashes each file; the courier keeps only the capability.
    void share_next(std::size_t i) {
        if (i >= found_.size()) {
            (void)dir_->drop();
            dir_.reset();
            status_.state = "delivering";
            if (is_here(params_.to_host)) return deliver();
            return go(params_.to_host);
        }
        const std::int64_t began = now_ms(abi::Clock::monotonic);
        // --8<-- [start:share]
        pt::share_file(*dir_, found_[i], [this, i, began](paglets::Result<pt::SharedFile> f) {
            if (!f) return fail(pt::error_text("share " + found_[i], f.error()));
            const std::int64_t took = now_ms(abi::Clock::monotonic) - began;
            status_.bytes += static_cast<std::int64_t>(f->size);
            if (params_.max_bytes > 0 && status_.bytes > params_.max_bytes) {
                (void)f->content.drop();
                return fail("more than " + std::to_string(params_.max_bytes) + " bytes");
            }
            status_.files.push_back(Item{f->name, static_cast<std::int64_t>(f->size), f->sha256, false, took, 0});
            status_.share_ms += took;
            carried_.push_back(std::move(*f));  // name, hash, size and the artifact capability
            share_next(i + 1);
        });
        // --8<-- [end:share]
    }

    // -- the destination host: save -----------------------------------------------------------

    std::string target(const std::string& path) const {
        return params_.to_path.empty() ? path : params_.to_path + "/" + path;
    }

    void deliver() {
        pt::granted_dir(params_.to_root, "", {"write", "create"}, "file courier: deliver",
                        [this](paglets::Result<Capability> d) {
                            if (!d) return fail(pt::error_text("files of " + params_.to_root, d.error()));
                            dir_ = std::make_shared<Capability>(*d);
                            save_next(0);
                        });
    }

    void save_next(std::size_t i) {
        if (i >= carried_.size()) {
            (void)dir_->drop();
            dir_.reset();
            return go_home();
        }
        const std::string path = target(carried_[i].name);
        const auto slash = path.rfind('/');
        pt::make_dirs(*dir_, slash == std::string::npos ? std::string() : path.substr(0, slash),
                      [this, i, path](paglets::Result<void> made) {
                          if (!made) return fail(pt::error_text("mkdir for " + path, made.error()));
                          const std::int64_t began = now_ms(abi::Clock::monotonic);
                          // --8<-- [start:save]
                          // This host fetches the content from the source host and
                          // checks its SHA-256 before the file appears.
                          pt::save_file(*dir_, path, carried_[i].content,
                                        [this, i, path, began](paglets::Result<svc::files::Entry> saved) {
                                            if (saved) {
                                                const std::int64_t took = now_ms(abi::Clock::monotonic) - began;
                                                status_.files[i].verified = true;
                                                status_.files[i].save_ms = took;
                                                status_.save_ms += took;
                                                return save_next(i + 1);
                                            }
                                            if (saved.error() != abi::denied) {
                                                return fail(pt::error_text("save " + path, saved.error()));
                                            }
                                            // The source host keeps the content of its root.
                                            (void)dir_->drop();
                                            dir_.reset();
                                            refused_ = true;
                                            status_.error = "data residency: " + params_.from_host +
                                                            " does not give the content of " + params_.from_root +
                                                            " to " + params_.to_host;
                                            paglets::log("file courier: " + status_.error);
                                            go_home();
                                        });
                          // --8<-- [end:save]
                      });
    }

    Start params_;
    Status status_{"idle"};
    std::string home_;
    std::string here_;
    std::string here_name_;
    std::vector<std::string> found_;
    std::vector<pt::SharedFile> carried_;
    bool refused_ = false;
    std::int64_t started_ms_ = 0;
    std::shared_ptr<Capability> dir_;
};

}  // namespace

PAGLETS_PAGLET(FileCourier)
