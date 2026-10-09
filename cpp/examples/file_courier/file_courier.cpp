// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// File Courier (planning/cpp-demo-paglets.md, demo 5): carries files from
// one host to another without shared drives. Before it leaves home it
// checks that both hosts are in the mesh. On the source host it asks for a
// directory of the source root (the mesh policy decides through `grants`),
// finds the files and reads them into its memory with their SHA-256; on
// the destination host it writes them below the destination root, reads
// them back and compares the hashes; then it comes home with a receipt.
//
// Reading marks the paglet with the source root (data residency): when the
// content may not go to the destination host, the move is refused and the
// courier stays where it read the files.

#include "file_courier.schema.gen.hpp"

#include <paglets/patterns/files.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/sha256.hpp>

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
            if (status_.state == "returning") status_.state = "delivered";
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        // Refused by data residency, or the host is gone: it stays here.
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
        carried_.clear();
        paglets::log("file courier: " + status_.error);
    }

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
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto mi = paglets::service("mesh-info");
        if (!mi) {
            (void)paglets::reply_to(*reply, Started{false, "no mesh-info"});
            return;
        }
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
    }

    // -- the source host: find and read ------------------------------------------------------

    void pick_up() {
        pt::granted_dir(params_.from_root, "", {"read"}, "file courier: pick up", [this](paglets::Result<Capability> d) {
            if (!d) return fail(pt::error_text("files of " + params_.from_root, d.error()));
            dir_ = std::make_shared<Capability>(*d);
            pt::endpoint("files", [this](paglets::Result<paglets::Endpoint> files) {
                if (!files) return fail(pt::error_text("files", files.error()));
                svc::files::Client client{*files};
                svc::files::FindRequest q;
                q.pattern = params_.pattern;
                q.limit = static_cast<std::uint32_t>(std::max<std::int64_t>(1, params_.max_files));
                auto sent = client.find(
                    q,
                    [this](paglets::Result<svc::files::FindReply> r, Message&) {
                        if (!r) return fail(pt::error_text("find", r.error()));
                        found_.clear();
                        for (const auto& e : r->entries) {
                            if (e.type == svc::files::EntryType::file) found_.push_back(e.path);
                        }
                        read_next(0);
                    },
                    pt::lending(*dir_));
                if (!sent) fail(pt::error_text("find", sent.error()));
            });
        });
    }

    void read_next(std::size_t i) {
        if (i >= found_.size()) {
            (void)dir_->drop();
            dir_.reset();
            status_.state = "delivering";
            if (is_here(params_.to_host)) return deliver();
            return go(params_.to_host);
        }
        const auto budget = static_cast<std::uint64_t>(std::max<std::int64_t>(0, params_.max_bytes - status_.bytes));
        pt::read_file(*dir_, found_[i], budget, [this, i](paglets::Result<pt::CarriedFile> f) {
            if (!f) return fail(pt::error_text("read " + found_[i], f.error()));
            Item item;
            item.path = f->name;
            item.size = static_cast<std::int64_t>(f->data.size());
            item.sha256 = paglets::to_hex(paglets::sha256(f->data));
            status_.bytes += item.size;
            status_.files.push_back(std::move(item));
            carried_.push_back(std::move(*f));
            read_next(i + 1);
        });
    }

    // -- the destination host: write, read back, compare -------------------------------------

    std::string target(const std::string& path) const {
        return params_.to_path.empty() ? path : params_.to_path + "/" + path;
    }

    void deliver() {
        pt::granted_dir(params_.to_root, "", {"read", "write", "create"}, "file courier: deliver",
                        [this](paglets::Result<Capability> d) {
                            if (!d) return fail(pt::error_text("files of " + params_.to_root, d.error()));
                            dir_ = std::make_shared<Capability>(*d);
                            write_next(0);
                        });
    }

    void write_next(std::size_t i) {
        if (i >= carried_.size()) {
            (void)dir_->drop();
            dir_.reset();
            carried_.clear();  // delivered: the content stays on the destination host
            status_.state = "returning";
            if (is_here(home_)) {
                status_.state = "delivered";
                return;
            }
            return go(home_);
        }
        const std::string path = target(carried_[i].name);
        const auto slash = path.rfind('/');
        pt::make_dirs(*dir_, slash == std::string::npos ? std::string() : path.substr(0, slash),
                      [this, i, path](paglets::Result<void> made) {
                          if (!made) return fail(pt::error_text("mkdir for " + path, made.error()));
                          pt::write_file(*dir_, path, carried_[i].data, [this, i, path](paglets::Result<void> w) {
                              if (!w) return fail(pt::error_text("write " + path, w.error()));
                              verify(i, path);
                          });
                      });
    }

    void verify(std::size_t i, const std::string& path) {
        const auto size = static_cast<std::uint64_t>(carried_[i].data.size());
        pt::read_file(*dir_, path, size, [this, i, path](paglets::Result<pt::CarriedFile> back) {
            if (!back) return fail(pt::error_text("read back " + path, back.error()));
            status_.files[i].verified = paglets::to_hex(paglets::sha256(back->data)) == status_.files[i].sha256;
            if (!status_.files[i].verified) return fail("checksum differs after writing " + path);
            write_next(i + 1);
        });
    }

    Start params_;
    Status status_{"idle"};
    std::string home_;
    std::string here_;
    std::string here_name_;
    std::vector<std::string> found_;
    std::vector<pt::CarriedFile> carried_;
    std::shared_ptr<Capability> dir_;
};

}  // namespace

PAGLETS_PAGLET(FileCourier)
