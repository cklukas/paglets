// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Tree Compare (planning/cpp-demo-paglets.md, demo 6): is a directory tree
// the same on several hosts? The paglet sends a clone to every host; each
// clone asks its host for the directory (the mesh policy decides through
// `grants`), lists the files with their sizes and times and, if asked,
// hashes them (SHA-256) where they are, so the content never travels. The
// parent reports the files missing on some hosts and the files that differ
// in size or content.

#include "tree_compare.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>
#include <paglets/patterns/files.hpp>
#include <paglets/sha256.hpp>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace tree_compare_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

constexpr std::size_t max_differences = 1000;

class TreeCompare : public paglets::Paglet {
public:
    TreeCompare() {
        router().on<Compare>("start", [this](const Compare& c, Message& m) { start(c, m); });
        router().on("report", [this](Message& m) { (void)m.reply(report_); });
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            Scan s;
            s.name = c.host_name.empty() ? (c.destination.empty() ? "here" : c.destination) : c.host_name;
            s.ok = c.ok && paglets::decode(c.result, s.tree);
            if (!s.ok) report_.errors.push_back(s.name + ": " + (c.error.empty() ? "no report" : c.error));
            for (std::size_t i = 0; i < destinations_.size(); ++i) {
                if (destinations_[i] == c.destination && !scans_[i].done) {
                    s.done = true;
                    scans_[i] = std::move(s);
                    break;
                }
            }
            if (report_.state == "scanning" && std::ranges::all_of(scans_, [](const Scan& x) { return x.done; })) compare();
        });
    }

    void on_cloned(paglets::Started& s) override {
        Compare q;
        if (s.caps.empty() || !s.decode_args(q)) {
            (void)paglets::dispose();
            return;
        }
        parent_ = paglets::Endpoint(s.caps[0].release());
        scan_here(q);
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }

private:
    struct Scan {
        std::string name;
        bool done = false;
        bool ok = false;
        HostTree tree;
    };

    // -- the parent -----------------------------------------------------------------------

    void start(const Compare& q, Message& m) {
        if (q.root.empty()) {
            (void)m.reply(Started{false, "no root", 0});
            return;
        }
        if (report_.state == "scanning") {
            (void)m.reply(Started{false, "already scanning", 0});
            return;
        }
        report_ = Report{"scanning"};
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, q, reply](std::vector<std::string> hosts) {
            if (hosts.size() < 2) {
                report_.state = "idle";
                (void)paglets::reply_to(*reply, Started{false, "at least two hosts needed", 0});
                return;
            }
            destinations_ = hosts;
            scans_.assign(hosts.size(), Scan{});
            const auto sent = fan_.start(hosts, paglets::encode(q), q.timeout_ms);
            (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out",
                                                    static_cast<std::int64_t>(hosts.size())});
        };
        if (!q.hosts.empty()) return go(q.hosts);
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

    void compare() {
        // path -> the scanned hosts' entries (index into scans_)
        std::map<std::string, std::vector<std::pair<std::size_t, const TreeEntry*>>> by_path;
        std::vector<std::size_t> scanned;
        for (std::size_t i = 0; i < scans_.size(); ++i) {
            report_.hosts.push_back(scans_[i].name);
            if (!scans_[i].ok) continue;
            scanned.push_back(i);
            report_.truncated = report_.truncated || scans_[i].tree.truncated;
            for (const auto& e : scans_[i].tree.entries) by_path[e.path].emplace_back(i, &e);
        }
        for (const auto& [path, have] : by_path) {
            ++report_.files;
            Difference d;
            d.path = path;
            for (const auto& [i, e] : have) d.copies.push_back(Copy{scans_[i].name, e->size, e->modified_ms, e->sha256});
            if (have.size() < scanned.size()) {
                d.kind = "missing";
                for (const auto i : scanned) {
                    if (std::ranges::none_of(have, [i](const auto& h) { return h.first == i; }))
                        d.missing_on.push_back(scans_[i].name);
                }
            } else if (std::ranges::any_of(have, [&](const auto& h) { return h.second->size != have.front().second->size; })) {
                d.kind = "size";
            } else if (std::ranges::any_of(have, [&](const auto& h) { return h.second->sha256 != have.front().second->sha256; })) {
                d.kind = "content";
            } else {
                ++report_.same;
                continue;
            }
            if (report_.differences.size() < max_differences) {
                report_.differences.push_back(std::move(d));
            } else {
                report_.truncated = true;
            }
        }
        report_.state = "done";
    }

    // -- a clone --------------------------------------------------------------------------

    void scan_here(const Compare& q) {
        pt::granted_dir(q.root, q.path, {"read"}, "tree compare", [this, q](paglets::Result<paglets::Capability> dir) {
            if (!dir) return finish_error(pt::error_text("files of " + q.root, dir.error()));
            dir_ = std::make_shared<paglets::Capability>(*dir);
            pt::endpoint("files", [this, q](paglets::Result<paglets::Endpoint> files) {
                if (!files) return finish_error(pt::error_text("files", files.error()));
                svc::files::Client client{*files};
                svc::files::FindRequest f;
                f.pattern = q.pattern;
                f.limit = 100'000;
                auto sent = client.find(
                    f,
                    [this, q](paglets::Result<svc::files::FindReply> r, Message&) {
                        if (!r) return finish_error(pt::error_text("find", r.error()));
                        tree_.truncated = r->truncated;
                        for (const auto& e : r->entries) {
                            if (e.type != svc::files::EntryType::file) continue;
                            tree_.entries.push_back(TreeEntry{e.path, static_cast<std::int64_t>(e.size), e.modified_ms, {}});
                        }
                        if (!q.hashes) return finish();
                        max_file_bytes_ = static_cast<std::uint64_t>(std::max<std::int64_t>(0, q.max_file_bytes));
                        hash_next(0);
                    },
                    pt::lending(*dir_));
                if (!sent) finish_error(pt::error_text("find", sent.error()));
            });
        });
    }

    void hash_next(std::size_t i) {
        while (i < tree_.entries.size() && static_cast<std::uint64_t>(tree_.entries[i].size) > max_file_bytes_) ++i;
        if (i >= tree_.entries.size()) return finish();
        pt::read_file(*dir_, tree_.entries[i].path, max_file_bytes_, [this, i](paglets::Result<pt::CarriedFile> f) {
            if (f) tree_.entries[i].sha256 = paglets::to_hex(paglets::sha256(f->data));
            hash_next(i + 1);
        });
    }

    void finish() {
        (void)dir_->drop();
        pt::report(parent_, paglets::encode(tree_), {}, [] { (void)paglets::dispose(); });
    }

    void finish_error(const std::string& why) {
        if (dir_) (void)dir_->drop();
        pt::report_error(parent_, why, [] { (void)paglets::dispose(); });
    }

    pt::FanOut fan_;
    Report report_{"idle"};
    std::vector<std::string> destinations_;
    std::vector<Scan> scans_;
    paglets::Endpoint parent_;
    std::shared_ptr<paglets::Capability> dir_;
    HostTree tree_;
    std::uint64_t max_file_bytes_ = 0;
};

}  // namespace

PAGLETS_PAGLET(TreeCompare)
