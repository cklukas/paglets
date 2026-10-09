// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Duplicate Finder (planning/cpp-demo-paglets.md, demo 4): files with the
// same content anywhere in the mesh, in two phases of fan-out. First every
// host reports the sizes of its files (cheap); then only the files whose
// size occurs more than once are hashed (SHA-256), on the host that has
// them, so the content never travels.

#include "dupes.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>
#include <paglets/patterns/files.hpp>
#include <paglets/sha256.hpp>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {

using namespace dupes_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

class Dupes : public paglets::Paglet {
public:
    Dupes() {
        router().on<Search>("start", [this](const Search& s, Message& m) { start(s, m); });
        router().on("result", [this](Message& m) { (void)m.reply(result_); });
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            HostFiles h;
            if (c.ok && paglets::decode(c.result, h)) {
                for (auto& f : h.files) {
                    f.host_name = c.host_name;
                    found_.push_back(std::move(f));
                }
            } else {
                result_.errors.push_back((c.host_name.empty() ? c.destination : c.host_name) + ": " + c.error);
            }
            if (fan_.finished()) phase_done();
        });
    }

    void on_cloned(paglets::Started& s) override {
        Phase p;
        if (s.caps.empty() || !s.decode_args(p)) {
            (void)paglets::dispose();
            return;
        }
        parent_ = paglets::Endpoint(s.caps[0].release());
        phase_ = p;
        pt::granted_dir(p.search.root, "", {"read"}, "duplicate finder", [this](paglets::Result<paglets::Capability> d) {
            if (!d) return done(pt::error_text("files of " + phase_.search.root, d.error()));
            dir_ = std::make_shared<paglets::Capability>(*d);
            find();
        });
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }

private:
    // -- the parent ---------------------------------------------------------------------

    void start(const Search& s, Message& m) {
        search_ = s;
        result_ = Result{"sizes", 0, 0, {}, 0, {}};
        found_.clear();
        hosts_.clear();
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, reply](std::vector<std::string> hosts) {
            hosts_ = std::move(hosts);
            const auto sent = fan_.start(hosts_, paglets::encode(Phase{1, search_, {}}), search_.timeout_ms);
            (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out"});
        };
        if (!s.hosts.empty()) return go(s.hosts);
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

    void phase_done() {
        if (result_.state == "sizes") {
            result_.files = static_cast<std::int64_t>(found_.size());
            std::map<std::int64_t, int> count;
            for (const auto& f : found_) ++count[f.size];
            std::vector<std::int64_t> sizes;
            for (const auto& [size, n] : count) {
                if (n > 1) sizes.push_back(size);
            }
            result_.candidates = static_cast<std::int64_t>(
                std::ranges::count_if(found_, [&](const File& f) { return count[f.size] > 1; }));
            found_.clear();
            if (sizes.empty()) {
                result_.state = "done";
                return;
            }
            result_.state = "hashing";
            fan_.start(hosts_, paglets::encode(Phase{2, search_, std::move(sizes)}), search_.timeout_ms);
            return;
        }
        // Hashes: groups of two or more.
        std::map<std::string, Group> groups;
        for (auto& f : found_) {
            auto& g = groups[f.sha256];
            g.sha256 = f.sha256;
            g.size = f.size;
            g.copies.push_back(std::move(f));
        }
        for (auto& [hash, g] : groups) {
            if (g.copies.size() < 2) continue;
            result_.wasted += g.size * static_cast<std::int64_t>(g.copies.size() - 1);
            std::ranges::sort(g.copies, [](const File& a, const File& b) {
                return a.host_name != b.host_name ? a.host_name < b.host_name : a.path < b.path;
            });
            result_.groups.push_back(std::move(g));
        }
        std::ranges::sort(result_.groups, [](const Group& a, const Group& b) {
            return a.size * static_cast<std::int64_t>(a.copies.size()) > b.size * static_cast<std::int64_t>(b.copies.size());
        });
        result_.state = "done";
    }

    // -- a clone ------------------------------------------------------------------------

    void find() {
        pt::endpoint("files", [this](paglets::Result<paglets::Endpoint> files) {
            if (!files) return done(pt::error_text("files", files.error()));
            files_ = *files;
            svc::files::Client client{files_};
            svc::files::FindRequest q;
            q.pattern = phase_.search.pattern;
            q.min_size = static_cast<std::uint64_t>(std::max<std::int64_t>(0, phase_.search.min_size));
            q.limit = 100'000;
            auto sent = client.find(
                q,
                [this](paglets::Result<svc::files::FindReply> r, Message&) {
                    if (!r) return done(pt::error_text("find", r.error()));
                    const std::set<std::int64_t> wanted(phase_.sizes.begin(), phase_.sizes.end());
                    for (const auto& e : r->entries) {
                        if (e.type != svc::files::EntryType::file) continue;
                        const auto size = static_cast<std::int64_t>(e.size);
                        if (phase_.phase == 2 && !wanted.contains(size)) continue;
                        mine_.files.push_back(File{{}, e.path, size, {}});
                    }
                    if (phase_.phase == 1) return done({});
                    hash(0);
                },
                pt::lending(*dir_));
            if (!sent) done(pt::error_text("find", sent.error()));
        });
    }

    // Phase 2: the files' SHA-256, one file at a time, here.
    void hash(std::size_t i) {
        if (i >= mine_.files.size()) return done({});
        pt::read_file(*dir_, mine_.files[i].path, 64u * 1024 * 1024, [this, i](paglets::Result<pt::CarriedFile> f) {
            if (f) mine_.files[i].sha256 = paglets::to_hex(paglets::sha256(f->data));
            hash(i + 1);
        });
    }

    void done(std::string error) {
        if (dir_) (void)dir_->drop();
        if (!error.empty()) {
            pt::report_error(parent_, std::move(error));
        } else {
            std::erase_if(mine_.files, [&](const File& f) { return phase_.phase == 2 && f.sha256.empty(); });
            pt::report(parent_, paglets::encode(mine_));
        }
        (void)paglets::after_raw(2000, "dispose", {});
        router().on("dispose", [](Message&) { (void)paglets::dispose(); });
    }

    // The parent.
    pt::FanOut fan_;
    Search search_;
    std::vector<std::string> hosts_;
    std::vector<File> found_;
    Result result_{"idle", 0, 0, {}, 0, {}};
    // A clone.
    paglets::Endpoint parent_;
    paglets::Endpoint files_;
    Phase phase_;
    std::shared_ptr<paglets::Capability> dir_;
    HostFiles mine_;
};

}  // namespace

PAGLETS_PAGLET(Dupes)
