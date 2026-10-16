// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh File Finder and Storage Analyzer (planning/cpp-demo-paglets.md,
// demos 2 and 3): the paglet sends a clone of itself to every host. Each
// clone asks its host for a directory of the named root (the mesh policy
// decides through `grants`), scans matching files with a background task,
// adds up what it found and reports to the parent; the parent puts the
// mesh's answer together.

#include "finder.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>
#include <paglets/patterns/files.hpp>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace finder_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

std::string extension_of(const std::string& path) {
    const auto slash = path.rfind('/');
    const auto dot = path.rfind('.');
    if (dot == std::string::npos || (slash != std::string::npos && dot < slash) || dot + 1 == path.size()) return {};
    std::string ext = path.substr(dot + 1);
    std::ranges::transform(ext, ext.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return ext;
}

class Finder : public paglets::Paglet {
public:
    Finder() {
        router().on<Search>("start", [this](const Search& s, Message& m) { start(s, m); });
        router().on("findings", [this](Message& m) { (void)m.reply(findings_); });
        router().on<Progress>("finder.progress", [this](const Progress& p, Message& m) {
            if (findings_.finished || !m.sender() || m.sender()->id != p.clone) return;
            const auto& children = fan_.children();
            if (!std::ranges::any_of(children, [&](const auto& c) { return !c.done && c.clone == p.clone; })) return;
            auto it = std::ranges::find(findings_.progress, p.clone, &Progress::clone);
            if (it == findings_.progress.end()) findings_.progress.push_back(p);
            else *it = p;
        });
        router().on("cancel", [this](Message& m) {
            findings_.cancelled = true;
            fan_.cancel();
            findings_.finished = true;
            (void)m.reply(findings_);
        });
        router().on("finder.cancel", [this](Message&) {
            if (scan_) (void)scan_.cancel();
            else cancel_requested_ = true;
        });
        // --8<-- [start:collect]
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            HostFindings h;
            if (!c.ok || !paglets::decode(c.result, h)) {
                h = HostFindings{};
                h.error = c.error.empty() ? "no report" : c.error;
                h.cancelled = findings_.cancelled;
                const auto progress = std::ranges::find(findings_.progress, c.clone, &Progress::clone);
                if (progress != findings_.progress.end()) {
                    h.visited = progress->visited;
                    h.files = progress->files;
                    h.bytes = progress->bytes;
                }
            }
            std::erase_if(findings_.progress, [&](const auto& p) { return p.clone == c.clone; });
            h.host = c.host;
            h.host_name = c.host_name.empty() ? c.destination : c.host_name;
            findings_.files += h.files;
            findings_.bytes += h.bytes;
            findings_.hosts.push_back(std::move(h));
            findings_.finished = fan_.finished();
        });
        // --8<-- [end:collect]
    }

    // --8<-- [start:cloned]
    void on_cloned(paglets::Started& s) override {
        Search q;
        if (s.caps.empty() || !s.decode_args(q)) {
            (void)paglets::dispose();
            return;
        }
        parent_ = paglets::Endpoint(s.caps[0].release());
        search_here(q);
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }
    // --8<-- [end:cloned]

private:
    // --8<-- [start:start]
    void start(const Search& s, Message& m) {
        if (s.root.empty() || s.timeout_ms <= 0 || s.max_results < 0 || s.max_results > 1000 || !fan_.finished() || starting_) {
            (void)m.reply(Started{false, "invalid search or a search is already running", 0});
            return;
        }
        findings_ = Findings{};
        starting_ = true;
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, s, reply](std::vector<std::string> hosts) {
            starting_ = false;
            if (findings_.cancelled) {
                findings_.finished = true;
                (void)paglets::reply_to(*reply, Started{false, "cancelled", 0});
                return;
            }
            const auto sent = fan_.start(hosts, paglets::encode(s), s.timeout_ms, "finder.cancel");
            findings_.finished = fan_.finished();
            (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out",
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
    // --8<-- [end:start]

    // -- a clone --------------------------------------------------------------------

    // --8<-- [start:search]
    void search_here(const Search& q) {
        params_ = q;
        if (auto me = paglets::self_info()) clone_id_ = me->id;
        pt::granted_dir(q.root, "", {"read"}, "mesh file finder", [this, q](paglets::Result<paglets::Capability> dir) {
            if (!dir) return finish(std::unexpected(dir.error()));
            dir_ = *dir;
            if (cancel_requested_) return finish(std::unexpected(paglets::abi::cancelled));
            pt::endpoint("files", [this, q](paglets::Result<paglets::Endpoint> files) {
                if (!files) return finish(std::unexpected(files.error()));
                if (cancel_requested_) return finish(std::unexpected(paglets::abi::cancelled));
                svc::files::FindRequest request;
                request.pattern = q.pattern;
                if (q.min_size > 0) request.min_size = static_cast<std::uint64_t>(q.min_size);
                scan_ = pt::scan_files(*files, dir_, request,
                    [this](const svc::files::ScanBatch& batch) { accumulate(batch); },
                    [this](paglets::Result<void> result) { finish(result); }, q.timeout_ms);
            });
        });
    }
    // --8<-- [end:search]

    void accumulate(const svc::files::ScanBatch& batch) {
        mine_.visited = batch.completed;
        for (const auto& e : batch.entries) {
            if (e.type != svc::files::EntryType::file) continue;
            Found f{e.path, static_cast<std::int64_t>(e.size), e.modified_ms};
            ++mine_.files;
            mine_.bytes += f.size;
            auto ext = extension_of(e.path);
            if (!by_ext_.contains(ext) && (by_ext_.size() >= 128 || extension_bytes_ + ext.size() + 64 > 4096)) {
                ext = "(other extensions)";
                mine_.extensions_truncated = true;
            }
            if (!by_ext_.contains(ext)) extension_bytes_ += ext.size() + 64;
            auto& t = by_ext_[ext];
            t.extension = ext;
            ++t.files;
            t.bytes += f.size;
            mine_.found.push_back(f);
            std::ranges::sort(mine_.found, {}, &Found::path);
            if (mine_.found.size() > static_cast<std::size_t>(params_.max_results)) mine_.found.pop_back();
            // Bound samples by bytes as well as count; totals still cover every file.
            std::size_t bytes = 0;
            for (const auto& sample : mine_.found) bytes += sample.path.size() + 64;
            while (bytes > 8 * 1024 && !mine_.found.empty()) {
                bytes -= mine_.found.back().path.size() + 64;
                mine_.found.pop_back();
            }
            mine_.largest.push_back(std::move(f));
            std::ranges::sort(mine_.largest, [](const Found& a, const Found& b) { return a.size > b.size; });
            if (mine_.largest.size() > 5) mine_.largest.pop_back();
            std::size_t largest_bytes = 0;
            for (const auto& sample : mine_.largest) largest_bytes += sample.path.size() + 64;
            while (largest_bytes > 8 * 1024 && !mine_.largest.empty()) {
                largest_bytes -= mine_.largest.back().path.size() + 64;
                mine_.largest.pop_back();
            }
        }
        mine_.truncated = mine_.files > static_cast<std::int64_t>(mine_.found.size());
        const auto now = pt::unix_ms();
        if (batch.finished || now >= next_progress_) {
            paglets::SendOptions options;
            options.async = true;
            (void)parent_.send("finder.progress", Progress{clone_id_, mine_.visited, mine_.files, mine_.bytes}, options);
            next_progress_ = now + 250;
        }
    }

    void finish(paglets::Result<void> result) {
        if (reported_) return;
        reported_ = true;
        (void)dir_.drop();
        if (!result) {
            mine_.error = pt::error_text("scan", result.error());
            mine_.cancelled = result.error() == paglets::abi::cancelled;
        }
        for (auto& [ext, total] : by_ext_) mine_.extensions.push_back(std::move(total));
        std::ranges::sort(mine_.extensions, [](const auto& a, const auto& b) { return a.bytes > b.bytes; });
        pt::report(parent_, paglets::encode(mine_), {}, [] { (void)paglets::dispose(); });
    }

    Search params_;
    HostFindings mine_;
    std::map<std::string, ExtensionTotal> by_ext_;
    paglets::Capability dir_;
    paglets::tasks::TaskHandle scan_;
    std::string clone_id_;
    std::int64_t next_progress_ = 0;
    std::size_t extension_bytes_ = 0;
    bool reported_ = false;
    bool cancel_requested_ = false;
    bool starting_ = false;
    pt::FanOut fan_;
    Findings findings_;
    paglets::Endpoint parent_;
};

}  // namespace

PAGLETS_PAGLET(Finder)
