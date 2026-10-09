// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh File Finder and Storage Analyzer (planning/cpp-demo-paglets.md,
// demos 2 and 3): the paglet sends a clone of itself to every host. Each
// clone asks its host for a directory of the named root (the mesh policy
// decides through `grants`), finds the matching files with `files.find`,
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
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            HostFindings h;
            if (!c.ok || !paglets::decode(c.result, h)) {
                h = HostFindings{};
                h.error = c.error.empty() ? "no report" : c.error;
            }
            h.host = c.host;
            h.host_name = c.host_name.empty() ? c.destination : c.host_name;
            findings_.files += h.files;
            findings_.bytes += h.bytes;
            findings_.hosts.push_back(std::move(h));
            findings_.finished = fan_.finished();
        });
    }

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

private:
    void start(const Search& s, Message& m) {
        if (s.root.empty()) {
            (void)m.reply(Started{false, "no root", 0});
            return;
        }
        findings_ = Findings{};
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, s, reply](std::vector<std::string> hosts) {
            const auto sent = fan_.start(hosts, paglets::encode(s), s.timeout_ms);
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

    // -- a clone --------------------------------------------------------------------

    void search_here(const Search& q) {
        pt::granted_dir(q.root, "", {"read"}, "mesh file finder", [this, q](paglets::Result<paglets::Capability> dir) {
            if (!dir) return pt::report_error(parent_, pt::error_text("files of " + q.root, dir.error()));
            auto cap = std::make_shared<paglets::Capability>(*dir);
            pt::endpoint("files", [this, q, cap](paglets::Result<paglets::Endpoint> files) {
                if (!files) return pt::report_error(parent_, pt::error_text("files", files.error()));
                svc::files::Client client{*files};
                svc::files::FindRequest f;
                f.pattern = q.pattern;
                if (q.min_size > 0) f.min_size = static_cast<std::uint64_t>(q.min_size);
                f.limit = 100'000;
                auto sent = client.find(
                    f,
                    [this, q, cap](paglets::Result<svc::files::FindReply> r, Message&) {
                        (void)cap->drop();
                        if (!r) return pt::report_error(parent_, pt::error_text("find", r.error()));
                        pt::report(parent_, paglets::encode(analyze(q, *r)));
                        (void)paglets::after_raw(2000, "done", {});
                        router().on("done", [](Message&) { (void)paglets::dispose(); });
                    },
                    pt::lending(*cap));
                if (!sent) pt::report_error(parent_, pt::error_text("find", sent.error()));
            });
        });
    }

    static HostFindings analyze(const Search& q, const svc::files::FindReply& r) {
        HostFindings h;
        h.truncated = r.truncated;
        std::map<std::string, ExtensionTotal> by_ext;
        std::vector<Found> all;
        for (const auto& e : r.entries) {
            if (e.type != svc::files::EntryType::file) continue;
            Found f{e.path, static_cast<std::int64_t>(e.size), e.modified_ms};
            ++h.files;
            h.bytes += f.size;
            auto& t = by_ext[extension_of(e.path)];
            t.extension = extension_of(e.path);
            ++t.files;
            t.bytes += f.size;
            all.push_back(std::move(f));
        }
        std::ranges::sort(all, [](const Found& a, const Found& b) { return a.path < b.path; });
        h.found.assign(all.begin(), all.begin() + static_cast<std::ptrdiff_t>(std::min<std::size_t>(
                                                      all.size(), static_cast<std::size_t>(std::max<std::int64_t>(0, q.max_results)))));
        std::ranges::sort(all, [](const Found& a, const Found& b) { return a.size > b.size; });
        h.largest.assign(all.begin(), all.begin() + static_cast<std::ptrdiff_t>(std::min<std::size_t>(all.size(), 5)));
        for (auto& [ext, t] : by_ext) h.extensions.push_back(t);
        std::ranges::sort(h.extensions, [](const auto& a, const auto& b) { return a.bytes > b.bytes; });
        return h;
    }

    pt::FanOut fan_;
    Findings findings_;
    paglets::Endpoint parent_;
};

}  // namespace

PAGLETS_PAGLET(Finder)
