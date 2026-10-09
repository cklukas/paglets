// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Inventory Collector, Process Finder and Mesh Top
// (planning/cpp-demo-paglets.md, demos 8, 10 and 11): a clone on every
// host asks `server-info` (the mesh policy decides which operations) for
// the system summary, load, volumes and processes, in the same schema on
// macOS, Linux and Windows, and reports; the parent keeps the mesh's
// inventory. With a process name it finds those processes everywhere.

#include "inventory.schema.gen.hpp"

#include <paglets/patterns/fanout.hpp>
#include <paglets/services/server_info.gen.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace inventory_msgs;
namespace svc = paglets::services;
namespace si = paglets::services::server_info;
namespace pt = paglets::patterns;
using paglets::Message;

class Inventory : public paglets::Paglet {
public:
    Inventory() {
        router().on<Collect>("start", [this](const Collect& c, Message& m) { start(c, m); });
        router().on("report", [this](Message& m) { (void)m.reply(report_); });
        fan_.attach(router(), [this](const pt::FanOut::Child& c) {
            HostInventory h;
            if (!c.ok || !paglets::decode(c.result, h)) {
                h = HostInventory{};
                h.error = c.error.empty() ? "no report" : c.error;
            }
            h.host = c.host;
            h.host_name = c.host_name.empty() ? c.destination : c.host_name;
            report_.hosts.push_back(std::move(h));
            report_.finished = fan_.finished();
            if (report_.finished) {
                std::ranges::sort(report_.hosts, [](const auto& a, const auto& b) { return a.host_name < b.host_name; });
            }
        });
    }

    void on_cloned(paglets::Started& s) override {
        Collect q;
        if (s.caps.empty() || !s.decode_args(q)) {
            (void)paglets::dispose();
            return;
        }
        parent_ = paglets::Endpoint(s.caps[0].release());
        query_ = q;
        pt::lookup("server-info", [this](paglets::Result<paglets::Endpoint> ep) {
            if (!ep) return finish(pt::error_text("server-info", ep.error()));
            info_ = *ep;
            summary();
        });
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (!f.clone.empty()) fan_.clone_failed(f);
    }

private:
    void start(const Collect& c, Message& m) {
        report_ = Report{};
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        auto go = [this, c, reply](std::vector<std::string> hosts) {
            const auto sent = fan_.start(hosts, paglets::encode(c), c.timeout_ms);
            (void)paglets::reply_to(*reply, Started{sent > 0, sent > 0 ? "" : "no clone went out",
                                                    static_cast<std::int64_t>(hosts.size())});
        };
        if (!c.hosts.empty()) return go(c.hosts);
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

    // -- a clone: summary, load, volumes, processes, then the report -------------------

    void summary() {
        si::Client c{info_};
        auto sent = c.summary({}, [this](paglets::Result<si::Summary> s, Message&) {
            if (!s) return finish(pt::error_text("summary", s.error()));
            inv_.os = s->os;
            inv_.os_version = s->os_version;
            inv_.architecture = s->architecture;
            inv_.cpu_model = s->cpu_model;
            inv_.cpus = s->cpu_count;
            inv_.memory_total = static_cast<std::int64_t>(s->memory_total);
            inv_.uptime_s = static_cast<std::int64_t>(s->uptime_s);
            load();
        });
        if (!sent) finish(pt::error_text("summary", sent.error()));
    }

    void load() {
        si::Client c{info_};
        auto sent = c.load({}, [this](paglets::Result<si::Load> l, Message&) {
            if (l) {
                inv_.cpu_percent = l->cpu_percent;
                inv_.memory_available = static_cast<std::int64_t>(l->memory_available);
            }
            volumes();
        });
        if (!sent) volumes();
    }

    void volumes() {
        si::Client c{info_};
        auto sent = c.volumes({}, [this](paglets::Result<si::Volumes> v, Message&) {
            if (v) {
                for (const auto& x : v->volumes) {
                    inv_.volumes.push_back(VolumeInfo{x.mount_point, x.filesystem, static_cast<std::int64_t>(x.total),
                                                      static_cast<std::int64_t>(x.available)});
                }
            }
            processes();
        });
        if (!sent) processes();
    }

    void processes() {
        si::Client c{info_};
        si::ProcessesRequest q;
        q.limit = query_.process.empty() ? static_cast<std::uint32_t>(std::max<std::int64_t>(0, query_.top)) : 10'000;
        auto sent = c.processes(q, [this](paglets::Result<si::Processes> p, Message&) {
            if (p) {
                inv_.processes_total = p->total;
                for (const auto& x : p->processes) {
                    if (!query_.process.empty() && x.name.find(query_.process) == std::string::npos) continue;
                    inv_.processes.push_back(ProcessInfo{x.pid, x.name, static_cast<std::int64_t>(x.memory)});
                }
            }
            finish({});
        });
        if (!sent) finish({});
    }

    void finish(std::string error) {
        if (!error.empty()) return pt::report_error(parent_, std::move(error));
        pt::report(parent_, paglets::encode(inv_));
        (void)paglets::after_raw(2000, "done", {});
        router().on("done", [](Message&) { (void)paglets::dispose(); });
    }

    // The parent.
    pt::FanOut fan_;
    Report report_;
    // A clone.
    paglets::Endpoint parent_;
    paglets::Endpoint info_;
    Collect query_;
    HostInventory inv_;
};

}  // namespace

PAGLETS_PAGLET(Inventory)
