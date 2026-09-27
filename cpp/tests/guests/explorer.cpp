// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Test guest for the standard system paglets: every operation goes through
// the generated service clients, and every answer to the host is deferred
// until the chain of service calls is done.

#include "explorer.schema.gen.hpp"

#include <paglets/paglet.hpp>
#include <paglets/services/artifacts.gen.hpp>
#include <paglets/services/directory.gen.hpp>
#include <paglets/services/files.gen.hpp>
#include <paglets/services/grants.gen.hpp>
#include <paglets/services/pubsub.gen.hpp>
#include <paglets/services/server_info.gen.hpp>
#include <paglets/services/storage.gen.hpp>
#include <paglets/services/user_info.gen.hpp>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

namespace abi = paglets::abi;
namespace svc = paglets::services;
using paglets::Capability;
using paglets::Endpoint;
using paglets::Message;

std::string failure(std::string_view step, std::int32_t status) {
    return std::string(step) + ":" + std::string(abi::error_name(status));
}

// Looks up a service in the directory; `next` receives its endpoint.
void lookup(std::string name, std::function<void(paglets::Result<Endpoint>)> next) {
    auto dir = paglets::service("directory");
    if (!dir) {
        next(std::unexpected(dir.error()));
        return;
    }
    svc::directory::Client client{*dir};
    auto sent = client.lookup(svc::directory::LookupRequest{std::move(name)},
                              [next](paglets::Result<svc::directory::LookupReply> r, Message& m) {
                                  if (!r) {
                                      next(std::unexpected(r.error()));
                                  } else if (m.cap_count() == 0) {
                                      next(std::unexpected(abi::internal));
                                  } else {
                                      next(Endpoint(m.take_cap(0)));
                                  }
                              });
    if (!sent) next(std::unexpected(sent.error()));
}

paglets::RequestOptions lending(const Capability& cap) {
    paglets::RequestOptions o;
    o.lend.push_back(cap);
    return o;
}

class Explorer : public paglets::Paglet {
public:
    Explorer() {
        router().on<explorer::Explore>("explore", [this](const explorer::Explore& q, Message& m) { explore(q, m); });
        router().on<explorer::FileOps>("file_ops", [this](const explorer::FileOps& q, Message& m) { file_ops(q, m); });
        router().on<explorer::Explore>("read_file", [](const explorer::Explore& q, Message& m) { read_file(q, m); });
        router().on<explorer::Store>("store", [](const explorer::Store& q, Message& m) { store(q, m); });
        router().on<explorer::Store>("fetch", [](const explorer::Store& q, Message& m) { fetch(q, m); });
        router().on<explorer::Text>("artifact", [](const explorer::Text& q, Message& m) { artifact(q, m); });
        router().on<explorer::Text>("topic", [](const explorer::Text& q, Message& m) { topic(q, m); });
        router().on<explorer::Text>("notify", [](const explorer::Text& q, Message& m) { notify(q, m); });
        router().on<explorer::Name>("publish", [](const explorer::Name& q, Message& m) { publish(q, m); });
        router().on<explorer::Name>("greet", [](const explorer::Name& q, Message& m) { greet(q, m); });
        router().on<explorer::Text>("service_ops", [](const explorer::Text& q, Message& m) { service_ops(q, m); });
        router().on("hello", [this](Message& m) {
            journal_.push_back("hello:" + std::string(m.payload().begin(), m.payload().end()));
        });
        router().on("news", [this](Message& m) {
            journal_.push_back("news:" + std::string(m.payload().begin(), m.payload().end()) + ":" +
                               m.badge().value_or(""));
        });
        router().on("journal", [this](Message& m) { m.reply(journal_); });
        router().on<explorer::Access>("request_access", [](const explorer::Access& q, Message& m) { access(q, m); });
        router().on<explorer::Text>("release", [](const explorer::Text& q, Message& m) { release(q, m); });
        // Late answers of the grants service (after an admin decided).
        router().on<svc::grants::Answer>("grants.granted", [this](const svc::grants::Answer& a, Message& m) {
            const auto handle = m.cap_count() > 0 ? m.take_cap(0).handle() : 0;
            journal_.push_back("granted-late:" + std::to_string(handle) + ":" + a.grant);
        });
        router().on<svc::grants::Answer>("grants.denied", [this](const svc::grants::Answer& a, Message&) {
            journal_.push_back("denied-late:" + a.request + ":" + a.reason);
        });
        router().on("info", [](Message& m) {
            auto info = paglets::self_info();
            if (info) m.reply_raw(abi::encode(*info));
        });
    }

private:
    struct Exploration {
        Capability reply;
        Capability dir;
        explorer::Report report;
        Endpoint files;
        Endpoint info;
    };
    using State = std::shared_ptr<Exploration>;

    static void finish(const State& s) { paglets::reply_to(s->reply, s->report); }
    static void fail(const State& s, std::string_view step, std::int32_t status) {
        s->report.error = failure(step, status);
        finish(s);
    }

    void explore(const explorer::Explore& q, Message& m) {
        auto s = std::make_shared<Exploration>();
        s->reply = m.defer_reply();
        s->dir = Capability(q.dir);
        const std::string pattern = q.pattern;
        lookup("files", [s, pattern](paglets::Result<Endpoint> files) {
            if (!files) return fail(s, "lookup files", files.error());
            s->files = *files;
            svc::files::Client client{s->files};
            svc::files::FindRequest find;
            find.pattern = pattern;
            auto sent = client.find(
                find,
                [s](paglets::Result<svc::files::FindReply> r, Message&) {
                    if (!r) return fail(s, "find", r.error());
                    for (const auto& e : r->entries) s->report.found.push_back(e.path);
                    if (r->entries.empty()) return system_info(s);
                    svc::files::Client reader{s->files};
                    auto read = reader.read(
                        svc::files::ReadRequest{r->entries.front().path, 0, std::nullopt},
                        [s](paglets::Result<svc::files::ReadReply> content, Message&) {
                            if (!content) return fail(s, "read", content.error());
                            s->report.first_content.assign(content->data.begin(), content->data.end());
                            system_info(s);
                        },
                        lending(s->dir));
                    if (!read) fail(s, "read", read.error());
                },
                lending(s->dir));
            if (!sent) fail(s, "find", sent.error());
        });
    }

    static void system_info(const State& s) {
        lookup("server-info", [s](paglets::Result<Endpoint> info) {
            if (!info) return fail(s, "lookup server-info", info.error());
            s->info = *info;
            svc::server_info::Client client{s->info};
            auto sent = client.summary({}, [s](paglets::Result<svc::server_info::Summary> r, Message&) {
                if (!r) return fail(s, "summary", r.error());
                s->report.os = r->os;
                s->report.host_name = r->host_name;
                s->report.architecture = r->architecture;
                s->report.cpu_count = r->cpu_count;
                s->report.memory_total = r->memory_total;
                svc::server_info::Client c{s->info};
                auto load = c.load({}, [s](paglets::Result<svc::server_info::Load> l, Message&) {
                    if (!l) return fail(s, "load", l.error());
                    s->report.cpu_percent = l->cpu_percent;
                    s->report.cores = static_cast<std::uint32_t>(l->per_core.size());
                    s->report.memory_available = l->memory_available;
                    volumes(s);
                });
                if (!load) fail(s, "load", load.error());
            });
            if (!sent) fail(s, "summary", sent.error());
        });
    }

    static void volumes(const State& s) {
        svc::server_info::Client c{s->info};
        auto sent = c.volumes({}, [s](paglets::Result<svc::server_info::Volumes> v, Message&) {
            if (!v) return fail(s, "volumes", v.error());
            s->report.volumes = static_cast<std::uint32_t>(v->volumes.size());
            for (const auto& vol : v->volumes) {
                if (vol.total > s->report.largest_volume) {
                    s->report.largest_volume = vol.total;
                    s->report.largest_volume_free = vol.free;
                }
            }
            svc::server_info::Client p{s->info};
            auto procs = p.processes(svc::server_info::ProcessesRequest{5},
                                     [s](paglets::Result<svc::server_info::Processes> r, Message&) {
                                         if (!r) return fail(s, "processes", r.error());
                                         s->report.processes = r->total;
                                         s->report.processes_listed = static_cast<std::uint32_t>(r->processes.size());
                                         finish(s);
                                     });
            if (!procs) fail(s, "processes", procs.error());
        });
        if (!sent) fail(s, "volumes", sent.error());
    }

    // write a/b.txt (mkdir a first), list a, move to c.txt, stat, delete a.
    struct Ops {
        Capability reply;
        Capability dir;
        Endpoint files;
        std::vector<std::string> log;
    };

    void file_ops(const explorer::FileOps& q, Message& m) {
        auto s = std::make_shared<Ops>();
        s->reply = m.defer_reply();
        s->dir = Capability(q.dir);
        auto done = [s](std::string entry) {
            s->log.push_back(std::move(entry));
            paglets::reply_to(s->reply, s->log);
        };
        lookup("files", [s, done](paglets::Result<Endpoint> files) {
            if (!files) return done(failure("lookup", files.error()));
            s->files = *files;
            svc::files::Client c{s->files};
            (void)c.mkdir(
                svc::files::MkdirRequest{"a", false},
                [s, done](paglets::Result<svc::files::Entry> r, Message&) {
                    s->log.push_back(r ? "mkdir:" + r->path : failure("mkdir", r.error()));
                    svc::files::Client c2{s->files};
                    const std::string text = "hello files";
                    (void)c2.write(
                        svc::files::WriteRequest{"a/b.txt", std::vector<std::uint8_t>(text.begin(), text.end()),
                                                 svc::files::WriteMode::create_new},
                        [s, done](paglets::Result<svc::files::Entry> w, Message&) {
                            s->log.push_back(w ? "write:" + w->path + ":" + std::to_string(w->size)
                                               : failure("write", w.error()));
                            svc::files::Client c3{s->files};
                            (void)c3.list(
                                svc::files::ListRequest{"a"},
                                [s, done](paglets::Result<svc::files::ListReply> l, Message&) {
                                    std::string entry = "list:";
                                    if (l) {
                                        for (const auto& e : l->entries) entry += e.name + ",";
                                    } else {
                                        entry = failure("list", l.error());
                                    }
                                    s->log.push_back(entry);
                                    svc::files::Client c4{s->files};
                                    (void)c4.move(
                                        svc::files::MoveRequest{"a/b.txt", "c.txt"},
                                        [s, done](paglets::Result<svc::files::Entry> mv, Message&) {
                                            s->log.push_back(mv ? "move:" + mv->path : failure("move", mv.error()));
                                            svc::files::Client c5{s->files};
                                            (void)c5.remove(
                                                svc::files::DeleteRequest{"a", true},
                                                [s, done](paglets::Result<svc::files::DeleteReply> d, Message&) {
                                                    done(d ? "remove:" + std::to_string(d->removed)
                                                           : failure("remove", d.error()));
                                                },
                                                lending(s->dir));
                                        },
                                        lending(s->dir));
                                },
                                lending(s->dir));
                        },
                        lending(s->dir));
                },
                lending(s->dir));
        });
    }

    // Reads `pattern` as a path below the lent directory.
    static void read_file(const explorer::Explore& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        Capability dir(q.dir);
        lookup("files", [reply, dir, path = q.pattern](paglets::Result<Endpoint> files) {
            if (!files) return (void)paglets::reply_to(*reply, failure("lookup", files.error()));
            svc::files::Client c{*files};
            auto sent = c.read(
                svc::files::ReadRequest{path, 0, std::nullopt},
                [reply](paglets::Result<svc::files::ReadReply> r, Message&) {
                    paglets::reply_to(*reply, r ? "content:" + std::string(r->data.begin(), r->data.end())
                                                : failure("read", r.error()));
                },
                lending(dir));
            if (!sent) paglets::reply_to(*reply, failure("read", sent.error()));
        });
    }

    static void store(const explorer::Store& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto storage = paglets::service("storage");
        if (!storage) return (void)paglets::reply_to(*reply, failure("service", storage.error()));
        svc::storage::Client c{*storage};
        (void)c.put(
            svc::storage::PutRequest{q.key, q.value}, [reply](paglets::Result<svc::storage::Usage> u, Message&) {
                paglets::reply_to(*reply, u ? "used:" + std::to_string(u->used) + ":keys:" + std::to_string(u->keys)
                                            : failure("put", u.error()));
            });
    }

    static void fetch(const explorer::Store& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto storage = paglets::service("storage");
        if (!storage) return (void)paglets::reply_to(*reply, failure("service", storage.error()));
        svc::storage::Client c{*storage};
        (void)c.get(svc::storage::GetRequest{q.key}, [reply](paglets::Result<svc::storage::GetReply> g, Message&) {
            if (!g) return (void)paglets::reply_to(*reply, failure("get", g.error()));
            paglets::reply_to(
                *reply, g->found ? "value:" + std::string(g->value.begin(), g->value.end()) : std::string("missing"));
        });
    }

    static void artifact(const explorer::Text& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        lookup("artifacts", [reply, text = q.text](paglets::Result<Endpoint> ep) {
            if (!ep) return (void)paglets::reply_to(*reply, failure("lookup", ep.error()));
            Endpoint store = *ep;
            svc::artifacts::Client c{store};
            (void)c.put(svc::artifacts::PutRequest{std::vector<std::uint8_t>(text.begin(), text.end()), "text/plain"},
                        [reply, store](paglets::Result<svc::artifacts::Info> info, Message& msg) {
                            if (!info) return (void)paglets::reply_to(*reply, failure("put", info.error()));
                            Capability blob = msg.take_cap(0);
                            svc::artifacts::Client g{store};
                            (void)g.get(
                                svc::artifacts::GetRequest{7, std::nullopt},
                                [reply, hash = info->hash](paglets::Result<svc::artifacts::GetReply> r, Message&) {
                                    paglets::reply_to(*reply,
                                                      r ? hash + ":" + std::string(r->data.begin(), r->data.end())
                                                        : failure("get", r.error()));
                                },
                                lending(blob));
                        });
        });
    }

    // Creates a topic, subscribes itself as "news" and publishes the text.
    static void topic(const explorer::Text& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        lookup("pubsub", [reply, text = q.text](paglets::Result<Endpoint> ep) {
            if (!ep) return (void)paglets::reply_to(*reply, failure("lookup", ep.error()));
            Endpoint pubsub = *ep;
            svc::pubsub::Client c{pubsub};
            (void)c.create(
                svc::pubsub::CreateRequest{"weather"},
                [reply, pubsub, text](paglets::Result<svc::pubsub::Topic> t, Message& msg) {
                    if (!t) return (void)paglets::reply_to(*reply, failure("create", t.error()));
                    Capability topic = msg.take_cap(0);
                    svc::pubsub::Client s{pubsub};
                    (void)s.subscribe(
                        svc::pubsub::SubscribeRequest{"news"},
                        [reply, pubsub, topic, text](paglets::Result<svc::pubsub::SubscribeReply> sub, Message&) {
                            if (!sub) return (void)paglets::reply_to(*reply, failure("subscribe", sub.error()));
                            svc::pubsub::Client p{pubsub};
                            (void)p.publish(
                                svc::pubsub::PublishRequest{std::vector<std::uint8_t>(text.begin(), text.end()), 3},
                                [reply](paglets::Result<svc::pubsub::PublishReply> r, Message&) {
                                    paglets::reply_to(*reply, r ? "delivered:" + std::to_string(r->delivered)
                                                                : failure("publish", r.error()));
                                },
                                lending(topic));
                        },
                        lending(topic));
                });
        });
    }

    static void notify(const explorer::Text& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto ui = paglets::service("user-info");
        if (!ui) return (void)paglets::reply_to(*reply, failure("service", ui.error()));
        svc::user_info::Client c{*ui};
        (void)c.notify(svc::user_info::NotifyRequest{q.text, "from the explorer", svc::user_info::Level::success},
                       [reply](paglets::Result<svc::user_info::NotifyReply> r, Message&) {
                           paglets::reply_to(*reply,
                                             r ? "notified:" + std::to_string(r->id) : failure("notify", r.error()));
                       });
    }

    static void publish(const explorer::Name& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto dir = paglets::service("directory");
        if (!dir) return (void)paglets::reply_to(*reply, failure("service", dir.error()));
        svc::directory::Client c{*dir};
        (void)c.publish(svc::directory::PublishRequest{q.name, {"hello"}, q.is_public},
                        [reply](paglets::Result<svc::directory::PublishReply> r, Message&) {
                            paglets::reply_to(*reply, r ? "published:" + r->name : failure("publish", r.error()));
                        });
    }

    // Looks up a published name and says hello to it.
    static void greet(const explorer::Name& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        lookup(q.name, [reply](paglets::Result<Endpoint> ep) {
            if (!ep) return (void)paglets::reply_to(*reply, failure("lookup", ep.error()));
            auto sent = ep->send_raw("hello", paglets::Bytes{'h', 'i'});
            const auto denied = ep->send("journal");
            paglets::reply_to(*reply, std::string(sent ? "sent" : failure("send", sent.error())) + ":" +
                                          (denied ? "journal allowed" : failure("journal", denied.error())));
        });
    }

    // The operations of a service endpoint from the directory, and `describe`.
    static void service_ops(const explorer::Text& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        lookup(q.text, [reply](paglets::Result<Endpoint> ep) {
            if (!ep) return (void)paglets::reply_to(*reply, failure("lookup", ep.error()));
            auto info = ep->inspect();
            std::string out = info ? info->target + ":" : failure("inspect", info.error());
            if (info) {
                for (const auto& o : info->ops) out += o + ",";
                out += info->transferable ? "transferable" : "fixed";
            }
            paglets::reply_to(*reply, out);
        });
    }

    // "granted:<handle>:<grant>", "pending:<request>" or "denied:<rule>".
    static void access(const explorer::Access& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto grants = paglets::service("grants");
        if (!grants) return (void)paglets::reply_to(*reply, failure("service", grants.error()));
        svc::grants::Client c{*grants};
        svc::grants::Request request{svc::grants::Item{q.service, q.ops, q.root, q.path}, q.duration_ms, "test"};
        auto sent = c.request(request, [reply](paglets::Result<svc::grants::Answer> a, Message& msg) {
            if (!a) return (void)paglets::reply_to(*reply, failure("request", a.error()));
            switch (a->status) {
                case svc::grants::Status::granted: {
                    const auto handle = msg.cap_count() > 0 ? msg.take_cap(0).handle() : 0;
                    paglets::reply_to(*reply, "granted:" + std::to_string(handle) + ":" + a->grant);
                    return;
                }
                case svc::grants::Status::pending: paglets::reply_to(*reply, "pending:" + a->request); return;
                case svc::grants::Status::denied: paglets::reply_to(*reply, "denied:" + a->rule); return;
            }
        });
        if (!sent) paglets::reply_to(*reply, failure("request", sent.error()));
    }

    static void release(const explorer::Text& q, Message& m) {
        auto reply = std::make_shared<Capability>(m.defer_reply());
        auto grants = paglets::service("grants");
        if (!grants) return (void)paglets::reply_to(*reply, failure("service", grants.error()));
        svc::grants::Client c{*grants};
        (void)c.release(
            svc::grants::ReleaseRequest{q.text}, [reply](paglets::Result<svc::grants::ReleaseReply> r, Message&) {
                paglets::reply_to(
                    *reply, r ? std::string(r->released ? "released" : "unknown") : failure("release", r.error()));
            });
    }

    std::vector<std::string> journal_;
};

}  // namespace

PAGLETS_PAGLET(Explorer)
