// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// AI Document Digest demo (planning/cpp-web-ai.md, section 5): a paglet
// visits the hosts that keep documents, reads them there (the host grants
// it a directory by the mesh policy), moves to a host that offers `ai` to
// summarize them, and writes the digest on a third host.
//
// What the paglet read stays in its memory, so data residency applies:
// when a source root's content may not leave its host, the move to the AI
// host is refused and the paglet stays where it read the content.

#include "digest.schema.gen.hpp"

#include <paglets/paglet.hpp>
#include <paglets/services/ai.gen.hpp>
#include <paglets/services/directory.gen.hpp>
#include <paglets/services/files.gen.hpp>
#include <paglets/services/grants.gen.hpp>
#include <paglets/services/mesh_info.gen.hpp>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace digest_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
using paglets::Capability;
using paglets::Endpoint;
using paglets::Message;

constexpr std::uint64_t max_document = 64 * 1024;

void lookup(std::string name, std::function<void(paglets::Result<Endpoint>)> next) {
    auto dir = paglets::service("directory");
    if (!dir) return next(std::unexpected(dir.error()));
    svc::directory::Client client{*dir};
    auto sent = client.lookup(svc::directory::LookupRequest{std::move(name)},
                              [next](paglets::Result<svc::directory::LookupReply> r, Message& m) {
                                  if (!r) return next(std::unexpected(r.error()));
                                  if (m.cap_count() == 0) return next(std::unexpected(abi::internal));
                                  next(Endpoint(m.take_cap(0)));
                              });
    if (!sent) next(std::unexpected(sent.error()));
}

void endpoint(std::string name, std::function<void(paglets::Result<Endpoint>)> next) {
    if (auto ep = paglets::service(name)) return next(*ep);
    lookup(std::move(name), std::move(next));
}

paglets::RequestOptions lending(const Capability& cap) {
    paglets::RequestOptions o;
    o.lend.push_back(cap);
    return o;
}

std::string error_text(std::string_view what, std::int32_t code) {
    return std::string(what) + ": " + std::string(abi::error_name(code));
}

class Digest : public paglets::Paglet {
public:
    Digest() {
        router().on<Start>("start", [this](const Start& s, Message& m) {
            if (status_.state != "idle") {
                (void)m.reply(Started{false, "already started"});
                return;
            }
            if (s.sources.empty() || s.output.host.empty()) {
                (void)m.reply(Started{false, "no sources or no output"});
                return;
            }
            params_ = s;
            status_.state = "collecting";
            (void)m.reply(Started{true, {}});
            where([this] { next_source(); });
        });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        where([this] {
            if (status_.state == "collecting") return collect();
            if (status_.state == "summarizing") return summarize(0);
            if (status_.state == "writing") return write();
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        // Refused by data residency, or no host to go to: it stays here.
        status_.state = f.reason.find("data residency") != std::string::npos ? "refused" : "failed";
        status_.error = f.reason;
        paglets::log("digest: " + f.reason);
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

    bool is_here(const std::string& host) const {
        return host == here_name_ || host == here_ || (host.size() >= 8 && here_.starts_with(host));
    }

    void go(const std::string& destination) {
        if (auto moved = paglets::dispatch(destination); !moved) fail(error_text("dispatch to " + destination, moved.error()));
    }

    void fail(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
        paglets::log("digest: " + status_.error);
    }

    // -- collecting -------------------------------------------------------------------------

    void next_source() {
        if (source_ >= params_.sources.size()) {
            status_.state = "summarizing";
            return go(params_.ai);
        }
        const auto& s = params_.sources[source_];
        if (is_here(s.host)) return collect();
        go(s.host);
    }

    // A directory of the source's root, granted by the mesh policy.
    void with_dir(const std::string& root, const std::vector<std::string>& rights,
                  std::function<void(paglets::Result<std::shared_ptr<Capability>>)> next) {
        endpoint("grants", [root, rights, next](paglets::Result<Endpoint> grants) {
            if (!grants) return next(std::unexpected(grants.error()));
            svc::grants::Client client{*grants};
            svc::grants::Request q;
            q.item = svc::grants::Item{"files", rights, root, ""};
            q.reason = "document digest";
            auto sent = client.request(q, [next](paglets::Result<svc::grants::Answer> a, Message& m) {
                if (!a) return next(std::unexpected(a.error()));
                if (a->status != svc::grants::Status::granted || m.cap_count() == 0)
                    return next(std::unexpected(abi::denied));
                next(std::make_shared<Capability>(m.take_cap(0)));
            });
            if (!sent) next(std::unexpected(sent.error()));
        });
    }

    void collect() {
        const Source s = params_.sources[source_];
        with_dir(s.root, {"read"}, [this, s](paglets::Result<std::shared_ptr<Capability>> dir) {
            if (!dir) return fail(error_text("files of " + s.root, dir.error()));
            dir_ = *dir;
            endpoint("files", [this, s](paglets::Result<Endpoint> files) {
                if (!files) return fail(error_text("files", files.error()));
                files_ = *files;
                svc::files::Client client{files_};
                svc::files::FindRequest q;
                q.pattern = s.pattern;
                q.max_size = max_document;
                q.limit = static_cast<std::uint32_t>(std::max<std::int64_t>(1, params_.max_files));
                auto sent = client.find(
                    q,
                    [this](paglets::Result<svc::files::FindReply> r, Message&) {
                        if (!r) return fail(error_text("find", r.error()));
                        found_.clear();
                        for (const auto& e : r->entries) {
                            if (e.type == svc::files::EntryType::file) found_.push_back(e.path);
                        }
                        read_next(0);
                    },
                    lending(*dir_));
                if (!sent) fail(error_text("find", sent.error()));
            });
        });
    }

    void read_next(std::size_t i) {
        if (i >= found_.size() || static_cast<std::int64_t>(texts_.size()) >= params_.max_files) {
            dir_.reset();
            ++source_;
            return next_source();
        }
        svc::files::Client client{files_};
        auto sent = client.read(
            svc::files::ReadRequest{found_[i], 0, max_document},
            [this, i](paglets::Result<svc::files::ReadReply> r, Message&) {
                if (r) {
                    Document d;
                    d.host = here_name_;
                    d.path = params_.sources[source_].root + "/" + found_[i];
                    d.size = static_cast<std::int64_t>(r->size);
                    status_.documents.push_back(std::move(d));
                    texts_.emplace_back(r->data.begin(), r->data.end());
                }
                read_next(i + 1);
            },
            lending(*dir_));
        if (!sent) fail(error_text("read", sent.error()));
    }

    // -- summarizing --------------------------------------------------------------------------

    void summarize(std::size_t i) {
        if (i >= texts_.size()) {
            texts_.clear();
            status_.state = "writing";
            if (is_here(params_.output.host)) return write();
            return go(params_.output.host);
        }
        lookup("ai", [this, i](paglets::Result<Endpoint> ai) {
            if (!ai) return fail(error_text("ai", ai.error()));
            svc::ai::Client client{*ai};
            svc::ai::SummarizeRequest q;
            q.text = texts_[i];
            q.max_words = params_.max_words;
            auto sent = client.summarize(q, [this, i](paglets::Result<svc::ai::TextReply> r, Message&) {
                if (!r) return fail(error_text("summarize", r.error()));
                status_.documents[i].summary = r->text;
                status_.model = r->model;
                summarize(i + 1);
            });
            if (!sent) fail(error_text("summarize", sent.error()));
        });
    }

    // -- writing ------------------------------------------------------------------------------

    std::string digest_text() const {
        std::string out = "# Document digest\n\nSummaries by " + status_.model + ".\n";
        for (const auto& d : status_.documents) {
            out += "\n## " + d.host + ": " + d.path + "\n\n" + d.summary + "\n";
        }
        return out;
    }

    void write() {
        const Output o = params_.output;
        with_dir(o.root, {"write", "create"}, [this, o](paglets::Result<std::shared_ptr<Capability>> dir) {
            if (!dir) return fail(error_text("files of " + o.root, dir.error()));
            endpoint("files", [this, o, dir = *dir](paglets::Result<Endpoint> files) {
                if (!files) return fail(error_text("files", files.error()));
                svc::files::Client client{*files};
                const std::string text = digest_text();
                auto sent = client.write(
                    svc::files::WriteRequest{o.path, std::vector<std::uint8_t>(text.begin(), text.end()),
                                             svc::files::WriteMode::replace},
                    [this](paglets::Result<svc::files::Entry> r, Message&) {
                        if (!r) return fail(error_text("write", r.error()));
                        status_.state = "written";
                    },
                    lending(*dir));
                if (!sent) fail(error_text("write", sent.error()));
            });
        });
    }

    Start params_;
    Status status_{"idle"};
    std::string here_;
    std::string here_name_;
    std::size_t source_ = 0;
    std::vector<std::string> found_;
    std::vector<std::string> texts_;
    std::shared_ptr<Capability> dir_;
    Endpoint files_;
};

}  // namespace

PAGLETS_PAGLET(Digest)
