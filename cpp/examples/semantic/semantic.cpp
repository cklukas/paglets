// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Semantic Mesh Search (planning/cpp-demo-paglets.md, demo 17): the paglet
// reads documents on the hosts that keep them, moves to a host offering
// `ai.embed`, embeds them there and keeps the index (vectors and excerpts)
// in its memory, an ordinary std::vector. Queries in natural language are
// embedded the same way and answered by cosine similarity. The index stays
// with the paglet: it can move on, checkpoint and come back without a
// database.

#include "semantic.schema.gen.hpp"

#include <paglets/patterns/files.hpp>
#include <paglets/services/ai.gen.hpp>

#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace semantic_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

constexpr std::uint64_t max_document = 64 * 1024;

double cosine(const std::vector<double>& a, const std::vector<double>& b) {
    double dot = 0, na = 0, nb = 0;
    for (std::size_t i = 0; i < std::min(a.size(), b.size()); ++i) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    return na > 0 && nb > 0 ? dot / (std::sqrt(na) * std::sqrt(nb)) : 0;
}

class Semantic : public paglets::Paglet {
public:
    Semantic() {
        router().on<Index>("index", [this](const Index& q, Message& m) {
            params_ = q;
            docs_.clear();
            status_ = Status{"collecting", 0, {}, {}, {}};
            source_ = 0;
            (void)m.reply(status_);
            where([this] { next_source(); });
        });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
        router().on<Query>("query", [this](const Query& q, Message& m) { query(q, m); });
    }

    void on_arrived(const paglets::abi::ArrivedEvent&) override {
        where([this] {
            if (status_.state == "collecting") return collect();
            if (status_.state == "embedding") return embed(0);
        });
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override { fail("move: " + f.reason); }

private:
    struct Doc {
        std::string host_name;
        std::string path;
        std::string text;
        std::vector<double> vector;
    };

    void where(std::function<void()> next) {
        pt::here([this, next](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) {
                here_ = s->host;
                status_.host_name = pt::host_label(*s);
            }
            next();
        });
    }

    bool is_here(const std::string& host) const {
        return host == status_.host_name || host == here_ || (host.size() >= 8 && here_.starts_with(host));
    }

    void fail(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
    }

    void next_source() {
        if (source_ >= params_.sources.size()) {
            status_.state = "embedding";
            if (auto moved = paglets::dispatch(params_.ai); !moved) fail(pt::error_text("dispatch", moved.error()));
            return;
        }
        if (is_here(params_.sources[source_].host)) return collect();
        if (auto moved = paglets::dispatch(params_.sources[source_].host); !moved)
            fail(pt::error_text("dispatch", moved.error()));
    }

    void collect() {
        const Source s = params_.sources[source_];
        pt::granted_dir(s.root, "", {"read"}, "semantic search", [this, s](paglets::Result<paglets::Capability> d) {
            if (!d) return fail(pt::error_text("files of " + s.root, d.error()));
            dir_ = std::make_shared<paglets::Capability>(*d);
            pt::endpoint("files", [this, s](paglets::Result<paglets::Endpoint> files) {
                if (!files) return fail(pt::error_text("files", files.error()));
                svc::files::Client c{*files};
                svc::files::FindRequest q;
                q.pattern = s.pattern;
                q.max_size = max_document;
                q.limit = static_cast<std::uint32_t>(std::max<std::int64_t>(1, params_.max_files));
                auto sent = c.find(
                    q,
                    [this](paglets::Result<svc::files::FindReply> r, Message&) {
                        if (!r) return fail(pt::error_text("find", r.error()));
                        found_.clear();
                        for (const auto& e : r->entries) {
                            if (e.type == svc::files::EntryType::file) found_.push_back(e.path);
                        }
                        read(0);
                    },
                    pt::lending(*dir_));
                if (!sent) fail(pt::error_text("find", sent.error()));
            });
        });
    }

    void read(std::size_t i) {
        if (i >= found_.size()) {
            (void)dir_->drop();
            ++source_;
            return next_source();
        }
        pt::read_file(*dir_, found_[i], max_document, [this, i](paglets::Result<pt::CarriedFile> f) {
            if (f) {
                docs_.push_back(Doc{status_.host_name, params_.sources[source_].root + "/" + found_[i],
                                    std::string(f->data.begin(), f->data.end()), {}});
            }
            read(i + 1);
        });
    }

    // At the AI host: the documents' vectors, a few at a time.
    void embed(std::size_t from) {
        if (from >= docs_.size()) {
            status_.state = "ready";
            status_.documents = static_cast<std::int64_t>(docs_.size());
            return;
        }
        pt::lookup("ai", [this, from](paglets::Result<paglets::Endpoint> ai) {
            if (!ai) return fail(pt::error_text("ai", ai.error()));
            ai_ = *ai;
            svc::ai::EmbedRequest q;
            const std::size_t to = std::min(docs_.size(), from + 16);
            for (std::size_t i = from; i < to; ++i) q.texts.push_back(docs_[i].text);
            svc::ai::Client c{ai_};
            auto sent = c.embed(q, [this, from, to](paglets::Result<svc::ai::EmbedReply> r, Message&) {
                if (!r || r->vectors.size() != to - from) return fail(r ? "embed: wrong count" : pt::error_text("embed", r.error()));
                status_.model = r->model;
                for (std::size_t i = from; i < to; ++i) docs_[i].vector = std::move(r->vectors[i - from].values);
                embed(to);
            });
            if (!sent) fail(pt::error_text("embed", sent.error()));
        });
    }

    void query(const Query& q, Message& m) {
        if (status_.state != "ready") {
            (void)m.reply(Hits{{}, "the index is not ready (" + status_.state + ")"});
            return;
        }
        auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
        svc::ai::Client c{ai_};
        auto sent = c.embed(svc::ai::EmbedRequest{{q.text}, status_.model},
                            [this, q, reply](paglets::Result<svc::ai::EmbedReply> r, Message&) {
                                Hits hits;
                                if (!r || r->vectors.empty()) {
                                    hits.error = r ? "no vector" : pt::error_text("embed", r.error());
                                    (void)paglets::reply_to(*reply, hits);
                                    return;
                                }
                                for (const auto& d : docs_) {
                                    std::string excerpt = d.text.substr(0, 120);
                                    hits.hits.push_back(Hit{d.host_name, d.path, cosine(r->vectors[0].values, d.vector),
                                                            std::move(excerpt)});
                                }
                                std::ranges::sort(hits.hits, [](const Hit& a, const Hit& b) { return a.score > b.score; });
                                hits.hits.resize(std::min<std::size_t>(
                                    hits.hits.size(), static_cast<std::size_t>(std::max<std::int64_t>(1, q.limit))));
                                (void)paglets::reply_to(*reply, hits);
                            });
        if (!sent) (void)paglets::reply_to(*reply, Hits{{}, pt::error_text("embed", sent.error())});
    }

    Index params_;
    Status status_{"idle", 0, {}, {}, {}};
    std::size_t source_ = 0;
    std::string here_;
    std::vector<std::string> found_;
    std::shared_ptr<paglets::Capability> dir_;
    std::vector<Doc> docs_;
    paglets::Endpoint ai_;
};

}  // namespace

PAGLETS_PAGLET(Semantic)
