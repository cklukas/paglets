// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Web Researcher (planning/cpp-demo-paglets.md, demo 21): a question goes
// out from a host without internet access. The paglet moves to a host that
// offers `web.search`, searches, reads the first pages there
// (`extract_text`), moves to a host that offers `ai.summarize`, summarizes
// every page, and comes home with a report of sources and summaries.

#include "researcher.schema.gen.hpp"

#include <paglets/patterns/notify.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/services/ai.gen.hpp>
#include <paglets/services/web.gen.hpp>

#include <string>
#include <vector>

namespace {

using namespace researcher_msgs;
namespace svc = paglets::services;
namespace pt = paglets::patterns;
using paglets::Message;

class Researcher : public paglets::Paglet {
public:
    Researcher() {
        router().on<Question>("start", [this](const Question& q, Message& m) { start(q, m); });
        router().on("report", [this](Message& m) { (void)m.reply(report_); });
    }

    void on_arrived(const paglets::abi::ArrivedEvent&) override {
        where([this] {
            if (report_.state == "searching") return search();
            if (report_.state == "summarizing") return summarize(0);
            if (report_.state == "returning") return arrived_home();
        });
    }

    void on_move_failed(const paglets::abi::MoveFailed& f) override {
        if (report_.state == "returning") return;  // stays where it is with the report
        fail("no host to go to: " + f.reason);
    }

private:
    void where(std::function<void()> next) {
        pt::here([this, next](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) {
                here_ = s->host;
                report_.hosts.push_back(pt::host_label(*s));
            }
            next();
        });
    }

    void start(const Question& q, Message& m) {
        params_ = q;
        report_ = Report{"searching", q.question, {}, {}, {}};
        (void)m.reply(report_);
        where([this] {
            home_ = here_;
            go(params_.web);
        });
    }

    void go(const std::string& destination) {
        if (auto moved = paglets::dispatch(destination); !moved) fail(pt::error_text("dispatch", moved.error()));
    }

    void fail(std::string why) {
        report_.error = std::move(why);
        report_.state = "failed";
        if (here_ != home_ && !home_.empty()) {
            report_.state = "returning";  // home with what it has
            report_.error = "failed: " + report_.error;
            go(home_);
        }
    }

    // At the web host.
    void search() {
        pt::lookup("web", [this](paglets::Result<paglets::Endpoint> web) {
            if (!web) return fail(pt::error_text("web", web.error()));
            web_ = *web;
            svc::web::Client c{web_};
            auto sent = c.search(svc::web::SearchRequest{params_.question, params_.pages},
                                 [this](paglets::Result<svc::web::SearchReply> r, Message&) {
                                     if (!r) return fail(pt::error_text("search", r.error()));
                                     for (const auto& h : r->results) report_.sources.push_back(Source{h.title, h.url, {}});
                                     texts_.assign(report_.sources.size(), {});
                                     read(0);
                                 });
            if (!sent) fail(pt::error_text("search", sent.error()));
        });
    }

    void read(std::size_t i) {
        if (i >= report_.sources.size()) {
            report_.state = "summarizing";
            return go(params_.ai);
        }
        svc::web::Client c{web_};
        auto sent = c.extract_text(svc::web::ExtractRequest{report_.sources[i].url},
                                   [this, i](paglets::Result<svc::web::ExtractReply> r, Message&) {
                                       if (r) {
                                           texts_[i] = r->text;
                                           if (report_.sources[i].title.empty()) report_.sources[i].title = r->title;
                                       } else {
                                           report_.sources[i].summary = pt::error_text("(not read", r.error()) + ")";
                                       }
                                       read(i + 1);
                                   });
        if (!sent) read(i + 1);
    }

    // At the AI host.
    void summarize(std::size_t i) {
        if (i >= report_.sources.size()) {
            texts_.clear();
            report_.state = "returning";
            return go(home_);
        }
        if (texts_[i].empty()) return summarize(i + 1);
        pt::lookup("ai", [this, i](paglets::Result<paglets::Endpoint> ai) {
            if (!ai) return fail(pt::error_text("ai", ai.error()));
            svc::ai::Client c{*ai};
            auto sent = c.summarize(svc::ai::SummarizeRequest{texts_[i], params_.max_words, ""},
                                    [this, i](paglets::Result<svc::ai::TextReply> r, Message&) {
                                        report_.sources[i].summary = r ? r->text : pt::error_text("(summarize", r.error()) + ")";
                                        summarize(i + 1);
                                    });
            if (!sent) fail(pt::error_text("summarize", sent.error()));
        });
    }

    void arrived_home() {
        const bool failed = report_.error.starts_with("failed: ");
        report_.state = failed ? "failed" : "done";
        pt::notify(failed ? pt::Level::error : pt::Level::success, "research: " + report_.question,
                   std::to_string(report_.sources.size()) + " sources");
    }

    Question params_;
    Report report_{"idle", {}, {}, {}, {}};
    std::string here_;
    std::string home_;
    paglets::Endpoint web_;
    std::vector<std::string> texts_;
};

}  // namespace

PAGLETS_PAGLET(Researcher)
