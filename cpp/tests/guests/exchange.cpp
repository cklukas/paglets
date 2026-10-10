// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Test paglet of the content exchange between hosts
// (planning/cpp-content-exchange.md): it shares a file, puts an artifact,
// moves with the artifact capability, saves and reads the content on other
// hosts.

#include "exchange.schema.gen.hpp"

#include <paglets/patterns.hpp>
#include <paglets/services/artifacts.gen.hpp>

#include <memory>
#include <optional>
#include <string>

namespace {

using namespace exchange_msgs;
namespace pt = paglets::patterns;
namespace svc = paglets::services;
using paglets::Capability;
using paglets::Message;

class Exchange : public paglets::Paglet {
public:
    Exchange() {
        router().on<Step>("share", [this](const Step& s, Message& m) {
            begin("share", m);
            pt::granted_dir(s.root, "", {"read"}, "share", [this, path = s.path](paglets::Result<Capability> d) {
                if (!d) return end(d.error());
                auto dir = std::make_shared<Capability>(*d);
                pt::share_file(*dir, path, [this, dir](paglets::Result<pt::SharedFile> f) {
                    (void)dir->drop();
                    if (!f) return end(f.error());
                    keep(f->content);
                    outcome_.hash = f->sha256;
                    outcome_.size = f->size;
                    end(0);
                });
            });
        });
        router().on<Step>("put", [this](const Step& s, Message& m) {
            begin("put", m);
            pt::endpoint("artifacts", [this, text = s.text](paglets::Result<paglets::Endpoint> store) {
                if (!store) return end(store.error());
                svc::artifacts::Client client{*store};
                auto sent = client.put(svc::artifacts::PutRequest{std::vector<std::uint8_t>(text.begin(), text.end()), "text/plain"},
                                       [this](paglets::Result<svc::artifacts::Info> info, Message& reply) {
                                           if (!info) return end(info.error());
                                           keep(reply.take_cap(0));
                                           outcome_.hash = info->hash;
                                           outcome_.size = info->size;
                                           end(0);
                                       });
                if (!sent) end(sent.error());
            });
        });
        router().on<Step>("save", [this](const Step& s, Message& m) {
            begin("save", m);
            if (!content_) return end(paglets::abi::bad_state);
            pt::granted_dir(s.root, "", {"write", "create"}, "save", [this, path = s.path](paglets::Result<Capability> d) {
                if (!d) return end(d.error());
                auto dir = std::make_shared<Capability>(*d);
                pt::save_file(*dir, path, *content_, [this, dir](paglets::Result<svc::files::Entry> e) {
                    (void)dir->drop();
                    if (!e) return end(e.error());
                    outcome_.size = e->size;
                    end(0);
                });
            });
        });
        router().on<Step>("read", [this](const Step& s, Message& m) {
            begin("read", m);
            if (!content_) return end(paglets::abi::bad_state);
            pt::endpoint("artifacts", [this, s](paglets::Result<paglets::Endpoint> store) {
                if (!store) return end(store.error());
                svc::artifacts::Client client{*store};
                paglets::RequestOptions options = pt::lending(*content_);
                options.timeout_ms = 600 * 1000;
                auto sent = client.get(
                    svc::artifacts::GetRequest{s.offset, s.length},
                    [this](paglets::Result<svc::artifacts::GetReply> r, Message&) {
                        if (!r) return end(r.error());
                        outcome_.size = r->size;
                        outcome_.data.assign(r->data.begin(), r->data.end());
                        end(0);
                    },
                    options);
                if (!sent) end(sent.error());
            });
        });
        router().on<Step>("go", [this](const Step& s, Message& m) {
            begin("go", m);
            if (auto moved = paglets::dispatch(s.host); !moved) end(moved.error());
        });
        router().on("outcome", [this](Message& m) { (void)m.reply(outcome_); });
    }

    void on_arrived(const paglets::abi::ArrivedEvent&) override {
        if (outcome_.step == "go") end(0);
    }

    void on_move_failed(const paglets::abi::MoveFailed&) override {
        if (outcome_.step == "go") end(paglets::abi::failed);
    }

private:
    void begin(std::string step, Message& m) {
        outcome_ = Outcome{std::move(step)};
        (void)m.reply(outcome_);
    }

    void end(std::int32_t status) {
        outcome_.status = status;
        outcome_.done = true;
    }

    void keep(Capability c) {
        if (content_) (void)content_->drop();
        content_ = c;
    }

    Outcome outcome_;
    std::optional<Capability> content_;
};

}  // namespace

PAGLETS_PAGLET(Exchange)
