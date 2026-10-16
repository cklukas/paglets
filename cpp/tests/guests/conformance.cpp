// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// ABI v1 conformance guest (planning/cpp-abi-v1.md, section 11). Requests
// named after an operation make the guest perform it and answer with the
// result code or document; everything the guest sees is recorded in a
// journal that the tests read back with "journal".

#include "conformance_cmd.hpp"

#include <paglets/paglet.hpp>
#include <paglets/async.hpp>

#include <cstdio>

#include <string>
#include <vector>

extern "C" {
__attribute__((import_module("paglets"), import_name("send"))) std::int32_t raw_send(std::int32_t, const void*,
                                                                                     std::uint32_t);
__attribute__((import_module("paglets"), import_name("request"))) std::int64_t raw_request(std::int32_t, const void*,
                                                                                           std::uint32_t);
__attribute__((import_module("paglets"), import_name("self_info"))) std::int32_t raw_self_info(void*, std::uint32_t);
__attribute__((import_module("paglets"), import_name("job_submit"))) std::int64_t raw_job_submit(const void*, std::uint32_t);
__attribute__((import_module("paglets"), import_name("reply"))) std::int32_t raw_reply(std::int32_t, const void*, std::uint32_t);
}

namespace {

using conformance::Cmd;
using paglets::Capability;
using paglets::Endpoint;
using paglets::Message;
namespace abi = paglets::abi;

std::vector<Capability> caps_of(const Cmd& c) {
    std::vector<Capability> caps;
    for (auto h : c.caps) caps.emplace_back(h);
    return caps;
}

std::vector<Capability> lent_of(const Cmd& c) {
    std::vector<Capability> caps;
    for (auto h : c.lend) caps.emplace_back(h);
    return caps;
}

std::int32_t code_of(const paglets::Result<void>& r) {
    return r ? 0 : r.error();
}

void clone_wait(paglets::compute::JobContext& job) {
    while (!job.cancelled()) {}
}
PAGLETS_JOB(clone_wait)

void move_echo(paglets::compute::JobContext& job) {
    if (auto input = job.input()) (void)job.write(*input);
}
PAGLETS_JOB(move_echo)

class Conformance : public paglets::Paglet {
public:
    Conformance() {
        auto& r = router();
        r.on("invalid_reply", [](Message& m) {
            std::vector<std::int32_t> results;
            for (const auto name : {"paglets.spoof", ""}) {
                abi::OutMessage out;
                out.name = name;
                auto doc = abi::encode(out);
                results.push_back(raw_reply(m.reply_handle(), doc.data(), doc.size()));
            }
            abi::OutMessage out;
            out.name = "answer";
            out.priority = abi::max_priority + 1;
            auto doc = abi::encode(out);
            results.push_back(raw_reply(m.reply_handle(), doc.data(), doc.size()));
            m.reply(results);  // the rejected replies did not consume this handle
        });
        r.otherwise([this](Message& m) -> std::int32_t {
            note("msg:" + m.name());
            return abi::not_handled;
        });
        r.on<Cmd>("watch_cleanup", [this](const Cmd& c, Message& m) {
            if (c.code == 0) {
                auto guard = std::make_shared<Cleanup>(&released_[0]);
                auto q = paglets::self().request_raw("defer", {}, [this, guard](Message&) { ++resumed_[0]; });
                (void)m.reply(q ? 0 : q.error());
            } else {
                wait_cleanup(c.code);
                (void)m.reply();
            }
        });
        r.on("cleanup_counts", [this](Message& m) {
            (void)m.reply(std::vector<std::int64_t>{released_[0], released_[1], released_[2],
                                                  resumed_[0], resumed_[1], resumed_[2]});
        });
        r.on("sender_now", [](Message& m) { m.reply(m.sender().value_or(abi::SenderRecord{})); });
        r.on("remember_sender", [this](Message& m) { remembered_sender_ = m.sender().value_or(abi::SenderRecord{}); });
        r.on("stored_sender", [this](Message& m) { m.reply(remembered_sender_); });
        r.on("echo", [this](Message& m) {
            note("msg:echo");
            m.reply_raw(paglets::Bytes(m.payload().begin(), m.payload().end()));
        });
        r.on("journal", [this](Message& m) { m.reply(journal_); });
        r.on<Cmd>("end_hook", [this](const Cmd& c, Message& m) {
            hook_ = c;
            m.reply();
        });
        // Stable memory: the default repeating pattern exercises page reuse;
        // code=1 uses deterministic noise to exercise multi-frame transfers.
        r.on<Cmd>("ballast", [this](const Cmd& c, Message& m) {
            ballast_.assign(static_cast<std::size_t>(c.ms), 0);
            std::uint32_t random = 0x9e3779b9;
            for (std::size_t i = 0; i < ballast_.size(); ++i) {
                if (c.code == 1) {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    ballast_[i] = static_cast<std::uint8_t>(random >> 24);
                } else ballast_[i] = static_cast<std::uint8_t>(i * 131 + 7);
            }
            m.reply(static_cast<std::int64_t>(ballast_.size()));
        });
        r.on("ballast_digest", [this](Message& m) {
            std::uint32_t hash = 2166136261u;
            for (auto byte : ballast_) hash = (hash ^ byte) * 16777619u;
            m.reply(static_cast<std::int64_t>(hash));
        });
        r.on("hops", [this](Message& m) { m.reply(hops_); });
        r.on("count", [this](Message& m) {
            ++count_;
            m.reply(count_);
        });
        r.on("info", [](Message& m) {
            auto info = paglets::self_info();
            if (info) m.reply_raw(abi::encode(*info));
        });
        r.on("caps", [](Message& m) {
            auto caps = paglets::list_caps();
            std::vector<std::int32_t> handles;
            if (caps) {
                // Leaves out the reply capability of this request.
                for (auto& c : *caps) {
                    if (c.handle() != m.reply_handle()) handles.push_back(c.handle());
                }
            }
            m.reply(handles);
        });
        r.on("received_caps", [this](Message& m) {
            std::vector<std::int32_t> handles;
            for (std::size_t i = 0; i < m.cap_count(); ++i) handles.push_back(m.take_cap(i).handle());
            note("caps:" + std::to_string(handles.size()));
            m.reply(handles);
        });
        r.on("take_badged", [this](Message& m) {
            for (std::size_t i = 0; i < m.cap_count(); ++i) (void)m.take_cap(i);
            const auto badge = m.badge().value_or("");
            note("badged:" + badge);
            if (m.reply_handle() > 0) m.reply_raw(paglets::Bytes(badge.begin(), badge.end()));
        });
        r.on<Cmd>("reply_self_transfer", [this](const Cmd& c, Message& m) {
            abi::OutMessage out;
            out.name = "reply";
            out.caps = {m.reply_handle()};
            out.async = c.async;
            const auto doc = abi::encode(out);
            const auto result = raw_reply(m.reply_handle(), doc.data(), doc.size());
            note("reply-self-transfer:" + std::to_string(result));
            if (!c.async) m.reply(result);
        });
        r.on("echo_with_self", [](Message& m) {
            m.reply_raw(paglets::Bytes(m.payload().begin(), m.payload().end()), {paglets::self()});
        });
        r.on<Cmd>("pause_request_then_move", [this](const Cmd& c, Message& m) {
            const auto request = Endpoint(c.handle).request_raw(c.code ? "defer" : "echo_with_self", c.payload, [this](Message& reply) {
                const auto count = reply.cap_count();
                note("moved-reply:" + std::string(abi::error_name(reply.status())) + ":" +
                     std::string(reply.payload().begin(), reply.payload().end()) + ":" + std::to_string(count));
                for (std::size_t i = 0; i < count; ++i) {
                    auto cap = reply.take_cap(i);
                    auto info = cap.inspect();
                    if (info) note("moved-cap:" + info->target);
                }
            }, {.timeout_ms = c.code ? 20 : abi::default_timeout_ms});
            if (!request) { m.reply(request.error()); return; }
            const auto until = paglets::now_ns(abi::Clock::monotonic) + c.ms * 1'000'000;
            while (paglets::now_ns(abi::Clock::monotonic) < until) {}
            m.reply(code_of(paglets::dispatch(c.name)));
        });
        r.on<Cmd>("pause_job_then_move", [this](const Cmd& c, Message& m) {
            auto job = paglets::compute::submit_raw("move_echo", c.payload,
                [this](paglets::Result<paglets::compute::Output> output, Message&) {
                    note(output ? "moved-job:" + std::string(output->data.begin(), output->data.end())
                                : "moved-job-error:" + std::string(abi::error_name(output.error())));
                });
            if (!job) { m.reply(job.error()); return; }
            const auto until = paglets::now_ns(abi::Clock::monotonic) + c.ms * 1'000'000;
            while (paglets::now_ns(abi::Clock::monotonic) < until) {}
            m.reply(code_of(paglets::dispatch(c.name)));
        });
        r.on<Cmd>("pause_timer_then_move", [](const Cmd& c, Message& m) {
            const auto timer = paglets::after_raw(0, "timer-case-queued", {});
            if (!timer) { m.reply(timer.error()); return; }
            const auto until = paglets::now_ns(abi::Clock::monotonic) + c.ms * 1'000'000;
            while (paglets::now_ns(abi::Clock::monotonic) < until) {}
            m.reply(code_of(paglets::dispatch(c.name)));
        });
        r.on<Cmd>("inspect", [](const Cmd& c, Message& m) -> std::int32_t {
            auto info = Capability(c.handle).inspect();
            if (!info) return info.error();
            m.reply_raw(abi::encode(*info));
            return abi::handled;
        });
        r.on<Cmd>("derive", [](const Cmd& c, Message& m) -> std::int32_t {
            abi::DeriveSpec spec;
            if (!abi::decode(c.spec, spec)) return abi::malformed;
            auto derived = Capability(c.handle).derive(spec);
            m.reply(derived ? derived->handle() : derived.error());
            return abi::handled;
        });
        r.on<Cmd>("drop", [](const Cmd& c, Message& m) {
            Capability cap(c.handle);
            m.reply(code_of(cap.drop()));
        });
        r.on<Cmd>("send", [](const Cmd& c, Message& m) {
            paglets::SendOptions options;
            options.priority = c.priority;
            options.caps = caps_of(c);
            options.lend = lent_of(c);
            options.async = c.async;
            m.reply(code_of(Endpoint(c.handle).send_raw(c.name, c.payload, std::move(options))));
        });
        // Queue a self-directed operation before dispatch in the same handler.
        r.on<Cmd>("self_queue_then_move", [this](const Cmd& c, Message& m) {
            if (c.code == 1) {
                paglets::RequestOptions options;
                options.caps = caps_of(c);
                if (c.ms > 0) options.timeout_ms = c.ms;
                const auto request = paglets::self().request_raw("held-self", {}, [this](Message& reply) {
                    note("self-failure:" + std::string(abi::error_name(reply.status())));
                }, std::move(options));
                if (!request) { m.reply(request.error()); return; }
            } else {
                paglets::SendOptions options;
                options.async = true;
                options.caps = caps_of(c);
                const auto sent = paglets::self().send_raw("held-self", {}, std::move(options));
                if (!sent) { m.reply(code_of(sent)); return; }
            }
            m.reply(code_of(paglets::dispatch(c.name)));
        });
        // Requests cannot be asynchronous; the SDK has no option for it.
        r.on<Cmd>("async_request", [](const Cmd& c, Message& m) {
            abi::OutMessage out;
            out.name = c.name;
            out.async = true;
            const paglets::Bytes doc = abi::encode(out);
            m.reply(raw_request(c.handle, doc.data(), static_cast<std::uint32_t>(doc.size())));
        });
        r.on<Cmd>("request", [this](const Cmd& c, Message& m) {
            paglets::RequestOptions options;
            options.caps = caps_of(c);
            options.lend = lent_of(c);
            if (c.ms > 0) options.timeout_ms = c.ms;
            auto id = Endpoint(c.handle).request_raw(
                c.name, c.payload,
                [this](Message& reply) {
                    std::string entry = "reply:" + std::string(abi::error_name(reply.status())) + ":" +
                                        std::string(reply.payload().begin(), reply.payload().end());
                    // Capabilities that came with the reply stay in the table.
                    for (std::size_t i = 0; i < reply.cap_count(); ++i) {
                        entry += (i == 0 ? ":caps=" : ",") + std::to_string(reply.take_cap(i).handle());
                    }
                    note(std::move(entry));
                },
                std::move(options));
            m.reply(id ? static_cast<std::int64_t>(*id) : static_cast<std::int64_t>(id.error()));
        });
        r.on("defer", [this](Message& m) {
            note("msg:defer");
            deferred_.push_back(m.defer_reply());
        });
        r.on<Cmd>("answer", [this](const Cmd& c, Message& m) {
            std::int32_t result = abi::bad_state;
            if (!deferred_.empty()) {
                Capability reply = deferred_.front();
                deferred_.erase(deferred_.begin());
                auto r2 = paglets::reply_to(reply, c.payload, caps_of(c), c.async);
                result = code_of(r2);
            }
            m.reply(result);
        });
        r.on("drop_deferred", [this](Message& m) {
            std::int32_t result = abi::bad_state;
            if (!deferred_.empty()) {
                result = code_of(deferred_.front().drop());
                deferred_.erase(deferred_.begin());
            }
            m.reply(result);
        });
        r.on<Cmd>("fail", [this](const Cmd& c, Message&) -> std::int32_t {
            note("msg:fail");
            return c.code;
        });
        r.on("trap", [](Message&) { __builtin_trap(); });
        r.on("spin", [this](Message&) {
            volatile std::uint64_t n = 0;
            for (;;) n = n + 1;
        });
        r.on("recurse", [](Message& m) { m.reply(recurse(1)); });
        // Keeps the paglet busy so that later messages queue up.
        r.on<Cmd>("busy", [this](const Cmd& c, Message& m) {
            note("msg:busy");
            const std::int64_t until = paglets::now_ns(abi::Clock::monotonic) + c.ms * 1'000'000;
            while (paglets::now_ns(abi::Clock::monotonic) < until) {
            }
            m.reply();
        });
        // WASI stdout and stderr go to the host log.
        r.on<Cmd>("print", [](const Cmd& c, Message& m) {
            std::fwrite(c.payload.data(), 1, c.payload.size(), stdout);
            std::fflush(stdout);
            std::fputs(c.name.c_str(), stderr);  // unbuffered, without newline
            m.reply();
        });
        r.on("malformed_send", [](Message& m) {
            const std::uint8_t junk[] = {0xc1};
            m.reply(raw_send(abi::self_handle, junk, sizeof junk));
        });
        r.on<Cmd>("timer", [](const Cmd& c, Message& m) {
            auto t = paglets::after_raw(c.ms, c.name, c.payload);
            m.reply(t ? t->handle() : t.error());
        });
        r.on<Cmd>("child", [](const Cmd& c, Message& m) {
            std::optional<std::string> module;
            if (!c.name.empty()) module = c.name;
            auto child = paglets::create_child_raw(c.payload, caps_of(c), module);
            m.reply(child ? child->handle() : child.error());
        });
        r.on<Cmd>("lifecycle", [](const Cmd& c, Message& m) {
            switch (static_cast<abi::LifecycleOp>(c.code)) {
                case abi::LifecycleOp::dispose: m.reply(code_of(paglets::dispose())); return;
                case abi::LifecycleOp::deactivate: {
                    std::optional<std::int64_t> wake;
                    if (c.ms > 0) wake = c.ms;
                    m.reply(code_of(paglets::deactivate(wake)));
                    return;
                }
                case abi::LifecycleOp::clone: {
                    std::optional<std::string> destination;
                    if (!c.name.empty()) destination = c.name;
                    auto h = paglets::clone_raw(c.payload, caps_of(c), destination);
                    m.reply(h ? h->handle() : h.error());
                    return;
                }
                case abi::LifecycleOp::dispatch: m.reply(code_of(paglets::dispatch(c.name))); return;
                case abi::LifecycleOp::checkpoint: m.reply(code_of(paglets::checkpoint())); return;
                // Cmd.ms: 1 to come back after a restart, 0 not; Cmd.async: notify.
                case abi::LifecycleOp::restart: m.reply(code_of(paglets::set_restart(c.ms != 0, c.async))); return;
            }
            m.reply(static_cast<std::int32_t>(abi::invalid_argument));
        });
        // Keeps moving (WP13): every Cmd.ms milliseconds the paglet
        // dispatches itself to Cmd.name; refused dispatches (a pin) are noted
        // and tried again. Cmd.ms 0 stops it.
        r.on<Cmd>("roam", [this](const Cmd& c, Message& m) {
            roam_ms_ = c.ms;
            roam_to_ = c.name;
            if (roam_ms_ > 0) (void)paglets::after_raw(roam_ms_, "roam-tick", {});
            m.reply();
        });
        // Passes an out-of-bounds (pointer, length) pair to an import (C27).
        r.on("out_of_bounds", [](Message& m) {
            raw_send(abi::self_handle, reinterpret_cast<const void*>(0x7ffffff0), 0x100000);
            m.reply(std::string("unreachable"));
        });
        // Buffer protocol (C29): a too small buffer is left untouched.
        r.on("small_buffer", [](Message& m) {
            std::uint8_t buf[4] = {0xee, 0xee, 0xee, 0xee};
            const std::int32_t n = raw_self_info(buf, 2);
            const bool untouched = buf[0] == 0xee && buf[1] == 0xee;
            m.reply(std::vector<std::int64_t>{n, untouched ? 1 : 0});
        });
    }

    void on_created(paglets::Started& s) override {
        note("event:created:" + std::string(s.args.begin(), s.args.end()) + ":" + std::to_string(s.caps.size()));
        for (auto& c : s.caps) note("cap:" + std::to_string(c.handle()));
    }
    void on_cloned(paglets::Started& s) override { note("event:cloned:" + std::string(s.args.begin(), s.args.end())); }
    void on_activated() override { note("event:activated"); }
    void on_deactivating() override { note("event:deactivating"); end_hook("deactivate"); }
    void on_disposing() override { paglets::log("conformance paglet disposing"); end_hook("dispose"); }
    void on_dispatching(const abi::DispatchingEvent& e) override {
        note("event:dispatching:" + e.destination);
        end_hook("dispatch");
    }
    void on_arrived(const abi::ArrivedEvent& e) override {
        if (roam_ms_ > 0) {
            ++hops_;
            (void)paglets::after_raw(roam_ms_, "roam-tick", {});
            return;
        }
        note("event:arrived:" + e.from + ":" + std::to_string(e.lost.size()));
        for (const auto& l : e.lost) note("lost:" + l);
    }
    void on_recovered(const abi::Recovered& e) override {
        note(std::string("event:recovered:") + (e.image_ms > 0 && e.restarted_ms >= e.image_ms ? "ok" : "bad"));
    }
    void on_move_failed(const abi::MoveFailed& f) override {
        if (roam_ms_ > 0) {
            (void)paglets::after_raw(roam_ms_, "roam-tick", {});
            return;
        }
        note("move_failed:" + f.destination + ":" + f.reason + ":" + f.clone);
    }
    void on_undelivered(const abi::Undelivered& u) override {
        note("undelivered:" + u.name + ":" + std::string(abi::error_name(u.status)));
    }

    std::int32_t on_message(Message& m) override {
        if (m.name().starts_with("timer-case-") && m.kind() != abi::MessageKind::timer) {
            note("wrong-timer-kind:" + m.name());
            return abi::handled;
        }
        if (m.kind() == abi::MessageKind::timer) {
            if (m.name() == "roam-tick") {
                if (roam_ms_ <= 0) return abi::handled;
                if (auto r = paglets::dispatch(roam_to_); !r) {
                    // Pinned (or refused): stay, and try again later.
                    const std::string entry = "roam:" + std::string(abi::error_name(r.error()));
                    if (journal_.empty() || journal_.back() != entry) note(entry);
                    (void)paglets::after_raw(roam_ms_, "roam-tick", {});
                }
                return abi::handled;
            }
            if (m.name() == "unhandled_timer") return abi::not_handled;
            note("timer:" + m.name());
            return abi::handled;
        }
        return Paglet::on_message(m);
    }

private:
    abi::SenderRecord remembered_sender_;
    void end_hook(std::string_view name) {
        if (hook_.name != name) return;
        const std::string prefix = "end-hook:" + std::string(name) + ":";
        paglets::log(prefix + "entered");
        if (hook_.code == 1) __builtin_trap();
        if (hook_.code == 2) {
            volatile std::uint64_t n = 0;
            for (;;) n = n + 1;
        }
        if (hook_.code == 3) {
            auto report = [&](std::string_view operation, std::int32_t result) {
                paglets::log(prefix + std::string(operation) + ":" + std::to_string(result));
            };
            report("dispose", code_of(paglets::dispose()));
            report("deactivate", code_of(paglets::deactivate()));
            report("dispatch", code_of(paglets::dispatch("b")));
            report("checkpoint", code_of(paglets::checkpoint()));
            auto clone = paglets::clone_raw({}, {});
            report("clone", clone ? clone->handle() : clone.error());
            auto child = paglets::create_child_raw({}, {});
            report("child", child ? child->handle() : child.error());
            abi::JobSpec job;
            job.function = "unused";
            const auto doc = abi::encode(job);
            report("job", static_cast<std::int32_t>(raw_job_submit(doc.data(), doc.size())));
        }
    }

    static std::int32_t recurse(std::int32_t depth) {
        volatile char pad[256];
        pad[0] = static_cast<char>(depth);
        return depth > 1'000'000 ? depth : recurse(depth + 1) + pad[0];
    }

    struct Cleanup {
        explicit Cleanup(std::int64_t* n) : count(n) {}
        ~Cleanup() { ++*count; }
        std::int64_t* count;
    };
    paglets::Async wait_cleanup(int kind) {
        Cleanup guard(&released_[kind]);
        if (kind == 1) {
            (void)co_await paglets::ask_raw(paglets::self(), "defer");
        } else {
            (void)co_await paglets::compute::run_raw("clone_wait", {}, {.max_ms = 5000});
        }
        ++resumed_[kind];
    }
    std::int64_t released_[3]{};
    std::int64_t resumed_[3]{};

    void note(std::string entry) { journal_.push_back(std::move(entry)); }

    std::vector<std::string> journal_;
    Cmd hook_;
    std::vector<std::uint8_t> ballast_;
    std::vector<Capability> deferred_;
    std::int64_t count_ = 0;
    std::int64_t roam_ms_ = 0;
    std::string roam_to_;
    std::int64_t hops_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Conformance)
