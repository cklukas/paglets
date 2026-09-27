// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// ABI v1 conformance guest (planning/cpp-abi-v1.md, section 11). Requests
// named after an operation make the guest perform it and answer with the
// result code or document; everything the guest sees is recorded in a
// journal that the tests read back with "journal".

#include "conformance_cmd.hpp"

#include <paglets/paglet.hpp>

#include <cstdio>

#include <string>
#include <vector>

extern "C" {
__attribute__((import_module("paglets"), import_name("send"))) std::int32_t raw_send(std::int32_t, const void*,
                                                                                     std::uint32_t);
__attribute__((import_module("paglets"), import_name("self_info"))) std::int32_t raw_self_info(void*, std::uint32_t);
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

std::int32_t code_of(const paglets::Result<void>& r) {
    return r ? 0 : r.error();
}

class Conformance : public paglets::Paglet {
public:
    Conformance() {
        auto& r = router();
        r.otherwise([this](Message& m) -> std::int32_t {
            note("msg:" + m.name());
            return abi::not_handled;
        });
        r.on("echo", [this](Message& m) {
            note("msg:echo");
            m.reply_raw(paglets::Bytes(m.payload().begin(), m.payload().end()));
        });
        r.on("journal", [this](Message& m) { m.reply(journal_); });
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
            m.reply(code_of(Endpoint(c.handle).send_raw(c.name, c.payload, std::move(options))));
        });
        r.on<Cmd>("request", [this](const Cmd& c, Message& m) {
            paglets::RequestOptions options;
            options.caps = caps_of(c);
            if (c.ms > 0) options.timeout_ms = c.ms;
            auto id = Endpoint(c.handle).request_raw(
                c.name, c.payload,
                [this](Message& reply) {
                    note("reply:" + std::string(abi::error_name(reply.status())) + ":" +
                         std::string(reply.payload().begin(), reply.payload().end()));
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
                auto r2 = paglets::reply_to(reply, c.payload);
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
                    auto h = paglets::clone_raw(c.payload, caps_of(c));
                    m.reply(h ? h->handle() : h.error());
                    return;
                }
                case abi::LifecycleOp::dispatch: m.reply(code_of(paglets::dispatch(c.name))); return;
            }
            m.reply(static_cast<std::int32_t>(abi::invalid_argument));
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
    void on_deactivating() override { note("event:deactivating"); }
    void on_disposing() override { paglets::log("conformance paglet disposing"); }

    std::int32_t on_message(Message& m) override {
        if (m.kind() == abi::MessageKind::timer) {
            note("timer:" + m.name());
            return abi::handled;
        }
        return Paglet::on_message(m);
    }

private:
    static std::int32_t recurse(std::int32_t depth) {
        volatile char pad[256];
        pad[0] = static_cast<char>(depth);
        return depth > 1'000'000 ? depth : recurse(depth + 1) + pad[0];
    }

    void note(std::string entry) { journal_.push_back(std::move(entry)); }

    std::vector<std::string> journal_;
    std::vector<Capability> deferred_;
    std::int64_t count_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Conformance)
