// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Guest side of the paglet ABI v1: exports called by the host and wrappers
// around the `paglets` imports.

#include <paglets/paglet.hpp>

#include <cstdlib>

#define PAGLETS_IMPORT(name) __attribute__((import_module("paglets"), import_name(#name)))
#define PAGLETS_EXPORT(name) __attribute__((export_name(#name)))

extern "C" {

PAGLETS_IMPORT(log) void paglets_log(std::int32_t level, const void* text, std::uint32_t len);
PAGLETS_IMPORT(self_info) std::int32_t paglets_self_info(void* out, std::uint32_t cap);
PAGLETS_IMPORT(send) std::int32_t paglets_send(std::int32_t endpoint, const void* msg, std::uint32_t len);
PAGLETS_IMPORT(request) std::int64_t paglets_request(std::int32_t endpoint, const void* msg, std::uint32_t len);
PAGLETS_IMPORT(reply) std::int32_t paglets_reply(std::int32_t reply, const void* msg, std::uint32_t len);
PAGLETS_IMPORT(cap_derive) std::int32_t paglets_cap_derive(std::int32_t handle, const void* spec, std::uint32_t len);
PAGLETS_IMPORT(cap_drop) std::int32_t paglets_cap_drop(std::int32_t handle);
PAGLETS_IMPORT(cap_inspect) std::int32_t paglets_cap_inspect(std::int32_t handle, void* out, std::uint32_t cap);
PAGLETS_IMPORT(cap_list) std::int32_t paglets_cap_list(void* out, std::uint32_t cap);
PAGLETS_IMPORT(create_child) std::int32_t paglets_create_child(const void* spec, std::uint32_t len);
PAGLETS_IMPORT(lifecycle) std::int32_t paglets_lifecycle(std::int32_t op, const void* arg, std::uint32_t len);
PAGLETS_IMPORT(timer_set) std::int32_t paglets_timer_set(std::int64_t delay_ms, const void* msg, std::uint32_t len);
PAGLETS_IMPORT(now) std::int64_t paglets_now(std::int32_t clock);
PAGLETS_IMPORT(random) std::int32_t paglets_random(void* out, std::uint32_t len);

}  // extern "C"

namespace paglets {

namespace {

// Continuations of outstanding requests, by correlation ID. They live in
// linear memory and therefore survive deactivation and moves.
std::map<std::uint64_t, ReplyHandler>& pending_replies() {
    static std::map<std::uint64_t, ReplyHandler> pending;
    return pending;
}

// Calls an import that writes a document into (out, cap), growing the buffer
// until the document fits.
template <class F>
Result<Bytes> read_document(F&& call) {
    Bytes buf(256);
    for (int attempt = 0; attempt < 8; ++attempt) {
        const std::int32_t n = call(buf.data(), static_cast<std::uint32_t>(buf.size()));
        if (n < 0) return std::unexpected(n);
        if (static_cast<std::size_t>(n) <= buf.size()) {
            buf.resize(static_cast<std::size_t>(n));
            return buf;
        }
        buf.resize(static_cast<std::size_t>(n));
    }
    return std::unexpected(abi::internal);
}

std::vector<std::int32_t> handles_of(const std::vector<Capability>& caps) {
    std::vector<std::int32_t> handles;
    handles.reserve(caps.size());
    for (const auto& c : caps) handles.push_back(c.handle());
    return handles;
}

// After a successful transfer the sender's handles are gone.
void released(std::vector<Capability>& caps) {
    for (auto& c : caps) c.release();
}

Result<void> status(std::int32_t code) {
    if (code < 0) return std::unexpected(code);
    return {};
}

}  // namespace

// ---------------------------------------------------------------------------

void log(abi::LogLevel level, std::string_view text) {
    paglets_log(static_cast<std::int32_t>(level), text.data(), static_cast<std::uint32_t>(text.size()));
}

std::int64_t now_ns(abi::Clock clock) {
    return paglets_now(static_cast<std::int32_t>(clock));
}

void random_bytes(std::span<std::uint8_t> out) {
    paglets_random(out.data(), static_cast<std::uint32_t>(out.size()));
}

Result<abi::CapInfo> Capability::inspect() const {
    auto doc = read_document([&](void* out, std::uint32_t cap) { return paglets_cap_inspect(handle_, out, cap); });
    if (!doc) return std::unexpected(doc.error());
    abi::CapInfo info;
    if (!abi::decode(*doc, info)) return std::unexpected(abi::malformed);
    return info;
}

Result<Capability> Capability::derive(const abi::DeriveSpec& spec) const {
    const Bytes doc = abi::encode(spec);
    const std::int32_t h = paglets_cap_derive(handle_, doc.data(), static_cast<std::uint32_t>(doc.size()));
    if (h < 0) return std::unexpected(h);
    return Capability(h);
}

Result<void> Capability::drop() {
    const std::int32_t r = paglets_cap_drop(handle_);
    if (r == 0) handle_ = 0;
    return status(r);
}

Result<void> Endpoint::send_raw(std::string_view name, Bytes payload, SendOptions options) const {
    abi::OutMessage m;
    m.name = std::string(name);
    m.payload = std::move(payload);
    m.caps = handles_of(options.caps);
    m.lend = handles_of(options.lend);
    m.priority = options.priority;
    m.async = options.async;
    const Bytes doc = abi::encode(m);
    const std::int32_t r = paglets_send(handle(), doc.data(), static_cast<std::uint32_t>(doc.size()));
    if (r == 0) released(options.caps);
    return status(r);
}

Result<std::uint64_t> Endpoint::request_raw(std::string_view name, Bytes payload, ReplyHandler on_reply,
                                            RequestOptions options) const {
    abi::OutMessage m;
    m.name = std::string(name);
    m.payload = std::move(payload);
    m.caps = handles_of(options.caps);
    m.lend = handles_of(options.lend);
    m.priority = options.priority;
    m.timeout_ms = options.timeout_ms;
    const Bytes doc = abi::encode(m);
    const std::int64_t id = paglets_request(handle(), doc.data(), static_cast<std::uint32_t>(doc.size()));
    if (id < 0) return std::unexpected(static_cast<std::int32_t>(id));
    released(options.caps);
    const auto correlation = static_cast<std::uint64_t>(id);
    if (on_reply) pending_replies()[correlation] = std::move(on_reply);
    return correlation;
}

Result<abi::SelfInfo> self_info() {
    auto doc = read_document([](void* out, std::uint32_t cap) { return paglets_self_info(out, cap); });
    if (!doc) return std::unexpected(doc.error());
    abi::SelfInfo info;
    if (!abi::decode(*doc, info)) return std::unexpected(abi::malformed);
    return info;
}

Result<Endpoint> service(std::string_view name) {
    auto info = self_info();
    if (!info) return std::unexpected(info.error());
    for (const auto& [service_name, handle] : info->services) {
        if (service_name == name) return Endpoint(handle);
    }
    return std::unexpected(abi::not_found);
}

Result<std::vector<Capability>> list_caps() {
    auto doc = read_document([](void* out, std::uint32_t cap) { return paglets_cap_list(out, cap); });
    if (!doc) return std::unexpected(doc.error());
    std::vector<std::int32_t> handles;
    if (!abi::decode(*doc, handles)) return std::unexpected(abi::malformed);
    std::vector<Capability> caps;
    for (auto h : handles) caps.emplace_back(h);
    return caps;
}

// ---------------------------------------------------------------------------

Capability Message::take_cap(std::size_t index) {
    if (index >= d_.caps.size()) return Capability();
    return Capability(std::exchange(d_.caps[index], 0));
}

Result<void> Message::reply_raw(Bytes payload, std::vector<Capability> caps, bool async) {
    if (!is_request() || replied_ || deferred_) return std::unexpected(abi::bad_state);
    Capability reply(d_.reply);
    auto r = reply_to(reply, std::move(payload), std::move(caps), async);
    if (r) replied_ = true;
    return r;
}

Capability Message::defer_reply() {
    if (!is_request() || replied_ || deferred_) return Capability();
    deferred_ = true;
    return Capability(d_.reply);
}

Result<void> reply_to(Capability& reply, Bytes payload, std::vector<Capability> caps, bool async) {
    abi::OutMessage m;
    m.name = "reply";
    m.payload = std::move(payload);
    m.caps = handles_of(caps);
    m.async = async;
    const Bytes doc = abi::encode(m);
    const std::int32_t r = paglets_reply(reply.handle(), doc.data(), static_cast<std::uint32_t>(doc.size()));
    if (r == 0) {
        reply.release();
        released(caps);
    }
    return status(r);
}

void Paglet::on_undelivered(const abi::Undelivered& u) {
    log(abi::LogLevel::warning, "undelivered " + u.name + ": " + std::string(abi::error_name(u.status)));
}

std::int32_t Router::dispatch(Message& m) const {
    if (auto it = handlers_.find(m.name()); it != handlers_.end()) {
        return it->second(m);
    }
    if (fallback_) return fallback_(m);
    return abi::not_handled;
}

// ---------------------------------------------------------------------------

Result<Endpoint> create_child_raw(Bytes args, std::vector<Capability> caps, std::optional<std::string> module) {
    abi::ChildSpec spec;
    spec.module = std::move(module);
    spec.args = std::move(args);
    spec.caps = handles_of(caps);
    const Bytes doc = abi::encode(spec);
    const std::int32_t h = paglets_create_child(doc.data(), static_cast<std::uint32_t>(doc.size()));
    if (h < 0) return std::unexpected(h);
    released(caps);
    return Endpoint(h);
}

Result<void> dispose() {
    return status(paglets_lifecycle(static_cast<std::int32_t>(abi::LifecycleOp::dispose), nullptr, 0));
}

Result<void> deactivate(std::optional<std::int64_t> wake_after_ms) {
    const Bytes doc = abi::encode(abi::DeactivateArg{wake_after_ms});
    return status(paglets_lifecycle(static_cast<std::int32_t>(abi::LifecycleOp::deactivate), doc.data(),
                                    static_cast<std::uint32_t>(doc.size())));
}

Result<Endpoint> clone_raw(Bytes args, std::vector<Capability> caps) {
    abi::CloneArg arg;
    arg.args = std::move(args);
    arg.caps = handles_of(caps);
    const Bytes doc = abi::encode(arg);
    const std::int32_t h = paglets_lifecycle(static_cast<std::int32_t>(abi::LifecycleOp::clone), doc.data(),
                                             static_cast<std::uint32_t>(doc.size()));
    if (h < 0) return std::unexpected(h);
    released(caps);
    return Endpoint(h);
}

Result<void> dispatch(std::string destination) {
    const Bytes doc = abi::encode(abi::DispatchArg{std::move(destination)});
    return status(paglets_lifecycle(static_cast<std::int32_t>(abi::LifecycleOp::dispatch), doc.data(),
                                    static_cast<std::uint32_t>(doc.size())));
}

Result<Capability> after_raw(std::int64_t delay_ms, std::string_view name, Bytes payload, std::int32_t priority) {
    abi::OutMessage m;
    m.name = std::string(name);
    m.payload = std::move(payload);
    m.priority = priority;
    const Bytes doc = abi::encode(m);
    const std::int32_t h = paglets_timer_set(delay_ms, doc.data(), static_cast<std::uint32_t>(doc.size()));
    if (h < 0) return std::unexpected(h);
    return Capability(h);
}

}  // namespace paglets

// ---------------------------------------------------------------------------
// Exports

namespace {

std::span<const std::uint8_t> view(const std::uint8_t* ptr, std::uint32_t len) {
    return {ptr, len};
}

paglets::Started started_from(const paglets::abi::StartEvent& e) {
    paglets::Started s;
    s.args = e.args;
    s.original = e.original;
    for (auto h : e.caps) s.caps.emplace_back(h);
    return s;
}

}  // namespace

extern "C" {

PAGLETS_EXPORT(paglets_abi_v1) void paglets_abi_v1() {}

PAGLETS_EXPORT(paglets_alloc) void* paglets_alloc(std::uint32_t size) {
    return std::malloc(size == 0 ? 1 : size);
}

PAGLETS_EXPORT(paglets_free) void paglets_free(void* ptr) {
    std::free(ptr);
}

PAGLETS_EXPORT(paglets_on_event)
std::int32_t paglets_on_event(std::int32_t kind, const std::uint8_t* ptr, std::uint32_t len) {
    namespace abi = paglets::abi;
    paglets::Paglet& p = paglets::detail::paglet();
    switch (static_cast<abi::EventKind>(kind)) {
        case abi::EventKind::created:
        case abi::EventKind::cloned: {
            abi::StartEvent e;
            if (!abi::decode(view(ptr, len), e)) return abi::malformed;
            paglets::Started s = started_from(e);
            if (static_cast<abi::EventKind>(kind) == abi::EventKind::created) {
                p.on_created(s);
            } else {
                p.on_cloned(s);
            }
            return abi::handled;
        }
        case abi::EventKind::activated: p.on_activated(); return abi::handled;
        case abi::EventKind::deactivating: p.on_deactivating(); return abi::handled;
        case abi::EventKind::arrived: {
            abi::ArrivedEvent e;
            if (!abi::decode(view(ptr, len), e)) return abi::malformed;
            p.on_arrived(e);
            return abi::handled;
        }
        case abi::EventKind::dispatching: {
            abi::DispatchingEvent e;
            if (!abi::decode(view(ptr, len), e)) return abi::malformed;
            p.on_dispatching(e);
            return abi::handled;
        }
        case abi::EventKind::disposing: p.on_disposing(); return abi::handled;
    }
    return abi::not_handled;
}

PAGLETS_EXPORT(paglets_on_message)
std::int32_t paglets_on_message(const std::uint8_t* ptr, std::uint32_t len) {
    namespace abi = paglets::abi;
    abi::Delivered d;
    if (!abi::decode(view(ptr, len), d)) return abi::malformed;
    paglets::Message m(std::move(d));

    if (m.is_reply()) {
        auto& pending = paglets::pending_replies();
        if (auto it = pending.find(m.correlation()); it != pending.end()) {
            paglets::ReplyHandler handler = std::move(it->second);
            pending.erase(it);
            handler(m);
            return abi::handled;
        }
    }

    // Reports of asynchronous sends and replies come from the host (no sender).
    if (m.kind() == abi::MessageKind::message && m.name() == abi::undelivered_message && !m.sender()) {
        abi::Undelivered u;
        if (!m.decode(u)) return abi::malformed;
        paglets::detail::paglet().on_undelivered(u);
        return abi::handled;
    }

    const std::int32_t result = paglets::detail::paglet().on_message(m);
    // A handled request that was neither answered nor deferred is answered
    // `gone` by dropping its reply capability; for other results the host
    // answers with unknown_message or the error code.
    if (result == abi::handled && m.reply_pending()) {
        paglets_cap_drop(m.reply_handle());
    }
    return result;
}

}  // extern "C"
