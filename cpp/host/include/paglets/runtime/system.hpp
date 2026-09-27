// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Native system paglets (planning/cpp-system-paglets.md): C++ objects in the
// host process that are paglets for everybody else. They have a paglet ID
// (`system.<name>`), a mailbox and endpoints like any paglet, and handle one
// message at a time on the scheduler lanes; they never move, deactivate or
// end.
//
// A system paglet sees the capabilities a caller lends it for one message
// (`lend` in the outgoing message) and those transferred to it, replies
// with data and capabilities, mints resource capabilities it provides
// (`dir`, `file`, `artifact`, `topic`), and sends messages to paglets.

#pragma once

#include <paglets/abi.hpp>
#include <paglets/runtime/capability.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::runtime {

class Runtime;
using PagletId = std::string;
using Bytes = std::vector<std::uint8_t>;

// One message delivered to a system paglet.
class ServiceCall {
public:
    virtual ~ServiceCall() = default;

    virtual abi::MessageKind kind() const = 0;
    bool is_request() const { return kind() == abi::MessageKind::request; }
    virtual const std::string& name() const = 0;
    virtual std::span<const std::uint8_t> payload() const = 0;
    // Host-stamped sender record; absent for timers and replies.
    virtual const std::optional<abi::SenderRecord>& sender() const = 0;
    virtual const std::optional<std::string>& badge() const = 0;

    // Capabilities the sender lent for this message (copies; the sender
    // keeps them). Checked for revocation and expiry on delivery.
    virtual const std::vector<Cap>& lent() const = 0;
    // Capabilities transferred to the system paglet; it may keep them.
    virtual std::vector<Cap>& transferred() = 0;

    // Answers a request once; `status` 0 or an abi::Error.
    virtual std::expected<void, std::int32_t> reply(std::int32_t status, Bytes payload = {},
                                                    std::vector<Cap> caps = {}) = 0;
    virtual bool replied() const = 0;
    // Keeps the reply capability for an answer later (SystemContext::reply).
    virtual std::optional<Cap> defer() = 0;
};

// What a system paglet can do in the host. Thread-safe.
class SystemContext {
public:
    virtual ~SystemContext() = default;

    virtual Runtime& runtime() = 0;
    virtual const PagletId& id() const = 0;
    virtual std::string_view host_name() const = 0;

    // A resource capability provided by this system paglet.
    virtual Cap resource(std::string type, std::string resource, std::vector<std::string> rights,
                         std::optional<std::string> grant = std::nullopt,
                         std::optional<std::int64_t> expires = std::nullopt) = 0;
    // An endpoint capability to a paglet (or another system paglet).
    virtual Cap endpoint(const PagletId& target, std::vector<std::string> ops) = 0;

    // abi::ok when `cap` is usable (not revoked, not expired), a resource of
    // this system paglet with `type` and, unless `right` is empty, `right`;
    // otherwise the error to answer with.
    virtual std::int32_t check(const Cap& cap, std::string_view type, std::string_view right) const = 0;

    // A message from this system paglet to a paglet. Fails with an
    // abi::Error (not_found, quota, invalid_argument).
    virtual std::expected<void, std::int32_t> send(const PagletId& to, std::string_view name, Bytes payload,
                                                   std::vector<Cap> caps = {},
                                                   std::int32_t priority = abi::default_priority,
                                                   std::optional<std::string> badge = std::nullopt) = 0;
    // Answers a deferred request.
    virtual std::expected<void, std::int32_t> reply(Cap reply, std::int32_t status, Bytes payload = {},
                                                    std::vector<Cap> caps = {}) = 0;

    virtual void log(abi::LogLevel level, std::string_view text) = 0;
};

class SystemPaglet {
public:
    virtual ~SystemPaglet() = default;

    // Service name; the paglet ID is `system.<name>`.
    virtual std::string_view name() const = 0;
    // Operations of the endpoint the host installs in every new roaming and
    // resident paglet (planning/cpp-security-and-communication.md, 7.5);
    // empty: none.
    virtual std::vector<std::string> default_ops() const { return {}; }
    // Every operation it offers (for the directory and policies).
    virtual std::vector<std::string> operations() const { return {}; }

    // Called once when the runtime registers it.
    virtual void start(SystemContext&) {}
    // One message at a time, on a scheduler lane.
    virtual void handle(SystemContext& ctx, ServiceCall& call) = 0;
    // A paglet ended (disposed or failed), in order with messages.
    virtual void paglet_ended(SystemContext&, const PagletId&) {}
};

// ID of the system paglet with service name `name`.
inline PagletId system_paglet_id(std::string_view name) {
    return "system." + std::string(name);
}

}  // namespace paglets::runtime
