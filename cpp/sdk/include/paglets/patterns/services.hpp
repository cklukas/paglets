// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18, planning/cpp-patterns.md): reaching the host's services.
//
// - endpoint(name, next): the endpoint the host gave every paglet, else one
//   from the directory (for services such as `files`, `artifacts`, `web`).
// - lending(cap): request options that lend a capability to a system paglet.
// - here(next): the host the paglet is on, as mesh-info describes it.
//
// Modules using it list the services `directory` and `mesh_info`
// (paglets_add_module(... PATTERNS) does).

#pragma once

#include <paglets/paglet.hpp>
#include <paglets/services/directory.gen.hpp>
#include <paglets/services/mesh_info.gen.hpp>

#include <functional>
#include <string>
#include <utility>

namespace paglets::patterns {

// An endpoint from the directory (the policy decides which operations it
// carries).
inline void lookup(std::string name, std::function<void(Result<Endpoint>)> next) {
    auto dir = service("directory");
    if (!dir) return next(std::unexpected(dir.error()));
    services::directory::Client client{*dir};
    auto sent = client.lookup(services::directory::LookupRequest{std::move(name)},
                              [next](Result<services::directory::LookupReply> r, Message& m) {
                                  if (!r) return next(std::unexpected(r.error()));
                                  if (m.cap_count() == 0) return next(std::unexpected(abi::internal));
                                  next(Endpoint(m.take_cap(0)));
                              });
    if (!sent) next(std::unexpected(sent.error()));
}

// The host's default endpoint for `name`, else one from the directory.
inline void endpoint(std::string name, std::function<void(Result<Endpoint>)> next) {
    if (auto ep = service(name)) return next(*ep);
    lookup(std::move(name), std::move(next));
}

inline RequestOptions lending(const Capability& cap, RequestOptions options = {}) {
    options.lend.push_back(cap);
    return options;
}

// The host this paglet is on now (key ID, name, labels, load, offers).
inline void here(std::function<void(Result<services::mesh_info::Snapshot>)> next) {
    auto mi = service("mesh-info");
    if (!mi) return next(std::unexpected(mi.error()));
    services::mesh_info::Client client{*mi};
    auto sent = client.snapshot({}, [next](Result<services::mesh_info::Snapshot> s, Message&) { next(std::move(s)); });
    if (!sent) next(std::unexpected(sent.error()));
}

// A short name for a host: its ledger name, else the start of its key ID.
inline std::string host_label(const services::mesh_info::Snapshot& s) {
    return s.host_name.empty() ? s.host.substr(0, 8) : s.host_name;
}

// "what: error_name".
inline std::string error_text(std::string_view what, std::int32_t code) {
    return std::string(what) + ": " + std::string(abi::error_name(code));
}

}  // namespace paglets::patterns
