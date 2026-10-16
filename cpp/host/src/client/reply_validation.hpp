// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Validate known session replies before any consumer formats their fields or
// changes its ledger. Optional extension fields may be absent on older hosts;
// when present they must have the expected type. Unknown fields stay allowed.
#pragma once

#include <paglets/mesh/value.hpp>
#include <paglets/wire/interface.hpp>

#include <algorithm>
#include <initializer_list>
#include <limits>
#include <string_view>

namespace paglets::client::detail {
using mesh::Array;
using mesh::Fields;
using mesh::Map;
using mesh::Value;
using Check = bool (*)(const Value&);

inline bool string(const Value& v) {
    return v.as_str() != nullptr;
}
inline bool integer(const Value& v) {
    return v.as_int() != nullptr;
}
inline bool boolean(const Value& v) {
    return v.as_bool() != nullptr;
}
inline bool binary(const Value& v) {
    return v.as_bin() != nullptr;
}
inline bool key(const Value& v) {
    return v.as_bin() && v.as_bin()->size() == 32;
}
inline bool description(const Value& v) {
    const auto* text = v.as_str();
    return text && (text->empty() || wire::interface::parse(*text).has_value());
}

inline bool fields(const Fields& f, std::initializer_list<std::string_view> names, Check check, bool required = true) {
    for (const auto name : names) {
        const auto* v = f.get(name);
        if (v ? !check(*v) : required) return false;
    }
    return true;
}

template <class F>
bool array(const Fields& f, std::string_view name, F check, bool required = true) {
    const auto* v = f.get(name);
    if (!v) return !required;
    const auto* values = v->as_array();
    return values && std::all_of(values->begin(), values->end(), check);
}

inline bool paglet_row(const Value& v) {
    const auto* a = v.as_array();
    return a && a->size() >= 4 && std::all_of(a->begin(), a->begin() + 4, string) && (a->size() < 5 || string((*a)[4]));
}

inline bool host_row(const Value& v) {
    if (!v.as_map()) return false;
    const Fields f{*v.as_map()};
    return fields(f, {"key"}, key) && fields(f, {"name", "url", "abi", "via", "send_error"}, string, false) &&
           fields(f, {"self", "online", "compatible"}, boolean, false) &&
           fields(f, {"seen", "proto"}, integer, false) && array(f, "labels", string, false) &&
           array(f, "relays", key, false);
}

inline bool pin_row(const Value& v) {
    const auto* a = v.as_array();
    return a && a->size() == 5 && string((*a)[0]) && string((*a)[1]) && integer((*a)[2]) && string((*a)[3]) &&
           string((*a)[4]);
}

inline bool module_row(const Value& v) {
    if (!v.as_map()) return false;
    const Fields f{*v.as_map()};
    return fields(f, {"hash"}, string) && fields(f, {"size", "users"}, integer) &&
           fields(f, {"pinned"}, boolean, false) && fields(f, {"interface"}, description, false) &&
           array(f, "names", string, false);
}

inline bool job_row(const Value& v) {
    if (!v.as_map()) return false;
    const Fields f{*v.as_map()};
    return fields(f, {"id", "cores", "elapsed_ms"}, integer) && fields(f, {"paglet", "function", "state"}, string) &&
           fields(f, {"requester", "owner", "module"}, string, false) &&
           fields(f, {"done", "total", "queued_ms"}, integer, false) && fields(f, {"aot"}, boolean, false);
}

inline bool reply_shape(std::string_view type, std::string_view op, const Map& m) {
    const Fields f{m};
    if (type == "locate" || type == "pin" || type == "unpin") {
        if (!fields(f, {"host"}, key) || !fields(f, {"host_name"}, string, false) ||
            !fields(f, {"moves", "moved"}, integer, false))
            return false;
        if (type == "pin") return fields(f, {"pin"}, string) && fields(f, {"until"}, integer);
        if (type == "unpin") return fields(f, {"released"}, integer);
        return true;
    }
    if (type == "status") {
        return fields(f, {"key"}, key) && fields(f, {"name", "mesh"}, string) && fields(f, {"records"}, integer) &&
               array(f, "paglets", paglet_row) && fields(f, {"digest"}, key, false) &&
               array(f, "system", string, false) &&
               fields(f, {"version", "os", "os_version", "architecture", "cpu_model"}, string, false) &&
               fields(f, {"uptime_s", "system_uptime_s", "cpus", "memory_total"}, integer, false);
    }
    if (type == "hosts") return array(f, "hosts", host_row);
    if (type == "moves") {
        return fields(f, {"moves_out", "moves_in", "moves_failed"}, integer) &&
               fields(f,
                      {"pages_sent", "pages_received", "pages_reused", "bytes_sent", "image_pages", "capture_ns",
                       "ready_ns", "prepare_ns", "commit_ns", "destination_activation_ns", "arrival_acks",
                       "arrival_failures", "arrival_ack_ns", "cached_page_bytes", "memory_limit_pages"},
                      integer, false);
    }
    if (type == "pins") return array(f, "pins", pin_row);
    if (type == "push") return fields(f, {"added"}, integer) && array(f, "errors", string, false);
    if (type == "records") return array(f, "records", binary);
    if (type == "modules") return array(f, "modules", module_row);
    if (type == "launch") return fields(f, {"paglet"}, string);
    if (type == "push-module") return fields(f, {"hash"}, string);
    if (type == "describe") return fields(f, {"interface"}, description);
    if (type == "call") {
        const auto status = f.integer("status");
        return status && *status >= std::numeric_limits<std::int32_t>::min() &&
               *status <= std::numeric_limits<std::int32_t>::max() && fields(f, {"payload"}, binary);
    }
    if (type == "dispatch") {
        if (!fields(f, {"moved", "pending"}, boolean, false) || !fields(f, {"host"}, key, false) ||
            !fields(f, {"host_name", "reason"}, string, false))
            return false;
        const auto* pending = f.get("pending");
        const auto* moved = f.get("moved");
        if (pending && *pending->as_bool()) return !moved && !f.get("host");
        if (moved) return *moved->as_bool() ? fields(f, {"host"}, key) : fields(f, {"reason"}, string);
        // Older hosts acknowledge only that dispatch started.
        return !pending && !f.get("host");
    }
    if (type == "jobs") {
        if (op == "cancel") return fields(f, {"cancelled"}, boolean);
        return fields(f, {"slots", "running"}, integer) && fields(f, {"aot"}, boolean, false) &&
               array(f, "jobs", job_row);
    }
    if (type == "landscape") return array(f, "hosts", binary);
    if (type == "slots") return fields(f, {"status"}, binary);
    if (type == "zombies") {
        if (op == "list") return array(f, "groups", binary);
        if (op == "traces") return array(f, "records", binary) && fields(f, {"state"}, binary, false);
        if (op == "result") return fields(f, {"result"}, binary);
        if (op == "cancel" || op == "redirect") return fields(f, {"state"}, binary);
        if (op == "check") {
            return fields(f, {"records", "replicas", "hosts", "takeovers", "restarts", "accepted"}, integer) &&
                   array(f, "violations", string);
        }
    }
    // Raw/extension requests have no typed decoder here. Their caller owns
    // the schema; the common envelope is still checked.
    return true;
}
}  // namespace paglets::client::detail
