// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <paglets/mesh/ledger.hpp>

#include <expected>
#include <optional>
#include <string_view>

namespace paglets::host_detail {

enum class HostAddressError { invalid, missing, ambiguous };

// Resolve before filtering for reachability, policy or capacity: those filters
// must not change which identity a singleton address denotes. A canonical key
// is authoritative even when absent from the ledger; never reinterpret it as
// somebody else's display name. Names and optional short IDs must be unique
// across both namespaces, including the local host.
inline std::expected<mesh::PublicKey, HostAddressError> resolve_host_address(
    const mesh::LedgerState& ledger, std::string_view address, bool allow_prefix) {
    if (address.empty()) return std::unexpected(HostAddressError::invalid);
    if (auto key = mesh::parse_key_id(address)) {
        if (ledger.is_host(*key)) return *key;
        return std::unexpected(HostAddressError::missing);
    }
    std::optional<mesh::PublicKey> found;
    for (const auto& [key, host] : ledger.hosts) {
        if (host.name != address &&
            !(allow_prefix && address.size() >= 8 && mesh::key_id(key).starts_with(address))) continue;
        if (found) return std::unexpected(HostAddressError::ambiguous);
        found = key;
    }
    if (!found) return std::unexpected(HostAddressError::missing);
    return *found;
}

}  // namespace paglets::host_detail
