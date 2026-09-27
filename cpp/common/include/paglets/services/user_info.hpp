// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `user-info` system paglet (planning/cpp-system-paglets.md):
// notifications from a paglet to its owner. The host keeps them for the
// owner (a bounded list per owner) and passes them to its notification
// sink (log, CLI, and later the owner's devices).

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace paglets::services::user_info {

enum class Level { info, success, warning, error };

struct NotifyRequest {
    std::string title;  // at most 200 characters
    std::string text;   // at most 4000 characters
    Level level = Level::info;
};

struct NotifyReply {
    std::uint64_t id = 0;  // number of the notification on this host
};

struct Contract {
    static constexpr std::string_view service = "user-info";
    NotifyReply notify(const NotifyRequest&);
};

}  // namespace paglets::services::user_info
