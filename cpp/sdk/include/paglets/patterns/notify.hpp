// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): notifications to the paglet's owner through `user-info`.
// Fire and forget: a host without `user-info` (or a full queue) loses them,
// they never fail the paglet's work.

#pragma once

#include <paglets/patterns/services.hpp>
#include <paglets/services/user_info.gen.hpp>

#include <string>

namespace paglets::patterns {

using Level = services::user_info::Level;

inline void notify(Level level, std::string title, std::string text = {}) {
    if (title.size() > 200) title.resize(200);
    if (text.size() > 4000) text.resize(4000);
    endpoint("user-info", [level, title = std::move(title), text = std::move(text)](Result<Endpoint> ui) {
        if (!ui) return;
        services::user_info::Client client{*ui};
        (void)client.notify(services::user_info::NotifyRequest{title, text, level},
                            [](Result<services::user_info::NotifyReply>, Message&) {});
    });
}

}  // namespace paglets::patterns
