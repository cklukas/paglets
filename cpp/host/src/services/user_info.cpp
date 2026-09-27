// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `user-info` system paglet: notifications from paglets to their owner.

#include "services.hpp"

namespace paglets::services::impl {

UserInfo::UserInfo(std::size_t per_owner, std::function<void(const Notification&)> sink)
    : per_owner_(std::max<std::size_t>(per_owner, 1)), sink_(std::move(sink)) {}

Result<user_info::NotifyReply> UserInfo::notify(const user_info::NotifyRequest& q, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    if (q.title.empty() || q.title.size() > 200 || q.text.size() > 4000) return std::unexpected(abi::invalid_argument);
    Notification n;
    {
        std::lock_guard lock(mu_);
        n = Notification{next_id_++, caller->owner, caller->id, q.title, q.text, q.level, now_ms()};
        auto& list = by_owner_[caller->owner];
        list.push_back(n);
        while (list.size() > per_owner_) list.pop_front();
    }
    const auto level = q.level == user_info::Level::error     ? abi::LogLevel::error
                       : q.level == user_info::Level::warning ? abi::LogLevel::warning
                                                              : abi::LogLevel::info;
    op.ctx.log(level, "notification for " + n.owner + ": " + n.title);
    if (sink_) sink_(n);
    return user_info::NotifyReply{n.id};
}

std::vector<Notification> UserInfo::notifications(std::string_view owner) const {
    std::lock_guard lock(mu_);
    auto it = by_owner_.find(owner);
    if (it == by_owner_.end()) return {};
    return {it->second.begin(), it->second.end()};
}

}  // namespace paglets::services::impl
