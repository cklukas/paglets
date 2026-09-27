// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `pubsub` system paglet: topics reached through `topic` capabilities.
// Topics and subscriptions are kept in the service state file, so inactive
// subscribers keep receiving publications (they are activated by them) and
// survive host restarts; subscriptions end with their paglet.

#include "services.hpp"

#include <paglets/wire/reflect.hpp>

namespace paglets::services::impl {

namespace {

struct StoredSubscriber {
    std::string paglet;
    std::string message;
};

struct StoredTopic {
    std::string id;
    std::string name;
    std::vector<StoredSubscriber> subscribers;
};

struct StoredTopics {
    std::vector<StoredTopic> topics;
};

}  // namespace

PubSub::PubSub(fs::path state_file) : state_file_(std::move(state_file)) {
    load();
}

void PubSub::load() {
    if (state_file_.empty()) return;
    std::error_code ec;
    if (!fs::exists(state_file_, ec)) return;
    auto bytes = read_all(state_file_);
    StoredTopics stored;
    if (!bytes || !wire::from_msgpack(*bytes, stored)) return;
    for (auto& t : stored.topics) {
        Topic topic{std::move(t.name), {}};
        for (auto& s : t.subscribers) topic.subscribers[s.paglet] = std::move(s.message);
        topics_[t.id] = std::move(topic);
    }
}

void PubSub::save() {
    if (state_file_.empty()) return;
    StoredTopics stored;
    for (const auto& [id, t] : topics_) {
        StoredTopic st{id, t.name, {}};
        for (const auto& [paglet, message] : t.subscribers) st.subscribers.push_back({paglet, message});
        stored.topics.push_back(std::move(st));
    }
    std::error_code ec;
    fs::create_directories(state_file_.parent_path(), ec);
    (void)write_atomically(state_file_, wire::to_msgpack(stored));
}

void PubSub::paglet_ended(runtime::SystemContext&, const runtime::PagletId& id) {
    std::lock_guard lock(mu_);
    bool changed = false;
    for (auto& [topic_id, t] : topics_) changed = t.subscribers.erase(id) > 0 || changed;
    if (changed) save();
}

Result<std::string> PubSub::lent_topic(Operation& op, std::string_view right) const {
    if (op.lent().empty()) return std::unexpected(abi::invalid_argument);
    const runtime::Cap& cap = op.lent().front();
    if (auto c = op.ctx.check(cap, "topic", right); c != abi::ok) return std::unexpected(c);
    return cap.resource;
}

Result<pubsub::Topic> PubSub::create(const pubsub::CreateRequest& q, Operation& op) {
    if (!caller_of(op.call)) return std::unexpected(abi::denied);
    if (q.name.size() > 128) return std::unexpected(abi::invalid_argument);
    const std::string id = random_hex(16);
    {
        std::lock_guard lock(mu_);
        topics_[id] = Topic{q.name, {}};
        save();
    }
    op.give.push_back(op.ctx.resource("topic", id, {"publish", "subscribe"}));
    return pubsub::Topic{id, q.name};
}

Result<pubsub::PublishReply> PubSub::publish(const pubsub::PublishRequest& q, Operation& op) {
    auto id = lent_topic(op, "publish");
    if (!id) return std::unexpected(id.error());
    if (q.priority < 0 || q.priority > abi::max_priority) return std::unexpected(abi::invalid_argument);
    std::vector<std::pair<std::string, std::string>> targets;
    std::string name;
    {
        std::lock_guard lock(mu_);
        auto it = topics_.find(*id);
        if (it == topics_.end()) return std::unexpected(abi::not_found);
        name = it->second.name;
        targets.assign(it->second.subscribers.begin(), it->second.subscribers.end());
    }
    pubsub::PublishReply reply;
    std::vector<std::string> gone;
    for (const auto& [paglet, message] : targets) {
        auto sent = op.ctx.send(paglet, message, q.payload, {}, q.priority, name);
        if (sent) {
            ++reply.delivered;
        } else if (sent.error() == abi::not_found) {
            gone.push_back(paglet);  // ended while the host was down
        }
    }
    if (!gone.empty()) {
        std::lock_guard lock(mu_);
        if (auto it = topics_.find(*id); it != topics_.end()) {
            for (const auto& paglet : gone) it->second.subscribers.erase(paglet);
            save();
        }
    }
    return reply;
}

Result<pubsub::SubscribeReply> PubSub::subscribe(const pubsub::SubscribeRequest& q, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    auto id = lent_topic(op, "subscribe");
    if (!id) return std::unexpected(id.error());
    if (!abi::valid_message_name(q.message)) return std::unexpected(abi::invalid_argument);
    std::lock_guard lock(mu_);
    auto it = topics_.find(*id);
    if (it == topics_.end()) return std::unexpected(abi::not_found);
    it->second.subscribers[caller->id] = q.message;
    save();
    return pubsub::SubscribeReply{static_cast<std::uint32_t>(it->second.subscribers.size())};
}

Result<pubsub::UnsubscribeReply> PubSub::unsubscribe(const pubsub::UnsubscribeRequest&, Operation& op) {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    auto id = lent_topic(op, "subscribe");
    if (!id) return std::unexpected(id.error());
    std::lock_guard lock(mu_);
    auto it = topics_.find(*id);
    if (it == topics_.end()) return std::unexpected(abi::not_found);
    const bool existed = it->second.subscribers.erase(caller->id) > 0;
    if (existed) save();
    return pubsub::UnsubscribeReply{existed};
}

}  // namespace paglets::services::impl
