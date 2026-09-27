// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `pubsub` system paglet (planning/cpp-system-paglets.md):
// topics reached only through `topic` capabilities. `create` replies with a
// capability (rights `publish` and `subscribe`) as the first capability of
// the reply; holders derive narrower ones (publish-only, subscribe-only) and
// pass them on. `publish`, `subscribe` and `unsubscribe` take a lent topic
// capability. Subscribers receive each publication as a message named as
// they chose, with the publication as payload and the topic name as badge.
// Topics are host-local in M2 (mesh-wide with WP14).

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::pubsub {

struct CreateRequest {
    std::string name;  // informational label
};

struct Topic {
    std::string id;  // assigned by the host
    std::string name;
};

struct PublishRequest {
    std::vector<std::uint8_t> payload;
    std::int32_t priority = 3;
};

struct PublishReply {
    std::uint32_t delivered = 0;
};

struct SubscribeRequest {
    std::string message = "pubsub.message";  // message name of deliveries
};

struct SubscribeReply {
    std::uint32_t subscribers = 0;
};

struct UnsubscribeRequest {};

struct UnsubscribeReply {
    bool existed = false;
};

struct Contract {
    static constexpr std::string_view service = "pubsub";
    Topic create(const CreateRequest&);
    PublishReply publish(const PublishRequest&);
    SubscribeReply subscribe(const SubscribeRequest&);
    UnsubscribeReply unsubscribe(const UnsubscribeRequest&);
};

}  // namespace paglets::services::pubsub
