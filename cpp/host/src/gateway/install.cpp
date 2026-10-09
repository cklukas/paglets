// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Registers the configured gateway system paglets and announces them.

#include "gateway_impl.hpp"

namespace paglets::gateway {

namespace {

class Installed final : public Gateway {
public:
    std::shared_ptr<Web> web;
    std::shared_ptr<Ai> ai;
};

}  // namespace

std::expected<std::shared_ptr<Gateway>, std::string> install_gateway(runtime::Runtime& runtime,
                                                                     std::shared_ptr<services::SystemServices> services,
                                                                     GatewayConfig config) {
    auto installed = std::make_shared<Installed>();
    if (config.web.enabled) {
        for (const auto& prefix : config.web.internal) {
            if (!parse_url(prefix)) return std::unexpected("invalid internal URL prefix " + prefix);
        }
        if (!config.web.proxy.empty() && !parse_url(config.web.proxy))
            return std::unexpected("invalid proxy " + config.web.proxy);
        if (!config.web.ca_file.empty()) {
            if (auto problem = ca_file_problem(config.web.ca_file)) return std::unexpected(*problem);
        }
        installed->web = std::make_shared<Web>(config.web, services, config.authorize);
        if (auto id = runtime.add_system_paglet(installed->web); !id) return std::unexpected(id.error());
        if (config.offers) config.offers("web", installed->web->offer());
    }
    if (config.ai.enabled) {
        if (config.ai.backend != "ollama" && config.ai.backend != "test")
            return std::unexpected("unknown AI backend " + config.ai.backend);
        installed->ai = std::make_shared<Ai>(config.ai, config.authorize, config.offers);
        if (auto id = runtime.add_system_paglet(installed->ai); !id) return std::unexpected(id.error());
    }
    return installed;
}

}  // namespace paglets::gateway
