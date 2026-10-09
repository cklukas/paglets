// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Gateway system paglets (WP17, planning/cpp-web-ai.md): `web` (mediated
// web access) and `ai` (local inference). They run only where a host is
// configured for them, and announce themselves as service offers, so
// paglets find them with mesh-info and move there.

#pragma once

#include <paglets/abi.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/services/mesh_info.hpp>
#include <paglets/services/system_services.hpp>

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::gateway {

// Announces (an offer) or withdraws (nullopt) the offer of a service.
using OfferSink = std::function<void(const std::string& service, std::optional<services::mesh_info::Offer> offer)>;

// Decides whether `caller` may use `op` of `service` on `root` and `path`
// (for `web`: the URL's host and path; for `ai`: the model and ""). Absent:
// everything is allowed (a host without a mesh).
using Authorize = std::function<bool(const abi::SenderRecord& caller, std::string_view service, std::string_view op,
                                     std::string_view root, std::string_view path)>;

struct WebConfig {
    bool enabled = false;
    bool internet = true;               // public destinations
    std::vector<std::string> internal;  // URL prefixes of internal destinations allowed (admin choice)
    std::string proxy;                  // http://host:port; empty: direct
    std::string search_url;             // JSON search endpoint (SearXNG style), "{query}" is replaced
    std::string ca_file;                // extra CA certificates (PEM) for HTTPS destinations
    std::int64_t max_bytes = 4 * 1024 * 1024;
    std::int64_t max_download_bytes = 64 * 1024 * 1024;
    std::chrono::milliseconds timeout{20000};
    int max_redirects = 5;
    int max_parallel = 4;  // requests in flight; more wait
};

struct AiConfig {
    bool enabled = false;
    // Backends: "ollama" (url, models) and "test" (deterministic answers,
    // for tests and demonstrations without a model).
    std::string backend = "ollama";
    std::string url = "http://127.0.0.1:11434";
    std::vector<std::string> models;  // exposed models; empty: all the backend has
    std::chrono::milliseconds timeout{120000};
    std::chrono::milliseconds probe{30000};  // how often the backend is checked
    int max_parallel = 2;
    std::int64_t max_requests_per_owner = 600;  // per hour
    std::int64_t max_input_bytes = 256 * 1024;
};

struct GatewayConfig {
    WebConfig web;
    AiConfig ai;
    OfferSink offers;
    Authorize authorize;
};

class Gateway {
public:
    virtual ~Gateway() = default;
};

// Registers the configured gateway system paglets with `runtime` (artifacts
// they produce go to `services`).
std::expected<std::shared_ptr<Gateway>, std::string> install_gateway(runtime::Runtime& runtime,
                                                                     std::shared_ptr<services::SystemServices> services,
                                                                     GatewayConfig config);

// Destinations (exposed for tests): true if `address` (an IPv4 or IPv6
// literal) is loopback, private, link-local, shared, multicast or otherwise
// not a public internet address.
bool internal_address(std::string_view address);

// Readable text, title and links of an HTML page.
struct Extracted {
    std::string title;
    std::string text;
    std::vector<std::pair<std::string, std::string>> links;  // absolute URL, text
};
Extracted extract_html(std::string_view html, std::string_view base_url);

// `ref` relative to the absolute URL `base`; empty if not http(s).
std::string resolve_url(std::string_view base, std::string_view ref);

}  // namespace paglets::gateway
