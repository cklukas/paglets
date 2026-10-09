// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `web` system paglet (planning/cpp-web-ai.md, section 2): GET and HEAD
// of http(s) URLs on behalf of paglets, with the destination checked at
// every hop (internal addresses refused unless allowed), the mesh policy
// asked per URL, and the bodies bounded. Requests run on job threads; the
// paglet's request is answered when done.

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>

#include "gateway_impl.hpp"

#include <paglets/runtime/runtime.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/json.hpp>
#include <paglets/wire/reflect.hpp>

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <algorithm>
#include <cctype>
#include <fstream>

namespace paglets::gateway {

namespace web = services::web;

// -- CA certificates ---------------------------------------------------------------------------

std::optional<std::string> ca_file_problem(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return "cannot read the web CA file " + path;
    const std::string pem(std::istreambuf_iterator<char>(in), {});
    BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    X509* x = bio == nullptr ? nullptr : PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    ERR_clear_error();
    if (x == nullptr) return "the web CA file " + path + " holds no PEM certificate";
    X509_free(x);
    return std::nullopt;
}

// -- jobs ------------------------------------------------------------------------------------

void Jobs::post(std::function<void(bool)> job) {
    std::lock_guard lock(mu_);
    if (stopping_) {
        // After stop: the job learns at once.
        job(true);
        return;
    }
    queue_.push_back(std::move(job));
    if (busy_ + static_cast<int>(queue_.size()) > static_cast<int>(workers_.size()) &&
        static_cast<int>(workers_.size()) < threads_) {
        workers_.emplace_back([this] { run(); });
    }
    cv_.notify_one();
}

void Jobs::run() {
    std::unique_lock lock(mu_);
    while (true) {
        cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
        if (queue_.empty()) return;
        auto job = std::move(queue_.front());
        queue_.pop_front();
        ++busy_;
        const bool stopping = stopping_;
        lock.unlock();
        job(stopping);
        lock.lock();
        --busy_;
    }
}

void Jobs::stop() {
    std::vector<std::thread> workers;
    {
        std::lock_guard lock(mu_);
        if (stopping_ && workers_.empty()) return;
        stopping_ = true;
        workers.swap(workers_);
    }
    cv_.notify_all();
    for (auto& t : workers) t.join();
    // Jobs nobody ran: they learn that the host stops.
    std::deque<std::function<void(bool)>> left;
    {
        std::lock_guard lock(mu_);
        left.swap(queue_);
    }
    for (auto& job : left) job(true);
}

std::size_t Jobs::pending() const {
    std::lock_guard lock(mu_);
    return queue_.size() + static_cast<std::size_t>(busy_);
}

// -- HTTP ------------------------------------------------------------------------------------

namespace {

std::string lower(std::string_view s) {
    std::string out(s);
    std::ranges::transform(out, out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

// Every address the host name resolves to (a literal: itself).
std::vector<std::string> resolve(const std::string& host, int port) {
    std::vector<std::string> out;
    std::string name = host;
    if (name.size() >= 2 && name.front() == '[') name = name.substr(1, name.size() - 2);
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    if (getaddrinfo(name.c_str(), std::to_string(port).c_str(), &hints, &result) != 0) return out;
    for (addrinfo* a = result; a != nullptr; a = a->ai_next) {
        char text[INET6_ADDRSTRLEN] = {};
        const void* raw = a->ai_family == AF_INET
                              ? static_cast<const void*>(&reinterpret_cast<const sockaddr_in*>(a->ai_addr)->sin_addr)
                              : static_cast<const void*>(&reinterpret_cast<const sockaddr_in6*>(a->ai_addr)->sin6_addr);
        if (inet_ntop(a->ai_family, raw, text, sizeof(text)) != nullptr) {
            if (std::ranges::find(out, std::string(text)) == out.end()) out.emplace_back(text);
        }
    }
    freeaddrinfo(result);
    return out;
}

}  // namespace

HttpResult http(const std::string& url_text, const HttpOptions& options, const HttpSettings& settings) {
    HttpResult out;
    std::string current = url_text;
    for (int hop = 0;; ++hop) {
        auto u = parse_url(current);
        if (!u) {
            out.error = "invalid URL " + current;
            out.code = abi::invalid_argument;
            return out;
        }
        out.url = u->text();
        // The destination: every address the name resolves to must be
        // allowed; the connection goes to the one checked (no second lookup
        // an attacker could answer differently).
        std::string address;
        if (options.check_destinations) {
            const bool allowed_internal = std::ranges::any_of(
                settings.internal, [&](const std::string& prefix) { return out.url.starts_with(prefix); });
            const auto addresses = resolve(u->host, u->port);
            if (addresses.empty() && settings.proxy.empty()) {
                out.error = "cannot resolve " + u->host;
                out.code = abi::not_found;
                return out;
            }
            for (const auto& a : addresses) {
                const bool internal = internal_address(a);
                if ((internal && !allowed_internal) || (!internal && !settings.internet && !allowed_internal)) {
                    out.error = internal ? "refused: " + u->host + " is an internal address (" + a + ")"
                                         : "refused: this host does not reach the internet";
                    out.code = abi::denied;
                    return out;
                }
            }
            if (!addresses.empty()) address = addresses.front();
        }
        httplib::Client client(u->origin());
        client.set_connection_timeout(std::chrono::duration_cast<std::chrono::seconds>(settings.timeout));
        client.set_read_timeout(std::chrono::duration_cast<std::chrono::seconds>(settings.timeout));
        client.set_write_timeout(std::chrono::duration_cast<std::chrono::seconds>(settings.timeout));
        client.set_follow_location(false);
        client.set_keep_alive(false);
        if (!address.empty() && settings.proxy.empty()) {
            std::string host = u->host;
            if (host.size() >= 2 && host.front() == '[') host = host.substr(1, host.size() - 2);
            client.set_hostname_addr_map({{host, address}});
        }
        if (!settings.proxy.empty()) {
            if (auto p = parse_url(settings.proxy)) {
                std::string phost = p->host;
                if (phost.size() >= 2 && phost.front() == '[') phost = phost.substr(1, phost.size() - 2);
                client.set_proxy(phost, p->port);
            }
        }
        if (u->scheme == "https") {
            client.enable_server_certificate_verification(true);
            if (!settings.ca_file.empty()) {
                // Extra CAs (an internal CA): the system's CAs stay trusted.
                client.set_ca_cert_path(settings.ca_file);
                client.enable_system_ca(true);
            }
        }
        httplib::Headers headers{{"User-Agent", "paglets-web/1"}, {"Accept-Encoding", "identity"}};
        int status = 0;
        std::vector<std::pair<std::string, std::string>> got_headers;
        std::string body;
        bool too_large = false;
        auto on_response = [&](const httplib::Response& r) {
            status = r.status;
            got_headers.clear();
            for (const auto& [k, v] : r.headers) got_headers.emplace_back(k, v);
            return true;
        };
        auto on_content = [&](const char* data, std::size_t length) {
            const auto room = static_cast<std::size_t>(std::max<std::int64_t>(0, options.max_bytes)) - body.size();
            if (length > room) {
                body.append(data, room);
                too_large = true;
                return false;
            }
            body.append(data, length);
            return true;
        };
        httplib::Result r;
        if (options.method == "HEAD") {
            r = client.Head(u->target, headers);
            if (r) on_response(*r);
        } else if (options.method == "POST") {
            r = client.Post(u->target, headers, options.body, options.content_type);
            if (r) {
                on_response(*r);
                body = r->body.substr(0, static_cast<std::size_t>(std::max<std::int64_t>(0, options.max_bytes)));
                too_large = r->body.size() > body.size();
            }
        } else {
            r = client.Get(u->target, headers, on_response, on_content);
        }
        if (!r && !(too_large && status != 0)) {
            out.error = "request to " + u->host + " failed: " + httplib::to_string(r.error());
            if (r.error() == httplib::Error::SSLServerVerification && r.ssl_backend_error() != 0 &&
                r.ssl_backend_error() < 1000) {
                out.error +=
                    std::string(" (") + X509_verify_cert_error_string(static_cast<long>(r.ssl_backend_error())) + ")";
            }
            out.code = abi::failed;
            return out;
        }
        // Redirects: each hop is checked like the first.
        if (status >= 300 && status < 400 && status != 304 && options.method != "POST") {
            std::string location;
            for (const auto& [k, v] : got_headers) {
                if (lower(k) == "location") location = v;
            }
            if (!location.empty()) {
                if (hop >= options.max_redirects) {
                    out.error = "too many redirects";
                    out.code = abi::failed;
                    return out;
                }
                const std::string next = resolve_url(out.url, location);
                if (next.empty()) {
                    out.error = "redirect to an invalid URL";
                    out.code = abi::invalid_argument;
                    return out;
                }
                current = next;
                continue;
            }
        }
        out.status = status;
        out.headers = std::move(got_headers);
        for (const auto& [k, v] : out.headers) {
            if (lower(k) == "content-type") out.content_type = v;
        }
        out.body = std::move(body);
        out.truncated = too_large;
        if (too_large && options.fail_when_larger) {
            out.error = "the body is larger than " + std::to_string(options.max_bytes) + " bytes";
            out.code = abi::too_large;
        }
        return out;
    }
}

// -- the `web` system paglet -------------------------------------------------------------------

Web::Web(WebConfig config, std::shared_ptr<services::SystemServices> services, Authorize authorize)
    : config_(std::move(config)), services_(std::move(services)), authorize_(std::move(authorize)),
      jobs_(config_.max_parallel) {}

void Web::start(runtime::SystemContext& ctx) {
    ctx_ = &ctx;
}

void Web::stop() {
    jobs_.stop();
}

HttpSettings Web::settings() const {
    return HttpSettings{config_.internet, config_.internal, config_.proxy, config_.ca_file, config_.timeout};
}

services::mesh_info::Offer Web::offer() const {
    services::mesh_info::Offer o;
    o.service = "web";
    o.ops = {"fetch", "extract_text", "download"};
    if (!config_.search_url.empty()) o.ops.emplace_back("search");
    using A = services::mesh_info::Attribute;
    o.attributes.push_back(A{"internet", config_.internet ? "yes" : "no", 0, false});
    std::string internal;
    for (const auto& p : config_.internal) internal += (internal.empty() ? "" : ",") + p;
    if (!internal.empty()) o.attributes.push_back(A{"internal", internal, 0, false});
    o.attributes.push_back(A{"max_bytes", {}, static_cast<double>(config_.max_bytes), true});
    o.attributes.push_back(A{"max_download_bytes", {}, static_cast<double>(config_.max_download_bytes), true});
    o.attributes.push_back(A{"queue", {}, static_cast<double>(jobs_.pending()), true});
    return o;
}

std::int32_t Web::admit(services::Operation& op, std::string_view op_name, const std::string& url, Url& parsed) const {
    if (!op.sender() || op.sender()->id == "host") return abi::denied;
    auto u = parse_url(url);
    if (!u) return abi::invalid_argument;
    parsed = *u;
    if (authorize_) {
        std::string path = u->path.substr(u->path.starts_with('/') ? 1 : 0);
        if (!authorize_(*op.sender(), "web", op_name, u->host, path)) return abi::denied;
    }
    return abi::ok;
}

void Web::later(services::Operation& op, std::function<void(runtime::SystemContext&, runtime::Cap)> work) {
    auto reply = op.call.defer();
    if (!reply) return;
    runtime::SystemContext* ctx = ctx_;
    jobs_.post([ctx, cap = std::move(*reply), work = std::move(work)](bool stopping) mutable {
        if (stopping) {
            (void)ctx->reply(std::move(cap), abi::failed);
            return;
        }
        work(*ctx, std::move(cap));
    });
}

namespace {

void answer_error(runtime::SystemContext& ctx, runtime::Cap cap, const HttpResult& r) {
    ctx.log(abi::LogLevel::info, "web: " + r.error);
    (void)ctx.reply(std::move(cap), r.code == 0 ? abi::failed : r.code, wire::to_msgpack(r.error));
}

}  // namespace

services::Result<web::Capabilities> Web::capabilities(const web::CapabilitiesRequest&, services::Operation&) {
    return web::Capabilities{config_.internet,        config_.internal,           !config_.search_url.empty(),
                             config_.max_bytes,       config_.max_download_bytes, !config_.proxy.empty()};
}

services::Result<web::FetchReply> Web::fetch(const web::FetchRequest& q, services::Operation& op) {
    Url u;
    if (q.method != "GET" && q.method != "HEAD") return std::unexpected(abi::invalid_argument);
    if (auto c = admit(op, "fetch", q.url, u); c != abi::ok) return std::unexpected(c);
    HttpOptions o;
    o.method = q.method;
    o.max_bytes = q.max_bytes > 0 ? std::min(q.max_bytes, config_.max_bytes) : config_.max_bytes;
    o.max_redirects = config_.max_redirects;
    later(op, [o, url = u.text(), s = settings()](runtime::SystemContext& ctx, runtime::Cap cap) {
        const HttpResult r = http(url, o, s);
        if (!r.error.empty()) return answer_error(ctx, std::move(cap), r);
        web::FetchReply reply;
        reply.status = r.status;
        reply.url = r.url;
        reply.content_type = r.content_type;
        for (const auto& [k, v] : r.headers) reply.headers.push_back(web::Header{k, v});
        reply.body.assign(r.body.begin(), r.body.end());
        reply.truncated = r.truncated;
        (void)ctx.reply(std::move(cap), abi::ok, wire::to_msgpack(reply));
    });
    return web::FetchReply{};
}

services::Result<web::ExtractReply> Web::extract_text(const web::ExtractRequest& q, services::Operation& op) {
    Url u;
    if (auto c = admit(op, "extract_text", q.url, u); c != abi::ok) return std::unexpected(c);
    HttpOptions o;
    o.max_bytes = config_.max_bytes;
    o.max_redirects = config_.max_redirects;
    later(op, [o, url = u.text(), s = settings()](runtime::SystemContext& ctx, runtime::Cap cap) {
        const HttpResult r = http(url, o, s);
        if (!r.error.empty()) return answer_error(ctx, std::move(cap), r);
        web::ExtractReply reply;
        reply.status = r.status;
        reply.url = r.url;
        if (lower(r.content_type).find("html") != std::string::npos || r.content_type.empty()) {
            auto e = extract_html(r.body, r.url);
            reply.title = std::move(e.title);
            reply.text = std::move(e.text);
            for (auto& [link, text] : e.links) reply.links.push_back(web::Link{std::move(link), std::move(text)});
        } else {
            reply.text = r.body;  // plain text and the like stay as they are
        }
        (void)ctx.reply(std::move(cap), abi::ok, wire::to_msgpack(reply));
    });
    return web::ExtractReply{};
}

services::Result<web::DownloadReply> Web::download(const web::DownloadRequest& q, services::Operation& op) {
    Url u;
    if (auto c = admit(op, "download", q.url, u); c != abi::ok) return std::unexpected(c);
    if (!q.sha256.empty() && (q.sha256.size() != 64 || q.sha256.find_first_not_of("0123456789abcdef") != std::string::npos))
        return std::unexpected(abi::invalid_argument);
    HttpOptions o;
    o.max_bytes = config_.max_download_bytes;
    o.fail_when_larger = true;
    o.max_redirects = config_.max_redirects;
    // What the paglet stores carries its marks (data residency).
    std::vector<std::string> marks = op.ctx.runtime().marks(op.sender()->id);
    later(op, [this, o, url = u.text(), s = settings(), expected = q.sha256,
               marks = std::move(marks)](runtime::SystemContext& ctx, runtime::Cap cap) {
        const HttpResult r = http(url, o, s);
        if (!r.error.empty()) return answer_error(ctx, std::move(cap), r);
        if (r.status < 200 || r.status >= 300) {
            web::DownloadReply reply;
            reply.status = r.status;
            reply.url = r.url;
            (void)ctx.reply(std::move(cap), abi::ok, wire::to_msgpack(reply));
            return;
        }
        const auto* bytes = reinterpret_cast<const std::uint8_t*>(r.body.data());
        const std::span<const std::uint8_t> content(bytes, r.body.size());
        const std::string hash = to_hex(sha256(content));
        if (!expected.empty() && hash != expected) {
            (void)ctx.reply(std::move(cap), abi::invalid_argument,
                            wire::to_msgpack(std::string("the content's hash is " + hash)));
            return;
        }
        auto artifact = services_->store_artifact(content, r.content_type.substr(0, 128), marks);
        if (!artifact) {
            (void)ctx.reply(std::move(cap), artifact.error());
            return;
        }
        web::DownloadReply reply{r.status, r.url, hash, static_cast<std::int64_t>(r.body.size()), r.content_type};
        (void)ctx.reply(std::move(cap), abi::ok, wire::to_msgpack(reply), {std::move(*artifact)});
    });
    return web::DownloadReply{};
}

services::Result<web::SearchReply> Web::search(const web::SearchRequest& q, services::Operation& op) {
    if (config_.search_url.empty()) return std::unexpected(abi::failed);
    if (q.query.empty() || q.query.size() > 1000) return std::unexpected(abi::invalid_argument);
    if (!op.sender() || op.sender()->id == "host") return std::unexpected(abi::denied);
    if (authorize_ && !authorize_(*op.sender(), "web", "search", "", "")) return std::unexpected(abi::denied);
    std::string url = config_.search_url;
    if (auto at = url.find("{query}"); at != std::string::npos) url.replace(at, 7, url_encode(q.query));
    HttpOptions o;
    o.max_bytes = config_.max_bytes;
    o.max_redirects = config_.max_redirects;
    o.check_destinations = false;  // the admins configured it
    const auto limit = static_cast<std::size_t>(std::clamp<std::int64_t>(q.limit, 1, 50));
    later(op, [o, url, limit, s = settings()](runtime::SystemContext& ctx, runtime::Cap cap) {
        const HttpResult r = http(url, o, s);
        if (!r.error.empty()) return answer_error(ctx, std::move(cap), r);
        auto json = wire::Json::parse(r.body);
        const wire::Json* results = json ? json->find("results") : nullptr;
        if (results == nullptr || !results->is_array()) {
            (void)ctx.reply(std::move(cap), abi::failed, wire::to_msgpack(std::string("unexpected search answer")));
            return;
        }
        web::SearchReply reply;
        for (const auto& item : results->as_array()) {
            if (reply.results.size() >= limit) break;
            auto text = [&](std::string_view key) {
                const wire::Json* v = item.find(key);
                return v != nullptr && v->is_string() ? v->as_string() : std::string();
            };
            if (text("url").empty()) continue;
            reply.results.push_back(web::SearchResult{text("title"), text("url"), text("content")});
        }
        (void)ctx.reply(std::move(cap), abi::ok, wire::to_msgpack(reply));
    });
    return web::SearchReply{};
}

}  // namespace paglets::gateway
