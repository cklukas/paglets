// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internals of the gateway system paglets (planning/cpp-web-ai.md).

#pragma once

#include <paglets/gateway/gateway.hpp>
#include <paglets/services/ai.hpp>
#include <paglets/services/contract.hpp>
#include <paglets/services/web.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace paglets::gateway {

struct Url {
    std::string scheme;  // http, https
    std::string host;    // lower case; IPv6 in brackets
    int port = 0;
    std::string target;  // path and query, "/" at least
    std::string path;    // without the query

    std::string origin() const;
    std::string text() const;
};
std::optional<Url> parse_url(std::string_view text);
std::string url_encode(std::string_view s);

// Work that blocks (network, inference) runs here, not on scheduler lanes:
// at most `threads` jobs at a time, the rest wait. stop() ends the threads
// after the jobs that run; waiting jobs run with `stopping` set.
class Jobs {
public:
    explicit Jobs(int threads) : threads_(std::max(1, threads)) {}
    ~Jobs() { stop(); }
    void post(std::function<void(bool stopping)> job);
    void stop();
    std::size_t pending() const;

private:
    void run();

    int threads_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void(bool)>> queue_;
    std::vector<std::thread> workers_;
    int busy_ = 0;
    bool stopping_ = false;
};

// An HTTP(S) response as far as it was read.
struct HttpResult {
    int status = 0;
    std::string url;  // final URL (after redirects)
    std::string content_type;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
    bool truncated = false;
    std::string error;  // empty: a response arrived
    std::int32_t code = 0;  // abi error for `error`
};

struct HttpSettings {
    bool internet = true;
    std::vector<std::string> internal;  // allowed internal URL prefixes
    std::string proxy;
    std::string ca_file;
    std::chrono::milliseconds timeout{20000};
};

struct HttpOptions {
    std::string method = "GET";  // GET, HEAD, POST
    std::string body;            // POST
    std::string content_type;    // POST
    std::int64_t max_bytes = 0;
    bool fail_when_larger = false;  // a larger body is an error (else truncated)
    int max_redirects = 0;
    bool check_destinations = true;
};

// One request, following redirects; with `check_destinations` every hop's
// addresses are checked against `settings` (internal ones refused unless a
// prefix allows them) and the connection goes to the checked address.
HttpResult http(const std::string& url, const HttpOptions& options, const HttpSettings& settings);

class Web final : public services::ContractPaglet<services::web::Contract, Web> {
public:
    Web(WebConfig config, std::shared_ptr<services::SystemServices> services, Authorize authorize);
    void start(runtime::SystemContext& ctx) override;
    void stop() override;

    services::Result<services::web::Capabilities> capabilities(const services::web::CapabilitiesRequest&,
                                                               services::Operation&);
    services::Result<services::web::FetchReply> fetch(const services::web::FetchRequest&, services::Operation&);
    services::Result<services::web::ExtractReply> extract_text(const services::web::ExtractRequest&,
                                                               services::Operation&);
    services::Result<services::web::DownloadReply> download(const services::web::DownloadRequest&,
                                                            services::Operation&);
    services::Result<services::web::SearchReply> search(const services::web::SearchRequest&, services::Operation&);

    services::mesh_info::Offer offer() const;

private:
    HttpSettings settings() const;
    // Checks the URL and the caller's permission.
    std::int32_t admit(services::Operation& op, std::string_view op_name, const std::string& url, Url& parsed) const;
    // Defers the reply and runs `work` on a job thread.
    void later(services::Operation& op, std::function<void(runtime::SystemContext&, runtime::Cap)> work);

    WebConfig config_;
    std::shared_ptr<services::SystemServices> services_;
    Authorize authorize_;
    runtime::SystemContext* ctx_ = nullptr;
    Jobs jobs_;
};

class AiBackend;

class Ai final : public services::ContractPaglet<services::ai::Contract, Ai> {
public:
    Ai(AiConfig config, Authorize authorize, OfferSink offers);
    ~Ai() override;
    void start(runtime::SystemContext& ctx) override;
    void stop() override;

    services::Result<services::ai::Capabilities> capabilities(const services::ai::CapabilitiesRequest&,
                                                              services::Operation&);
    services::Result<services::ai::TextReply> summarize(const services::ai::SummarizeRequest&, services::Operation&);
    services::Result<services::ai::ClassifyReply> classify(const services::ai::ClassifyRequest&,
                                                           services::Operation&);
    services::Result<services::ai::ExtractReply> extract(const services::ai::ExtractRequest&, services::Operation&);
    services::Result<services::ai::TextReply> generate(const services::ai::GenerateRequest&, services::Operation&);
    services::Result<services::ai::EmbedReply> embed(const services::ai::EmbedRequest&, services::Operation&);
    services::Result<services::ai::TextReply> describe_image(const services::ai::DescribeImageRequest&,
                                                             services::Operation&);

    // Checks the backend and announces or withdraws the offer.
    void probe();

private:
    template <class Reply>
    services::Result<Reply> run(services::Operation& op, std::string_view op_name, std::string model,
                                std::size_t input_bytes,
                                std::function<std::expected<Reply, std::int32_t>(AiBackend&, const std::string&)> work);
    std::string choose_model(const std::string& asked) const;

    AiConfig config_;
    Authorize authorize_;
    OfferSink offers_;
    std::unique_ptr<AiBackend> backend_;
    runtime::SystemContext* ctx_ = nullptr;
    Jobs jobs_;
    mutable std::mutex mu_;
    std::vector<services::ai::Model> models_;  // as last probed
    bool available_ = false;
    std::map<std::string, std::deque<std::int64_t>> requests_;  // owner -> request times (ms)
    std::thread prober_;
    std::condition_variable probe_cv_;
    bool stopping_ = false;
};

}  // namespace paglets::gateway
