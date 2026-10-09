// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `ai` system paglet (planning/cpp-web-ai.md, section 3): local
// inference through a backend. Requests run on job threads and are answered
// when done; each owner has a quota of requests per hour; the mesh policy
// decides per operation and model. The backend is probed regularly: while
// it answers, the host offers `ai` (with its models), otherwise it
// withdraws the offer.
//
// Backends: Ollama (its local HTTP API) and `test` (deterministic answers
// computed from the input, for tests and demonstrations without a model).
// Apple Foundation Models (macOS 27) needs a Swift bridge built on such a
// machine and is not part of this build.

#include "gateway_impl.hpp"

#include <paglets/runtime/runtime.hpp>
#include <paglets/wire/json.hpp>
#include <paglets/wire/reflect.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>

namespace paglets::gateway {

namespace ai = services::ai;
using wire::Json;

namespace {

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string lower(std::string_view s) {
    std::string out(s);
    std::ranges::transform(out, out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::vector<std::string> words_of(std::string_view text) {
    std::vector<std::string> out;
    std::string w;
    for (char c : text) {
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (!w.empty()) out.push_back(std::move(w));
            w.clear();
        } else {
            w += c;
        }
    }
    if (!w.empty()) out.push_back(std::move(w));
    return out;
}

}  // namespace

// -- backends ---------------------------------------------------------------------------------

class AiBackend {
public:
    virtual ~AiBackend() = default;
    virtual std::string name() const = 0;
    // The models it has now; nullopt: the backend does not answer.
    virtual std::optional<std::vector<ai::Model>> models() = 0;
    virtual std::expected<std::string, std::int32_t> generate(const std::string& model, const std::string& system,
                                                              const std::string& prompt,
                                                              const std::vector<std::uint8_t>* image) = 0;
    virtual std::expected<std::vector<ai::Vector>, std::int32_t> embed(const std::string& model,
                                                                       const std::vector<std::string>& texts) = 0;
    // Backends without a model of their own answer these directly.
    virtual bool native_tasks() const { return false; }
};

namespace {

// Answers computed from the input: the same input gives the same answer.
class TestBackend final : public AiBackend {
public:
    std::string name() const override { return "test"; }
    std::optional<std::vector<ai::Model>> models() override { return std::vector<ai::Model>{{"test", 8192, true}}; }
    bool native_tasks() const override { return true; }

    std::expected<std::string, std::int32_t> generate(const std::string&, const std::string& system,
                                                      const std::string& prompt,
                                                      const std::vector<std::uint8_t>* image) override {
        if (image != nullptr) return "an image of " + std::to_string(image->size()) + " bytes";
        return (system.empty() ? std::string() : "[" + system + "] ") + "echo: " + prompt;
    }

    std::expected<std::vector<ai::Vector>, std::int32_t> embed(const std::string&,
                                                               const std::vector<std::string>& texts) override {
        // The hashing trick over words: similar texts get similar vectors.
        std::vector<ai::Vector> out;
        for (const auto& t : texts) {
            ai::Vector v;
            v.values.assign(64, 0.0);
            for (const auto& w : words_of(lower(t))) {
                std::uint32_t h = 2166136261u;
                for (unsigned char c : w) h = (h ^ c) * 16777619u;
                v.values[h % 64] += 1.0;
            }
            double norm = 0;
            for (double x : v.values) norm += x * x;
            norm = std::sqrt(norm);
            if (norm > 0) {
                for (double& x : v.values) x /= norm;
            }
            out.push_back(std::move(v));
        }
        return out;
    }
};

class OllamaBackend final : public AiBackend {
public:
    OllamaBackend(std::string url, std::chrono::milliseconds timeout) : url_(std::move(url)), timeout_(timeout) {}
    std::string name() const override { return "ollama"; }

    std::optional<std::vector<ai::Model>> models() override {
        auto r = call("/api/tags", "", std::chrono::milliseconds(5000));
        if (!r) return std::nullopt;
        std::vector<ai::Model> out;
        if (const Json* list = r->find("models"); list != nullptr && list->is_array()) {
            for (const auto& m : list->as_array()) {
                const Json* name = m.find("name");
                if (name == nullptr || !name->is_string()) continue;
                ai::Model model{name->as_string(), 0, false};
                if (const Json* details = m.find("details")) {
                    if (const Json* families = details->find("families"); families != nullptr && families->is_array()) {
                        for (const auto& f : families->as_array()) {
                            if (f.is_string() && (f.as_string() == "clip" || f.as_string() == "mllama")) model.vision = true;
                        }
                    }
                }
                out.push_back(std::move(model));
            }
        }
        return out;
    }

    std::expected<std::string, std::int32_t> generate(const std::string& model, const std::string& system,
                                                      const std::string& prompt,
                                                      const std::vector<std::uint8_t>* image) override {
        Json::Object body{{"model", Json(model)}, {"prompt", Json(prompt)}, {"stream", Json(false)}};
        if (!system.empty()) body.emplace_back("system", Json(system));
        if (image != nullptr) body.emplace_back("images", Json(Json::Array{Json(base64(*image))}));
        auto r = call("/api/generate", Json(std::move(body)).dump(), timeout_);
        if (!r) return std::unexpected(r.error());
        const Json* text = r->find("response");
        if (text == nullptr || !text->is_string()) return std::unexpected(abi::failed);
        return text->as_string();
    }

    std::expected<std::vector<ai::Vector>, std::int32_t> embed(const std::string& model,
                                                               const std::vector<std::string>& texts) override {
        Json::Array input;
        for (const auto& t : texts) input.emplace_back(t);
        auto r = call("/api/embed",
                      Json(Json::Object{{"model", Json(model)}, {"input", Json(std::move(input))}}).dump(), timeout_);
        if (!r) return std::unexpected(r.error());
        const Json* list = r->find("embeddings");
        if (list == nullptr || !list->is_array()) return std::unexpected(abi::failed);
        std::vector<ai::Vector> out;
        for (const auto& e : list->as_array()) {
            ai::Vector v;
            if (e.is_array()) {
                for (const auto& x : e.as_array()) {
                    double d = 0;
                    if (x.number_as(d)) v.values.push_back(d);
                }
            }
            out.push_back(std::move(v));
        }
        return out;
    }

private:
    static std::string base64(const std::vector<std::uint8_t>& data) {
        static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string out;
        std::size_t i = 0;
        for (; i + 2 < data.size(); i += 3) {
            const std::uint32_t n = static_cast<std::uint32_t>(data[i]) << 16 | data[i + 1] << 8 | data[i + 2];
            out += table[n >> 18];
            out += table[(n >> 12) & 63];
            out += table[(n >> 6) & 63];
            out += table[n & 63];
        }
        if (i + 1 == data.size()) {
            const std::uint32_t n = static_cast<std::uint32_t>(data[i]) << 16;
            out += table[n >> 18];
            out += table[(n >> 12) & 63];
            out += "==";
        } else if (i + 2 == data.size()) {
            const std::uint32_t n = static_cast<std::uint32_t>(data[i]) << 16 | data[i + 1] << 8;
            out += table[n >> 18];
            out += table[(n >> 12) & 63];
            out += table[(n >> 6) & 63];
            out += '=';
        }
        return out;
    }

    // The backend is local and configured by the admins: no destination
    // checks, no proxy.
    std::expected<Json, std::int32_t> call(const std::string& path, const std::string& body,
                                           std::chrono::milliseconds timeout) {
        HttpOptions o;
        o.method = body.empty() ? "GET" : "POST";
        o.body = body;
        o.content_type = "application/json";
        o.max_bytes = 64 * 1024 * 1024;
        o.check_destinations = false;
        HttpSettings s;
        s.timeout = timeout;
        const HttpResult r = http(url_ + path, o, s);
        if (!r.error.empty() || r.status != 200) return std::unexpected(abi::failed);
        auto json = Json::parse(r.body);
        if (!json) return std::unexpected(abi::failed);
        return *json;
    }

    std::string url_;
    std::chrono::milliseconds timeout_;
};

// Prompts for the tasks (backends with a model).
std::string summarize_prompt(const ai::SummarizeRequest& q) {
    return "Summarize the following text in at most " + std::to_string(q.max_words) +
           " words. Answer with the summary only.\n\n" + q.text;
}

std::string classify_prompt(const ai::ClassifyRequest& q) {
    std::string labels;
    for (const auto& l : q.labels) labels += (labels.empty() ? "" : ", ") + l;
    return "Classify the following text into exactly one of these labels: " + labels +
           ". Answer with the label only.\n\n" + q.text;
}

std::string extract_prompt(const ai::ExtractRequest& q) {
    std::string fields;
    for (const auto& f : q.fields) fields += "- " + f.name + (f.description.empty() ? "" : ": " + f.description) + "\n";
    return "Extract these fields from the text below. Answer with one line per field, as `name: value`, and an empty "
           "value when the text does not say.\n" +
           fields + "\nText:\n" + q.text;
}

// The label the answer names (the first that appears), else the first.
std::string label_of(std::string_view answer, const std::vector<std::string>& labels) {
    const std::string low = lower(answer);
    std::size_t best = std::string::npos;
    std::string out = labels.empty() ? std::string() : labels.front();
    for (const auto& l : labels) {
        const auto at = low.find(lower(l));
        if (at != std::string::npos && at < best) {
            best = at;
            out = l;
        }
    }
    return out;
}

std::vector<ai::FieldValue> fields_of(std::string_view answer, const std::vector<ai::Field>& fields) {
    std::vector<ai::FieldValue> out;
    for (const auto& f : fields) {
        ai::FieldValue v{f.name, {}};
        const std::string key = lower(f.name) + ":";
        std::size_t start = 0;
        while (start < answer.size()) {
            const auto end = answer.find('\n', start);
            std::string_view line = answer.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start);
            while (!line.empty() && (line.front() == ' ' || line.front() == '-' || line.front() == '*')) line.remove_prefix(1);
            if (lower(line.substr(0, key.size())) == key) {
                std::string_view value = line.substr(key.size());
                while (!value.empty() && value.front() == ' ') value.remove_prefix(1);
                while (!value.empty() && (value.back() == ' ' || value.back() == '\r')) value.remove_suffix(1);
                v.value = std::string(value);
                break;
            }
            if (end == std::string_view::npos) break;
            start = end + 1;
        }
        out.push_back(std::move(v));
    }
    return out;
}

// The test backend's tasks: summaries are the leading words, labels the most
// frequent one in the text, fields `name: value` lines of the text.
std::string test_summary(const ai::SummarizeRequest& q) {
    const auto words = words_of(q.text);
    const auto n = std::min<std::size_t>(words.size(), static_cast<std::size_t>(std::max<std::int64_t>(1, q.max_words)));
    std::string out;
    for (std::size_t i = 0; i < n; ++i) out += (i == 0 ? "" : " ") + words[i];
    if (n < words.size()) out += " ...";
    return out;
}

std::string test_label(const ai::ClassifyRequest& q) {
    const std::string low = lower(q.text);
    std::string best = q.labels.empty() ? std::string() : q.labels.front();
    std::size_t most = 0;
    for (const auto& l : q.labels) {
        std::size_t count = 0;
        const std::string ll = lower(l);
        for (auto at = low.find(ll); !ll.empty() && at != std::string::npos; at = low.find(ll, at + ll.size())) ++count;
        if (count > most) {
            most = count;
            best = l;
        }
    }
    return best;
}

}  // namespace

// -- the `ai` system paglet ------------------------------------------------------------------

Ai::Ai(AiConfig config, Authorize authorize, OfferSink offers)
    : config_(std::move(config)), authorize_(std::move(authorize)), offers_(std::move(offers)),
      jobs_(config_.max_parallel) {
    if (config_.backend == "test") {
        backend_ = std::make_unique<TestBackend>();
    } else {
        backend_ = std::make_unique<OllamaBackend>(config_.url, config_.timeout);
    }
}

Ai::~Ai() {
    stop();
}

void Ai::start(runtime::SystemContext& ctx) {
    ctx_ = &ctx;
    probe();
    prober_ = std::thread([this] {
        std::unique_lock lock(mu_);
        while (!stopping_) {
            probe_cv_.wait_for(lock, config_.probe, [&] { return stopping_; });
            if (stopping_) break;
            lock.unlock();
            probe();
            lock.lock();
        }
    });
}

void Ai::stop() {
    {
        std::lock_guard lock(mu_);
        stopping_ = true;
    }
    probe_cv_.notify_all();
    if (prober_.joinable()) prober_.join();
    jobs_.stop();
}

void Ai::probe() {
    auto models = backend_->models();
    std::vector<ai::Model> exposed;
    if (models) {
        for (auto& m : *models) {
            if (config_.models.empty() || std::ranges::find(config_.models, m.name) != config_.models.end())
                exposed.push_back(std::move(m));
        }
    }
    const bool available = models.has_value() && !exposed.empty();
    bool changed = false;
    {
        std::lock_guard lock(mu_);
        changed = available != available_ || exposed.size() != models_.size() ||
                  !std::ranges::equal(exposed, models_, [](const auto& a, const auto& b) { return a.name == b.name; });
        available_ = available;
        models_ = exposed;
    }
    if (!changed || !offers_) return;
    if (!available) {
        offers_("ai", std::nullopt);
        return;
    }
    services::mesh_info::Offer o;
    o.service = "ai";
    o.ops = {"summarize", "classify", "extract", "generate", "embed"};
    using A = services::mesh_info::Attribute;
    std::string names;
    std::int64_t context = 0;
    bool vision = false;
    for (const auto& m : exposed) {
        names += (names.empty() ? "" : ",") + m.name;
        context = std::max(context, m.context);
        vision = vision || m.vision;
    }
    if (vision) o.ops.emplace_back("describe_image");
    o.attributes.push_back(A{"backend", backend_->name(), 0, false});
    o.attributes.push_back(A{"models", names, 0, false});
    std::string tasks;
    for (const auto& op : o.ops) tasks += (tasks.empty() ? "" : ",") + op;
    o.attributes.push_back(A{"tasks", tasks, 0, false});
    if (context > 0) o.attributes.push_back(A{"context", {}, static_cast<double>(context), true});
    o.attributes.push_back(A{"vision", vision ? "yes" : "no", 0, false});
    offers_("ai", std::move(o));
}

std::string Ai::choose_model(const std::string& asked) const {
    std::lock_guard lock(mu_);
    if (asked.empty()) return models_.empty() ? std::string() : models_.front().name;
    for (const auto& m : models_) {
        if (m.name == asked) return asked;
    }
    return {};
}

template <class Reply>
services::Result<Reply> Ai::run(services::Operation& op, std::string_view op_name, std::string asked,
                                std::size_t input_bytes,
                                std::function<std::expected<Reply, std::int32_t>(AiBackend&, const std::string&)> work) {
    if (!op.sender() || op.sender()->id == "host") return std::unexpected(abi::denied);
    if (input_bytes > static_cast<std::size_t>(config_.max_input_bytes)) return std::unexpected(abi::too_large);
    const std::string model = choose_model(asked);
    if (model.empty()) return std::unexpected(asked.empty() ? abi::failed : abi::not_found);
    if (authorize_ && !authorize_(*op.sender(), "ai", op_name, model, "")) return std::unexpected(abi::denied);
    {
        // The owner's quota: requests in the last hour.
        std::lock_guard lock(mu_);
        auto& times = requests_[op.sender()->owner];
        const std::int64_t now = now_ms();
        while (!times.empty() && times.front() < now - 3'600'000) times.pop_front();
        if (static_cast<std::int64_t>(times.size()) >= config_.max_requests_per_owner) return std::unexpected(abi::quota);
        times.push_back(now);
    }
    auto reply = op.call.defer();
    if (!reply) return Reply{};
    runtime::SystemContext* ctx = ctx_;
    AiBackend* backend = backend_.get();
    jobs_.post([ctx, backend, model, cap = std::move(*reply), work = std::move(work)](bool stopping) mutable {
        if (stopping) {
            (void)ctx->reply(std::move(cap), abi::failed);
            return;
        }
        auto r = work(*backend, model);
        if (!r) {
            (void)ctx->reply(std::move(cap), r.error());
            return;
        }
        (void)ctx->reply(std::move(cap), abi::ok, wire::to_msgpack(*r));
    });
    return Reply{};
}

services::Result<ai::Capabilities> Ai::capabilities(const ai::CapabilitiesRequest&, services::Operation&) {
    std::lock_guard lock(mu_);
    ai::Capabilities c;
    c.backend = backend_->name();
    c.available = available_;
    c.models = models_;
    c.tasks = {"summarize", "classify", "extract", "generate", "embed"};
    if (std::ranges::any_of(models_, [](const auto& m) { return m.vision; })) c.tasks.emplace_back("describe_image");
    c.max_input_bytes = config_.max_input_bytes;
    c.queue = static_cast<std::int64_t>(jobs_.pending());
    return c;
}

namespace {

std::int64_t since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
}

}  // namespace

services::Result<ai::TextReply> Ai::summarize(const ai::SummarizeRequest& q, services::Operation& op) {
    return run<ai::TextReply>(op, "summarize", q.model, q.text.size(),
                              [q](AiBackend& b, const std::string& model) -> std::expected<ai::TextReply, std::int32_t> {
                                  const auto start = std::chrono::steady_clock::now();
                                  if (b.native_tasks()) return ai::TextReply{test_summary(q), model, since(start)};
                                  auto text = b.generate(model, "", summarize_prompt(q), nullptr);
                                  if (!text) return std::unexpected(text.error());
                                  return ai::TextReply{*text, model, since(start)};
                              });
}

services::Result<ai::ClassifyReply> Ai::classify(const ai::ClassifyRequest& q, services::Operation& op) {
    if (q.labels.empty() || q.labels.size() > 100) return std::unexpected(abi::invalid_argument);
    return run<ai::ClassifyReply>(
        op, "classify", q.model, q.text.size(),
        [q](AiBackend& b, const std::string& model) -> std::expected<ai::ClassifyReply, std::int32_t> {
            if (b.native_tasks()) return ai::ClassifyReply{test_label(q), model};
            auto text = b.generate(model, "", classify_prompt(q), nullptr);
            if (!text) return std::unexpected(text.error());
            return ai::ClassifyReply{label_of(*text, q.labels), model};
        });
}

services::Result<ai::ExtractReply> Ai::extract(const ai::ExtractRequest& q, services::Operation& op) {
    if (q.fields.empty() || q.fields.size() > 100) return std::unexpected(abi::invalid_argument);
    return run<ai::ExtractReply>(
        op, "extract", q.model, q.text.size(),
        [q](AiBackend& b, const std::string& model) -> std::expected<ai::ExtractReply, std::int32_t> {
            if (b.native_tasks()) return ai::ExtractReply{fields_of(q.text, q.fields), model};
            auto text = b.generate(model, "", extract_prompt(q), nullptr);
            if (!text) return std::unexpected(text.error());
            return ai::ExtractReply{fields_of(*text, q.fields), model};
        });
}

services::Result<ai::TextReply> Ai::generate(const ai::GenerateRequest& q, services::Operation& op) {
    return run<ai::TextReply>(op, "generate", q.model, q.prompt.size() + q.system.size(),
                              [q](AiBackend& b, const std::string& model) -> std::expected<ai::TextReply, std::int32_t> {
                                  const auto start = std::chrono::steady_clock::now();
                                  auto text = b.generate(model, q.system, q.prompt, nullptr);
                                  if (!text) return std::unexpected(text.error());
                                  return ai::TextReply{*text, model, since(start)};
                              });
}

services::Result<ai::EmbedReply> Ai::embed(const ai::EmbedRequest& q, services::Operation& op) {
    if (q.texts.empty() || q.texts.size() > 256) return std::unexpected(abi::invalid_argument);
    std::size_t bytes = 0;
    for (const auto& t : q.texts) bytes += t.size();
    return run<ai::EmbedReply>(op, "embed", q.model, bytes,
                               [q](AiBackend& b, const std::string& model) -> std::expected<ai::EmbedReply, std::int32_t> {
                                   auto v = b.embed(model, q.texts);
                                   if (!v) return std::unexpected(v.error());
                                   return ai::EmbedReply{std::move(*v), model};
                               });
}

services::Result<ai::TextReply> Ai::describe_image(const ai::DescribeImageRequest& q, services::Operation& op) {
    if (q.image.empty()) return std::unexpected(abi::invalid_argument);
    return run<ai::TextReply>(
        op, "describe_image", q.model, q.image.size() + q.prompt.size(),
        [q](AiBackend& b, const std::string& model) -> std::expected<ai::TextReply, std::int32_t> {
            const auto start = std::chrono::steady_clock::now();
            auto text = b.generate(model, "", q.prompt.empty() ? "Describe this image briefly." : q.prompt, &q.image);
            if (!text) return std::unexpected(text.error());
            return ai::TextReply{*text, model, since(start)};
        });
}

}  // namespace paglets::gateway
