// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `artifacts` system paglet: a content-addressed blob store. Files are
// named by the SHA-256 of their content; a small side file keeps the media
// type, the creation time and the data residency marks of the paglets that
// stored it (planning/cpp-residency.md): whoever reads it gets them too.
//
// On a host of a mesh, artifact capabilities also name this host and a
// token it made (planning/cpp-content-exchange.md), so they work on every
// host: one that lacks the content fetches it from here, directly between
// the hosts, and keeps it in its own store. Files shared with
// `files.share` are served from where they are, without a copy.

#include "content_hash.hpp"
#include "services.hpp"

#include <paglets/runtime/runtime.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <fstream>
#include <limits>

namespace paglets::services::impl {

namespace {

struct Meta {
    std::string media_type;
    std::int64_t created_ms = 0;
    std::vector<std::string> marks;
};

// Shared files as they are remembered across restarts.
struct SharedRecord {
    std::string hash;
    std::string path;
    std::uint64_t size = 0;
    std::int64_t modified_ns = 0;
    std::string media_type;
    std::vector<std::string> marks;
    std::int64_t created_ms = 0;
};

struct SharesState {
    std::vector<SharedRecord> shares;
};

// Shared files remembered at most; the oldest are forgotten first.
constexpr std::size_t max_shares = 10000;
constexpr std::size_t copy_block = 1024 * 1024;

bool is_hex(std::string_view s) {
    return !s.empty() && s.find_first_not_of("0123456789abcdef") == std::string_view::npos;
}

bool valid_hash(std::string_view hash) {
    return hash.size() == 64 && is_hex(hash);
}

fs::path to_path(std::string_view utf8) {
    return fs::path(std::u8string(reinterpret_cast<const char8_t*>(utf8.data()), utf8.size()));
}

std::string utf8_of(const fs::path& p) {
    const std::u8string s = p.u8string();
    return std::string(reinterpret_cast<const char*>(s.data()), s.size());
}

std::optional<std::int64_t> modified_ns(const fs::path& p) {
    std::error_code ec;
    const auto t = fs::last_write_time(p, ec);
    if (ec) return std::nullopt;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

}  // namespace

Artifacts::Artifacts(fs::path root, bool temporary, std::uint64_t limit, fs::path shares_file)
    : root_(std::move(root)), temporary_(temporary), limit_(limit), shares_file_(std::move(shares_file)) {
    std::error_code ec;
    fs::create_directories(root_, ec);
    // Fetches a host stopped during leave incomplete files behind.
    for (const auto& entry : fs::directory_iterator(root_, ec)) {
        if (entry.path().filename().string().starts_with(".incoming-")) fs::remove(entry.path(), ec);
    }
    if (shares_file_.empty()) return;
    SharesState state;
    if (auto bytes = read_all(shares_file_); bytes && wire::from_msgpack(*bytes, state)) {
        for (auto& r : state.shares) {
            if (!valid_hash(r.hash)) continue;
            shares_[r.hash] = Share{std::move(r.path), r.size, r.modified_ns, std::move(r.media_type),
                                    std::move(r.marks), r.created_ms};
        }
    }
}

Artifacts::~Artifacts() {
    jobs_.stop();
    if (temporary_) {
        std::error_code ec;
        fs::remove_all(root_, ec);
    }
}

void Artifacts::start(runtime::SystemContext& ctx) {
    ctx_ = &ctx;
}

void Artifacts::stop() {
    jobs_.stop();
}

// -- references and capabilities ---------------------------------------------

std::optional<Artifacts::Ref> Artifacts::parse_resource(std::string_view resource) {
    Ref ref;
    const auto at = resource.find('@');
    ref.hash = std::string(resource.substr(0, at));
    if (!valid_hash(ref.hash)) return std::nullopt;
    if (at == std::string_view::npos) return ref;
    const std::string_view rest = resource.substr(at + 1);
    const auto slash = rest.find('/');
    if (slash == std::string_view::npos) return std::nullopt;
    ref.host = std::string(rest.substr(0, slash));
    ref.token = std::string(rest.substr(slash + 1));
    if (!is_hex(ref.host) || ref.host.size() > 128 || !is_hex(ref.token) || ref.token.size() > 128) {
        return std::nullopt;
    }
    return ref;
}

bool Artifacts::readable(const runtime::Cap& cap) {
    if (cap.kind != runtime::Cap::Kind::resource || cap.target != runtime::system_paglet_id("artifacts") ||
        cap.resource_type != "artifact" || !parse_resource(cap.resource)) {
        return false;
    }
    return std::ranges::find(cap.ops, std::string("read")) != cap.ops.end() ||
           std::ranges::find(cap.ops, std::string("*")) != cap.ops.end();
}

void Artifacts::set_exchange(ContentExchange exchange) {
    auto shared = std::make_shared<const ContentExchange>(std::move(exchange));
    std::lock_guard lock(mu_);
    exchange_ = std::move(shared);
}

std::string Artifacts::resource_of(std::string_view hash) const {
    std::shared_ptr<const ContentExchange> ex;
    {
        std::lock_guard lock(mu_);
        ex = exchange_;
    }
    if (!ex || !ex->token) return std::string(hash);
    return std::string(hash) + "@" + ex->host + "/" + ex->token(hash);
}

runtime::Cap Artifacts::capability(std::string_view hash) const {
    runtime::Cap c;
    c.kind = runtime::Cap::Kind::resource;
    c.target = runtime::system_paglet_id("artifacts");
    c.resource_type = "artifact";
    c.resource = resource_of(hash);
    c.ops = {"read"};
    c.transferable = true;
    c.id = random_hex(16);
    return c;
}

Result<Artifacts::Ref> Artifacts::lent_ref(Operation& op) const {
    if (op.lent().empty()) return std::unexpected(abi::invalid_argument);
    const runtime::Cap& cap = op.lent().front();
    if (auto c = op.ctx.check(cap, "artifact", "read"); c != abi::ok) return std::unexpected(c);
    auto ref = parse_resource(cap.resource);
    if (!ref) return std::unexpected(abi::bad_handle);
    return *ref;
}

// -- content of this host ----------------------------------------------------

std::optional<ServedContent> Artifacts::served_locked(std::string_view hash) const {
    if (!valid_hash(hash)) return std::nullopt;
    const fs::path data = root_ / std::string(hash);
    std::error_code ec;
    if (const auto size = fs::file_size(data, ec); !ec) {
        Meta meta;
        if (auto bytes = read_all(root_ / (std::string(hash) + ".meta"))) (void)wire::from_msgpack(*bytes, meta);
        return ServedContent{data, size, std::move(meta.marks)};
    }
    auto it = shares_.find(hash);
    if (it == shares_.end()) return std::nullopt;
    // A shared file is served while it is unchanged.
    const fs::path file = to_path(it->second.path);
    const auto size = fs::file_size(file, ec);
    if (ec || size != it->second.size || modified_ns(file) != it->second.modified_ns) return std::nullopt;
    return ServedContent{file, size, it->second.marks};
}

std::optional<ServedContent> Artifacts::served(std::string_view hash) const {
    std::lock_guard lock(mu_);
    return served_locked(hash);
}

Result<artifacts::Info> Artifacts::info_of(std::string_view hash) const {
    std::lock_guard lock(mu_);
    const fs::path data = root_ / std::string(hash);
    std::error_code ec;
    if (const auto size = fs::file_size(data, ec); !ec) {
        Meta meta;
        if (auto bytes = read_all(root_ / (std::string(hash) + ".meta"))) (void)wire::from_msgpack(*bytes, meta);
        return artifacts::Info{std::string(hash), size, meta.media_type, meta.created_ms};
    }
    if (!served_locked(hash)) return std::unexpected(abi::not_found);
    const Share& s = shares_.find(hash)->second;
    return artifacts::Info{std::string(hash), s.size, s.media_type, s.created_ms};
}

std::uint64_t Artifacts::used() const {
    std::uint64_t total = 0;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(root_, ec)) {
        const auto size = entry.file_size(ec);
        if (!ec) total += size;
    }
    return total;
}

Result<std::string> Artifacts::store(std::span<const std::uint8_t> content, std::string_view media_type,
                                     const std::vector<std::string>& marks) {
    if (media_type.size() > 128) return std::unexpected(abi::invalid_argument);
    ContentHash digest;
    digest.update(content);
    const std::string hash = to_hex(digest.finish());
    {
        std::lock_guard lock(mu_);
        const fs::path data = root_ / hash;
        std::error_code ec;
        if (fs::exists(data, ec)) {
            // The same content stored by a marked paglet takes its marks.
            Meta meta;
            if (auto bytes = read_all(root_ / (hash + ".meta"))) (void)wire::from_msgpack(*bytes, meta);
            bool changed = false;
            for (const auto& m : marks) {
                if (std::ranges::find(meta.marks, m) != meta.marks.end()) continue;
                meta.marks.push_back(m);
                changed = true;
            }
            if (changed && !write_atomically(root_ / (hash + ".meta"), wire::to_msgpack(meta)))
                return std::unexpected(abi::internal);
        } else {
            if (used() + content.size() > limit_) return std::unexpected(abi::quota);
            if (!write_atomically(root_ / (hash + ".meta"),
                                  wire::to_msgpack(Meta{std::string(media_type), now_ms(), marks})) ||
                !write_atomically(data, content)) {
                return std::unexpected(abi::internal);
            }
        }
    }
    return hash;
}

void Artifacts::save_shares() const {
    if (shares_file_.empty()) return;
    SharesState state;
    for (const auto& [hash, s] : shares_) {
        state.shares.push_back(SharedRecord{hash, s.path, s.size, s.modified_ns, s.media_type, s.marks, s.created_ms});
    }
    std::error_code ec;
    fs::create_directories(shares_file_.parent_path(), ec);
    (void)write_atomically(shares_file_, wire::to_msgpack(state));
}

Result<files::Hashed> Artifacts::hash_file(const fs::path& file) {
    std::error_code ec;
    const auto size = fs::file_size(file, ec);
    if (ec) return std::unexpected(abi::not_found);
    const auto before = modified_ns(file);
    std::ifstream in(file, std::ios::binary);
    if (!in) return std::unexpected(abi::denied);
    ContentHash digest;
    std::vector<std::uint8_t> block(copy_block);
    std::uint64_t read = 0;
    while (in) {
        in.read(reinterpret_cast<char*>(block.data()), static_cast<std::streamsize>(block.size()));
        const auto got = static_cast<std::size_t>(in.gcount());
        if (got == 0) break;
        digest.update(std::span<const std::uint8_t>(block.data(), got));
        read += got;
    }
    if (in.bad()) return std::unexpected(abi::internal);
    // A file that changed while it was hashed has no hash.
    if (read != size || fs::file_size(file, ec) != size || modified_ns(file) != before || !before) {
        return std::unexpected(abi::bad_state);
    }
    return files::Hashed{to_hex(digest.finish()), size};
}

Result<files::Shared> Artifacts::share_file(const fs::path& file, std::string media_type,
                                            std::vector<std::string> marks) {
    if (media_type.size() > 128) return std::unexpected(abi::invalid_argument);
    const auto before = modified_ns(file);
    auto hashed = hash_file(file);
    if (!hashed) return std::unexpected(hashed.error());
    // Served while it keeps the size and modification time it was hashed with.
    if (modified_ns(file) != before || !before) return std::unexpected(abi::bad_state);
    {
        std::lock_guard lock(mu_);
        shares_[hashed->sha256] =
            Share{utf8_of(file), hashed->size, *before, std::move(media_type), std::move(marks), now_ms()};
        while (shares_.size() > max_shares) {
            auto oldest = std::ranges::min_element(shares_, {}, [](const auto& e) { return e.second.created_ms; });
            shares_.erase(oldest);
        }
        save_shares();
    }
    return files::Shared{hashed->sha256, hashed->size};
}

Result<artifacts::GetReply> Artifacts::read_range(std::string_view hash, const artifacts::GetRequest& q,
                                                  const std::optional<abi::SenderRecord>& reader) {
    auto content = served(hash);
    if (!content) return std::unexpected(abi::not_found);
    const std::uint64_t size = content->size;
    if (q.offset > size) return std::unexpected(abi::invalid_argument);
    std::uint64_t length = std::min<std::uint64_t>(size - q.offset, Files::max_transfer);
    if (q.length) {
        if (*q.length > Files::max_transfer) return std::unexpected(abi::too_large);
        length = std::min(length, *q.length);
    }
    artifacts::GetReply reply;
    reply.size = size;
    reply.data.resize(static_cast<std::size_t>(length));
    std::ifstream in(content->file, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(q.offset));
    if (length > 0 && !in.read(reinterpret_cast<char*>(reply.data.data()), static_cast<std::streamsize>(length))) {
        return std::unexpected(abi::internal);
    }
    reply.eof = q.offset + length >= size;
    // The reader carries what the writers carried.
    if (reader && ctx_ != nullptr) {
        for (auto& m : content->marks) ctx_->runtime().mark(reader->id, std::move(m));
    }
    return reply;
}

// -- content of other hosts --------------------------------------------------

void Artifacts::obtain(const Ref& ref, std::function<void(std::int32_t)> done) {
    std::shared_ptr<const ContentExchange> ex;
    bool here = false;
    {
        std::lock_guard lock(mu_);
        here = served_locked(ref.hash).has_value();
        ex = exchange_;
    }
    if (here) return done(abi::ok);
    if (!ex || !ex->fetch || ref.host.empty() || ref.host == ex->host) return done(abi::not_found);
    bool first = false;
    {
        std::lock_guard lock(mu_);
        auto& waiters = waiting_[ref.hash];
        first = waiters.empty();
        waiters.push_back(std::move(done));
    }
    if (!first) return;  // fetched once for every waiter
    const std::uint64_t u = used();
    const fs::path tmp = root_ / (".incoming-" + random_hex(8));
    if (u >= limit_) return fetched(ref.hash, tmp, ContentFetched{abi::quota, "the artifact store is full", 0, {}});
    ex->fetch(ContentFetch{ref.hash, ref.host, ref.token, tmp, limit_ - u},
              [this, hash = ref.hash, tmp](ContentFetched r) {
                  jobs_.post([this, hash, tmp, r = std::move(r)](bool stopping) mutable {
                      if (stopping && r.status == abi::ok) r.status = abi::failed;
                      fetched(hash, tmp, r);
                  });
              });
}

void Artifacts::fetched(const std::string& hash, const fs::path& tmp, const ContentFetched& result) {
    std::int32_t status = result.status;
    std::error_code ec;
    if (status == abi::ok) {
        std::lock_guard lock(mu_);
        const fs::path data = root_ / hash;
        Meta meta;
        if (fs::exists(data, ec)) {
            fs::remove(tmp, ec);
            if (auto bytes = read_all(root_ / (hash + ".meta"))) (void)wire::from_msgpack(*bytes, meta);
        } else {
            meta.created_ms = now_ms();
        }
        for (const auto& m : result.marks) {
            if (std::ranges::find(meta.marks, m) == meta.marks.end()) meta.marks.push_back(m);
        }
        if (!write_atomically(root_ / (hash + ".meta"), wire::to_msgpack(meta))) {
            status = abi::internal;
        } else if (!fs::exists(data, ec)) {
            fs::rename(tmp, data, ec);
            if (ec) status = abi::internal;
        }
    }
    if (status != abi::ok) {
        fs::remove(tmp, ec);
        if (ctx_ != nullptr) {
            ctx_->log(abi::LogLevel::info, "artifacts: content " + hash.substr(0, 16) + " not fetched: " +
                                               (result.error.empty() ? std::string(abi::error_name(status))
                                                                     : result.error));
        }
    }
    std::vector<std::function<void(std::int32_t)>> waiters;
    {
        std::lock_guard lock(mu_);
        if (auto node = waiting_.extract(hash); !node.empty()) waiters = std::move(node.mapped());
    }
    for (auto& w : waiters) w(status);
}

void Artifacts::copy_to(const Ref& ref, fs::path into, std::function<void(std::int32_t)> done) {
    std::optional<ServedContent> here;
    std::shared_ptr<const ContentExchange> ex;
    {
        std::lock_guard lock(mu_);
        here = served_locked(ref.hash);
        ex = exchange_;
    }
    if (here) {
        jobs_.post([file = here->file, into = std::move(into), done = std::move(done)](bool stopping) {
            if (stopping) return done(abi::failed);
            std::error_code ec;
            fs::copy_file(file, into, fs::copy_options::overwrite_existing, ec);
            done(ec ? abi::internal : abi::ok);
        });
        return;
    }
    if (!ex || !ex->fetch || ref.host.empty() || ref.host == ex->host) {
        jobs_.post([done = std::move(done)](bool) { done(abi::not_found); });
        return;
    }
    ex->fetch(ContentFetch{ref.hash, ref.host, ref.token, std::move(into), std::numeric_limits<std::uint64_t>::max()},
              [this, done = std::move(done)](ContentFetched r) {
                  if (r.status != abi::ok && ctx_ != nullptr) {
                      ctx_->log(abi::LogLevel::info, "artifacts: content not fetched: " + r.error);
                  }
                  jobs_.post([done, status = r.status](bool) { done(status); });
              });
}

void Artifacts::load(const runtime::Cap& cap, std::uint64_t max,
                     std::function<void(std::expected<runtime::JobArtifactInput, std::int32_t>)> done) {
    if (!readable(cap)) return done(std::unexpected(abi::bad_handle));
    const Ref ref = *parse_resource(cap.resource);
    obtain(ref, [this, hash = ref.hash, max, done = std::move(done)](std::int32_t status) {
        if (status != abi::ok) return done(std::unexpected(status));
        auto content = served(hash);
        if (!content) return done(std::unexpected(abi::not_found));
        if (content->size > max) return done(std::unexpected(abi::too_large));
        auto bytes = read_all(content->file);
        if (!bytes) return done(std::unexpected(abi::internal));
        done(runtime::JobArtifactInput{std::move(*bytes), std::move(content->marks)});
    });
}

// -- operations --------------------------------------------------------------

Result<artifacts::Info> Artifacts::put(const artifacts::PutRequest& q, Operation& op) {
    if (!caller_of(op.call)) return std::unexpected(abi::denied);
    if (q.data.size() > Files::max_transfer) return std::unexpected(abi::too_large);
    auto hash = store(q.data, q.media_type, op.ctx.runtime().marks(op.sender()->id));
    if (!hash) return std::unexpected(hash.error());
    op.give.push_back(op.ctx.resource("artifact", resource_of(*hash), {"read"}));
    return info_of(*hash);
}

Result<artifacts::GetReply> Artifacts::get(const artifacts::GetRequest& q, Operation& op) {
    auto ref = lent_ref(op);
    if (!ref) return std::unexpected(ref.error());
    if (served(ref->hash)) return read_range(ref->hash, q, op.sender());
    // Fetched from the host that holds it first; then answered.
    auto reply = op.call.defer();
    if (!reply) return std::unexpected(abi::internal);
    obtain(*ref, [this, cap = std::move(*reply), hash = ref->hash, q, reader = op.sender()](std::int32_t status) {
        if (status != abi::ok) {
            (void)ctx_->reply(cap, status);
            return;
        }
        auto r = read_range(hash, q, reader);
        if (r) {
            (void)ctx_->reply(cap, abi::ok, wire::to_msgpack(*r));
        } else {
            (void)ctx_->reply(cap, r.error());
        }
    });
    return artifacts::GetReply{};
}

Result<artifacts::Info> Artifacts::stat(const artifacts::StatRequest&, Operation& op) {
    auto ref = lent_ref(op);
    if (!ref) return std::unexpected(ref.error());
    if (served(ref->hash)) return info_of(ref->hash);
    auto reply = op.call.defer();
    if (!reply) return std::unexpected(abi::internal);
    obtain(*ref, [this, cap = std::move(*reply), hash = ref->hash](std::int32_t status) {
        if (status != abi::ok) {
            (void)ctx_->reply(cap, status);
            return;
        }
        auto info = info_of(hash);
        if (info) {
            (void)ctx_->reply(cap, abi::ok, wire::to_msgpack(*info));
        } else {
            (void)ctx_->reply(cap, info.error());
        }
    });
    return artifacts::Info{};
}

}  // namespace paglets::services::impl
