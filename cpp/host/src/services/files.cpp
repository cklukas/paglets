// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `files` system paglet. Every request acts below the `dir` capability
// the caller lends. Paths are resolved so that nothing outside that
// directory is reached: the capability's directory and the parent of the
// requested entry are resolved canonically (following symbolic links) and
// must lie inside; the entry itself is taken literally for operations on
// entries (stat, list, move, delete) and resolved for operations on content
// (read, write, listing a directory), again inside.

#include "services.hpp"

#include <paglets/glob.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/wire/reflect.hpp>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <system_error>

namespace paglets::services::impl {

namespace {

// UTF-8 strings to paths and back, the same on every platform.
fs::path to_path(std::string_view utf8) {
    return fs::path(std::u8string(reinterpret_cast<const char8_t*>(utf8.data()), utf8.size()));
}

std::string utf8_of(const fs::path& p) {
    const std::u8string s = p.generic_u8string();
    return std::string(reinterpret_cast<const char*>(s.data()), s.size());
}

bool within(const fs::path& base, const fs::path& p) {
    const fs::path r = p.lexically_relative(base);
    return !r.empty() && *r.begin() != "..";
}

std::int32_t error_of(const std::error_code& ec) {
    if (ec == std::errc::no_such_file_or_directory) return abi::not_found;
    if (ec == std::errc::permission_denied || ec == std::errc::operation_not_permitted) return abi::denied;
    if (ec == std::errc::file_exists || ec == std::errc::directory_not_empty) return abi::bad_state;
    if (ec == std::errc::not_a_directory || ec == std::errc::is_a_directory) return abi::invalid_argument;
    return abi::internal;
}

std::int64_t modified_ms(const fs::path& p) {
    std::error_code ec;
    const auto t = fs::last_write_time(p, ec);
    if (ec) return 0;
    const auto sys = std::chrono::clock_cast<std::chrono::system_clock>(t);
    return std::chrono::duration_cast<std::chrono::milliseconds>(sys.time_since_epoch()).count();
}

// An entry without following a final symbolic link.
Result<files::Entry> entry_of(const fs::path& p, std::string rel) {
    std::error_code ec;
    const auto st = fs::symlink_status(p, ec);
    if (ec || !fs::exists(st)) return std::unexpected(ec ? error_of(ec) : abi::not_found);
    files::Entry e;
    e.name = rel.empty() ? std::string() : utf8_of(to_path(rel).filename());
    e.path = std::move(rel);
    switch (st.type()) {
        case fs::file_type::regular: {
            e.type = files::EntryType::file;
            const auto size = fs::file_size(p, ec);
            e.size = ec ? 0 : size;
            e.modified_ms = modified_ms(p);
            break;
        }
        case fs::file_type::directory:
            e.type = files::EntryType::directory;
            e.modified_ms = modified_ms(p);
            break;
        case fs::file_type::symlink: e.type = files::EntryType::symlink; break;  // the target may be outside
        default: e.type = files::EntryType::other; break;
    }
    return e;
}

}  // namespace

Files::Files(std::map<std::string, fs::path> roots, std::function<void()> before_scan_step)
    : roots_(std::move(roots)), before_scan_step_(std::move(before_scan_step)) {}

Result<Files::Resolved> Files::resolve(Operation& op, std::initializer_list<std::string_view> rights,
                                       std::string_view rel, bool mark) const {
    if (op.lent().empty()) return std::unexpected(abi::invalid_argument);
    const runtime::Cap& cap = op.lent().front();
    if (auto c = op.ctx.check(cap, "dir", ""); c != abi::ok) return std::unexpected(c);
    for (auto right : rights) {
        if (auto c = op.ctx.check(cap, "dir", right); c != abi::ok) return std::unexpected(c);
    }
    if (!valid_relative(rel, true)) return std::unexpected(abi::invalid_argument);
    const auto slash = cap.resource.find('/');
    const std::string root = cap.resource.substr(0, slash);
    const std::string cap_rel = slash == std::string::npos ? std::string() : cap.resource.substr(slash + 1);
    auto it = roots_.find(root);
    if (it == roots_.end() || !valid_relative(cap_rel, true)) return std::unexpected(abi::not_found);
    std::error_code ec;
    const fs::path root_path = fs::canonical(it->second, ec);
    if (ec) return std::unexpected(abi::not_found);
    const fs::path base = fs::weakly_canonical(root_path / to_path(cap_rel), ec);
    if (ec || !(base == root_path || within(root_path, base))) return std::unexpected(abi::denied);
    // A paglet that reads content of a root carries its mark for good (data
    // residency, planning/cpp-residency.md).
    if (mark && op.sender() && std::ranges::find(rights, std::string_view("read")) != rights.end()) {
        op.ctx.runtime().mark(op.sender()->id, "root:" + std::string(root));
    }
    return Resolved{base, rel.empty() ? base : base / to_path(rel), std::string(rel), root};
}

Result<Files::Resolved> Files::resolve(Operation& op, std::string_view right, std::string_view rel) const {
    return resolve(op, {right}, rel);
}

namespace {

// The entry itself (a final symbolic link is not followed); its parent must
// resolve inside `base`.
Result<fs::path> entry_path(const fs::path& base, const fs::path& path) {
    if (path == base) return base;
    std::error_code ec;
    const fs::path parent = fs::weakly_canonical(path.parent_path(), ec);
    if (ec) return std::unexpected(error_of(ec));
    if (!(parent == base || within(base, parent))) return std::unexpected(abi::denied);
    return parent / path.filename();
}

// The content the path refers to, following symbolic links, inside `base`.
Result<fs::path> content_path(const fs::path& base, const fs::path& path) {
    std::error_code ec;
    const fs::path full = fs::weakly_canonical(path, ec);
    if (ec) return std::unexpected(error_of(ec));
    if (!(full == base || within(base, full))) return std::unexpected(abi::denied);
    return full;
}

}  // namespace

Result<files::ListReply> Files::list(const files::ListRequest& q, Operation& op) {
    auto r = resolve(op, "read", q.path);
    if (!r) return std::unexpected(r.error());
    auto dir = content_path(r->base, r->path);
    if (!dir) return std::unexpected(dir.error());
    std::error_code ec;
    if (!fs::is_directory(*dir, ec)) return std::unexpected(ec ? error_of(ec) : abi::invalid_argument);
    files::ListReply reply;
    for (const auto& entry : fs::directory_iterator(*dir, fs::directory_options::skip_permission_denied, ec)) {
        auto e = entry_of(entry.path(), join_relative(r->rel, utf8_of(entry.path().filename())));
        if (e) reply.entries.push_back(std::move(*e));
    }
    if (ec) return std::unexpected(error_of(ec));
    std::ranges::sort(reply.entries, {}, &files::Entry::name);
    return reply;
}

Result<files::Entry> Files::stat(const files::StatRequest& q, Operation& op) {
    auto r = resolve(op, "read", q.path);
    if (!r) return std::unexpected(r.error());
    auto p = entry_path(r->base, r->path);
    if (!p) return std::unexpected(p.error());
    return entry_of(*p, r->rel);
}

// Scan cursors live on their host, owned by the requesting paglet. Only a
// background job touches an iterator; cancellation only sets its atomic flag.
struct Files::Scan {
    std::string owner;
    runtime::Cap dir;
    fs::path root_path;
    std::string cap_rel;
    fs::path path;
    std::string rel;
    files::FindRequest query;
    std::optional<fs::recursive_directory_iterator> iterator;
    std::chrono::steady_clock::time_point deadline;
    std::chrono::steady_clock::time_point touched;
    std::atomic<bool> cancelled{false};
    bool busy = false;  // scans_mu_ protects this and the map
    std::uint64_t visited = 0;
    std::uint64_t matched = 0;
};

Files::~Files() { stop(); }

void Files::stop() {
    {
        std::lock_guard lock(scans_mu_);
        scans_stopping_ = true;
        for (auto& [id, s] : scans_) s->cancelled = true;
        scans_.clear();
    }
    scan_jobs_.stop();
}

void Files::paglet_ended(runtime::SystemContext&, const runtime::PagletId& owner) {
    std::lock_guard lock(scans_mu_);
    std::erase_if(scans_, [&](const auto& item) {
        if (item.second->owner != owner) return false;
        item.second->cancelled = true;
        return true;
    });
}

Result<files::ScanStarted> Files::scan_start(const files::ScanRequest& q, Operation& op) {
    if (q.timeout_ms < -1 || q.query.pattern.empty() || q.query.pattern.size() > 256 ||
        std::ranges::count(glob::segments(q.query.pattern), std::string_view("**")) > 4)
        return std::unexpected(abi::invalid_argument);
    if (op.lent().empty()) return std::unexpected(abi::invalid_argument);
    const auto& cap = op.lent().front();
    if (auto c = op.ctx.check(cap, "dir", "read"); c != abi::ok) return std::unexpected(c);
    if (!valid_relative(q.query.path, true)) return std::unexpected(abi::invalid_argument);
    const auto slash = cap.resource.find('/');
    const std::string root = cap.resource.substr(0, slash);
    const std::string cap_rel = slash == std::string::npos ? std::string() : cap.resource.substr(slash + 1);
    const auto root_it = roots_.find(root);
    if (root_it == roots_.end() || !valid_relative(cap_rel, true)) return std::unexpected(abi::not_found);
    if (op.sender()) op.ctx.runtime().mark(op.sender()->id, "root:" + root);
    auto s = std::make_shared<Scan>();
    s->owner = op.sender() ? op.sender()->id : "host";
    s->dir = op.lent().front();
    s->root_path = root_it->second;
    s->cap_rel = cap_rel;
    s->rel = q.query.path;
    s->query = q.query;
    const auto now = std::chrono::steady_clock::now();
    const auto safe_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        (std::chrono::steady_clock::time_point::max() - now) / 2).count();
    s->deadline = q.timeout_ms > 0 ? now + std::chrono::milliseconds(std::min(q.timeout_ms, safe_ms))
                                : std::chrono::steady_clock::time_point::max();
    s->touched = now;
    const std::string id = random_hex(16);
    std::lock_guard lock(scans_mu_);
    if (scans_stopping_) return std::unexpected(abi::gone);
    // Idle cursors own no running work. Reap abandoned cursors on admission;
    // a hard count also bounds resources when callers stop pulling.
    std::erase_if(scans_, [&](const auto& item) {
        auto& old = *item.second;
        if (old.busy || (now < old.deadline && now - old.touched < std::chrono::minutes(1))) return false;
        old.cancelled = true;
        return true;
    });
    const auto owned = std::ranges::count_if(scans_, [&](const auto& item) { return item.second->owner == s->owner; });
    if (scans_.size() + scan_jobs_.pending() >= 64 || owned >= 4) return std::unexpected(abi::quota);
    scans_.emplace(id, std::move(s));
    return files::ScanStarted{id};
}

Result<files::ScanBatch> Files::scan_step(Scan& s, runtime::SystemContext& ctx) {
    const auto began = std::chrono::steady_clock::now();
    auto check = [&]() -> std::int32_t {
        if (s.cancelled) return abi::cancelled;
        if (std::chrono::steady_clock::now() >= s.deadline) return abi::timeout;
        return ctx.check(s.dir, "dir", "read");
    };
    if (auto c = check(); c != abi::ok) return std::unexpected(c);
    if (before_scan_step_) before_scan_step_();  // deterministic slow-I/O tests
    if (auto c = check(); c != abi::ok) return std::unexpected(c);
    std::error_code ec;
    if (!s.iterator) {
        // Even canonicalization can block on a slow filesystem. Do all
        // filesystem calls on the worker, never on a scheduler lane.
        const auto root = fs::canonical(s.root_path, ec);
        if (ec) return std::unexpected(error_of(ec));
        const auto base = fs::weakly_canonical(root / to_path(s.cap_rel), ec);
        if (ec) return std::unexpected(error_of(ec));
        if (!(base == root || within(root, base))) return std::unexpected(abi::denied);
        auto path = content_path(base, base / to_path(s.rel));
        if (!path) return std::unexpected(path.error());
        s.path = *path;
        s.iterator.emplace(s.path, fs::directory_options::skip_permission_denied, ec);
        if (ec) return std::unexpected(error_of(ec));
    }
    const auto pattern = glob::segments(s.query.pattern);
    files::ScanBatch batch;
    std::size_t bytes = 0;
    std::size_t inspected = 0;
    auto& it = *s.iterator;
    while (it != fs::recursive_directory_iterator()) {
        if (auto c = check(); c != abi::ok) return std::unexpected(c);
        const std::string rel = utf8_of(it->path().lexically_relative(s.path));
        ++s.visited;
        ++inspected;
        if (glob::match_segments(pattern, 0, glob::segments(rel), 0)) {
            auto e = entry_of(it->path(), join_relative(s.rel, rel));
            if (e && (!s.query.min_size || e->size >= *s.query.min_size) &&
                (!s.query.max_size || e->size <= *s.query.max_size) &&
                (!s.query.modified_after_ms || e->modified_ms >= *s.query.modified_after_ms) &&
                (!s.query.modified_before_ms || e->modified_ms < *s.query.modified_before_ms)) {
                ++s.matched;
                bytes += e->path.size() + e->name.size() + 128;
                batch.entries.push_back(std::move(*e));
            }
        }
        it.increment(ec);  // directory links are never followed
        if (ec) return std::unexpected(error_of(ec));
        if (batch.entries.size() >= 128 || bytes >= 128 * 1024 || inspected >= 2048 ||
            std::chrono::steady_clock::now() - began >= std::chrono::milliseconds(50)) break;
    }
    batch.completed = s.visited;
    batch.matched = s.matched;
    batch.finished = it == fs::recursive_directory_iterator();
    return batch;
}

Result<files::ScanBatch> Files::scan_next(const files::ScanId& q, Operation& op) {
    std::shared_ptr<Scan> s;
    {
        std::lock_guard lock(scans_mu_);
        auto it = scans_.find(q.id);
        if (it == scans_.end()) return std::unexpected(abi::not_found);
        s = it->second;
        if (s->owner != (op.sender() ? op.sender()->id : "host")) return std::unexpected(abi::denied);
        if (s->busy) return std::unexpected(abi::bad_state);
        s->busy = true;
        s->touched = std::chrono::steady_clock::now();
    }
    auto reply = op.call.defer();
    if (!reply) {
        std::lock_guard lock(scans_mu_);
        s->busy = false;
        return std::unexpected(abi::invalid_argument);
    }
    scan_jobs_.post([this, s, id = q.id, ctx = &op.ctx, reply = *reply](bool stopping) {
        Result<files::ScanBatch> batch = std::unexpected(abi::failed);
        try {
            if (!stopping) batch = scan_step(*s, *ctx);
        } catch (const std::exception&) { /* a failed scan still releases its cursor and answers */ }
        {
            std::lock_guard lock(scans_mu_);
            s->busy = false;
            s->touched = std::chrono::steady_clock::now();
            if (!batch || batch->finished) scans_.erase(id);
        }
        auto delivered = batch ? ctx->reply(reply, abi::ok, wire::to_msgpack(*batch)) : ctx->reply(reply, batch.error());
        if (!delivered) {
            std::lock_guard lock(scans_mu_);
            s->cancelled = true;
            scans_.erase(id);
        }
    });
    return files::ScanBatch{};  // deferred
}

Result<files::ScanCancelled> Files::scan_cancel(const files::ScanId& q, Operation& op) {
    std::lock_guard lock(scans_mu_);
    auto it = scans_.find(q.id);
    if (it == scans_.end()) return files::ScanCancelled{};
    if (it->second->owner != (op.sender() ? op.sender()->id : "host")) return std::unexpected(abi::denied);
    it->second->cancelled = true;
    scans_.erase(it);
    return files::ScanCancelled{true};
}

Result<files::FindReply> Files::find(const files::FindRequest& q, Operation& op) {
    // Compatibility one-reply API. New callers use scan_start/next/cancel
    // through patterns::scan_files so progress and cancellation are available.
    auto started = scan_start(files::ScanRequest{q}, op);
    if (!started) return std::unexpected(started.error());
    std::shared_ptr<Scan> s;
    {
        std::lock_guard lock(scans_mu_);
        s = scans_.at(started->id);
        s->busy = true;
    }
    auto reply = op.call.defer();
    if (!reply) {
        std::lock_guard lock(scans_mu_);
        scans_.erase(started->id);
        return std::unexpected(abi::invalid_argument);
    }
    scan_jobs_.post([this, s, id = started->id, ctx = &op.ctx, reply = *reply, limit = std::clamp(q.limit, 1u, 10000u)](bool stopping) {
        files::FindReply result;
        std::int32_t status = stopping ? abi::failed : abi::ok;
        std::size_t bytes = 0;
        try {
            while (status == abi::ok) {
                auto batch = scan_step(*s, *ctx);
                if (!batch) { status = batch.error(); break; }
                for (auto& e : batch->entries) {
                    if (result.entries.size() >= limit || bytes + e.path.size() + e.name.size() + 128 > 512 * 1024) {
                        result.truncated = true;
                        break;
                    }
                    bytes += e.path.size() + e.name.size() + 128;
                    result.entries.push_back(std::move(e));
                }
                if (result.truncated || batch->finished) break;
            }
            std::ranges::sort(result.entries, {}, &files::Entry::path);
        } catch (const std::exception&) { status = abi::failed; }
        {
            std::lock_guard lock(scans_mu_);
            scans_.erase(id);
        }
        (void)ctx->reply(reply, status, status == abi::ok ? wire::to_msgpack(result) : runtime::Bytes{});
    });
    return files::FindReply{};
}

Result<files::ReadReply> Files::read(const files::ReadRequest& q, Operation& op) {
    auto r = resolve(op, "read", q.path);
    if (!r) return std::unexpected(r.error());
    auto file = content_path(r->base, r->path);
    if (!file) return std::unexpected(file.error());
    std::error_code ec;
    if (!fs::is_regular_file(*file, ec)) return std::unexpected(ec ? error_of(ec) : abi::invalid_argument);
    const std::uint64_t size = fs::file_size(*file, ec);
    if (ec) return std::unexpected(error_of(ec));
    if (q.offset > size) return std::unexpected(abi::invalid_argument);
    std::uint64_t length = std::min<std::uint64_t>(size - q.offset, max_transfer);
    if (q.length) {
        if (*q.length > max_transfer) return std::unexpected(abi::too_large);
        length = std::min(length, *q.length);
    }
    files::ReadReply reply;
    reply.size = size;
    reply.data.resize(static_cast<std::size_t>(length));
    std::ifstream in(*file, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(q.offset));
    if (length > 0 && !in.read(reinterpret_cast<char*>(reply.data.data()), static_cast<std::streamsize>(length))) {
        return std::unexpected(abi::internal);
    }
    reply.eof = q.offset + length >= size;
    return reply;
}

Result<files::Entry> Files::write(const files::WriteRequest& q, Operation& op) {
    if (q.path.empty()) return std::unexpected(abi::invalid_argument);
    if (q.data.size() > max_transfer) return std::unexpected(abi::too_large);
    auto r = resolve(op, {}, q.path);
    if (!r) return std::unexpected(r.error());
    auto target = content_path(r->base, r->path);
    if (!target) return std::unexpected(target.error());
    std::error_code ec;
    const auto st = fs::status(*target, ec);
    const bool exists = fs::exists(st);
    if (exists && !fs::is_regular_file(st)) return std::unexpected(abi::invalid_argument);
    if (exists && q.mode == files::WriteMode::create_new) return std::unexpected(abi::bad_state);
    const runtime::Cap& cap = op.lent().front();
    if (auto c = op.ctx.check(cap, "dir", exists ? "write" : "create"); c != abi::ok) return std::unexpected(c);
    if (q.mode == files::WriteMode::append && exists) {
        std::ofstream out(*target, std::ios::binary | std::ios::app);
        if (!out.write(reinterpret_cast<const char*>(q.data.data()), static_cast<std::streamsize>(q.data.size()))) {
            return std::unexpected(abi::internal);
        }
    } else if (auto w = write_atomically(*target, q.data); !w) {
        return std::unexpected(abi::internal);
    }
    return entry_of(*target, r->rel);
}

Result<files::Entry> Files::mkdir(const files::MkdirRequest& q, Operation& op) {
    if (q.path.empty()) return std::unexpected(abi::invalid_argument);
    auto r = resolve(op, "create", q.path);
    if (!r) return std::unexpected(r.error());
    auto target = content_path(r->base, r->path);
    if (!target) return std::unexpected(target.error());
    std::error_code ec;
    if (fs::exists(*target, ec)) return std::unexpected(abi::bad_state);
    if (q.parents) {
        fs::create_directories(*target, ec);
    } else {
        fs::create_directory(*target, ec);
    }
    if (ec) return std::unexpected(error_of(ec));
    return entry_of(*target, r->rel);
}

Result<files::Entry> Files::move(const files::MoveRequest& q, Operation& op) {
    if (q.from.empty() || q.to.empty()) return std::unexpected(abi::invalid_argument);
    auto from = resolve(op, "delete", q.from);
    if (!from) return std::unexpected(from.error());
    auto to = resolve(op, "create", q.to);
    if (!to) return std::unexpected(to.error());
    auto source = entry_path(from->base, from->path);
    if (!source) return std::unexpected(source.error());
    auto target = entry_path(to->base, to->path);
    if (!target) return std::unexpected(target.error());
    std::error_code ec;
    if (!fs::exists(fs::symlink_status(*source, ec))) return std::unexpected(abi::not_found);
    if (fs::exists(fs::symlink_status(*target, ec))) return std::unexpected(abi::bad_state);
    // A directory cannot move into itself.
    if (within(*source, *target)) return std::unexpected(abi::invalid_argument);
    fs::rename(*source, *target, ec);
    if (ec) return std::unexpected(error_of(ec));
    return entry_of(*target, to->rel);
}

Result<files::DeleteReply> Files::remove(const files::DeleteRequest& q, Operation& op) {
    if (q.path.empty()) return std::unexpected(abi::invalid_argument);  // never the lent directory itself
    auto r = resolve(op, "delete", q.path);
    if (!r) return std::unexpected(r.error());
    auto target = entry_path(r->base, r->path);
    if (!target) return std::unexpected(target.error());
    std::error_code ec;
    const auto st = fs::symlink_status(*target, ec);
    if (!fs::exists(st)) return std::unexpected(abi::not_found);
    files::DeleteReply reply;
    if (fs::is_directory(st) && q.recursive) {
        const auto n = fs::remove_all(*target, ec);  // does not follow links inside
        if (ec) return std::unexpected(error_of(ec));
        reply.removed = n;
    } else {
        if (!fs::remove(*target, ec)) return std::unexpected(ec ? error_of(ec) : abi::not_found);
        reply.removed = 1;
    }
    return reply;
}

// -- content for other hosts (planning/cpp-content-exchange.md) ---------------

Result<files::Hashed> Files::hash(const files::HashRequest& q, Operation& op) {
    auto r = resolve(op, "read", q.path);
    if (!r) return std::unexpected(r.error());
    auto file = content_path(r->base, r->path);
    if (!file) return std::unexpected(file.error());
    std::error_code ec;
    if (!fs::is_regular_file(*file, ec)) return std::unexpected(ec ? error_of(ec) : abi::invalid_argument);
    auto reply = op.call.defer();
    if (!reply) return std::unexpected(abi::internal);
    runtime::SystemContext* ctx = &op.ctx;
    // Hashing a large file blocks: not on a scheduler lane.
    auto job = [ctx, cap = std::move(*reply), file = *file](bool stopping) {
        if (stopping) {
            (void)ctx->reply(cap, abi::failed);
            return;
        }
        auto hashed = Artifacts::hash_file(file);
        if (!hashed) {
            (void)ctx->reply(cap, hashed.error());
        } else {
            (void)ctx->reply(cap, abi::ok, wire::to_msgpack(*hashed));
        }
    };
    if (artifacts_) {
        artifacts_->jobs().post(std::move(job));
    } else {
        job(false);
    }
    return files::Hashed{};
}

Result<files::Shared> Files::share(const files::ShareRequest& q, Operation& op) {
    if (!artifacts_) return std::unexpected(abi::unsupported);
    // Nothing is read into the paglet: the content carries the root's mark
    // instead, and leaves this host only where the root's content may go.
    auto r = resolve(op, {"read"}, q.path, false);
    if (!r) return std::unexpected(r.error());
    auto file = content_path(r->base, r->path);
    if (!file) return std::unexpected(file.error());
    std::error_code ec;
    if (!fs::is_regular_file(*file, ec)) return std::unexpected(ec ? error_of(ec) : abi::invalid_argument);
    auto reply = op.call.defer();
    if (!reply) return std::unexpected(abi::internal);
    runtime::SystemContext* ctx = &op.ctx;
    // Hashing a large file blocks: not on a scheduler lane.
    artifacts_->jobs().post([artifacts = artifacts_, ctx, cap = std::move(*reply), file = *file, media = q.media_type,
                             root = r->root](bool stopping) {
        if (stopping) {
            (void)ctx->reply(cap, abi::failed);
            return;
        }
        auto shared = artifacts->share_file(file, media, {"root:" + root});
        if (!shared) {
            (void)ctx->reply(cap, shared.error());
            return;
        }
        (void)ctx->reply(cap, abi::ok, wire::to_msgpack(*shared), {artifacts->capability(shared->hash)});
    });
    return files::Shared{};
}

Result<files::Entry> Files::save(const files::SaveRequest& q, Operation& op) {
    if (!artifacts_) return std::unexpected(abi::unsupported);
    if (q.path.empty() || op.lent().size() < 2) return std::unexpected(abi::invalid_argument);
    const runtime::Cap& content = op.lent()[1];
    if (!Artifacts::readable(content)) return std::unexpected(abi::bad_handle);
    auto ref = Artifacts::parse_resource(content.resource);
    auto r = resolve(op, {}, q.path);
    if (!r) return std::unexpected(r.error());
    auto target = content_path(r->base, r->path);
    if (!target) return std::unexpected(target.error());
    std::error_code ec;
    const auto st = fs::status(*target, ec);
    const bool exists = fs::exists(st);
    if (exists && !fs::is_regular_file(st)) return std::unexpected(abi::invalid_argument);
    if (exists && !q.replace) return std::unexpected(abi::bad_state);
    if (!fs::is_directory(target->parent_path(), ec)) return std::unexpected(abi::not_found);
    if (auto c = op.ctx.check(op.lent().front(), "dir", exists ? "write" : "create"); c != abi::ok) {
        return std::unexpected(c);
    }
    auto reply = op.call.defer();
    if (!reply) return std::unexpected(abi::internal);
    runtime::SystemContext* ctx = &op.ctx;
    // Written beside the target and renamed when complete and verified: the
    // file appears whole or not at all.
    const fs::path tmp = target->parent_path() / (".paglets-save-" + random_hex(8));
    artifacts_->copy_to(*ref, tmp,
                        [ctx, cap = std::move(*reply), tmp, target = *target, rel = r->rel,
                         replace = q.replace](std::int32_t status) {
                            std::error_code err;
                            if (status == abi::ok && !replace && fs::exists(target, err)) status = abi::bad_state;
                            if (status == abi::ok) {
                                fs::rename(tmp, target, err);
                                if (err) status = error_of(err);
                            }
                            if (status != abi::ok) {
                                fs::remove(tmp, err);
                                (void)ctx->reply(cap, status);
                                return;
                            }
                            auto entry = entry_of(target, rel);
                            if (entry) {
                                (void)ctx->reply(cap, abi::ok, wire::to_msgpack(*entry));
                            } else {
                                (void)ctx->reply(cap, entry.error());
                            }
                        });
    return files::Entry{};
}

}  // namespace paglets::services::impl
