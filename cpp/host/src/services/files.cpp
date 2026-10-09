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

Files::Files(std::map<std::string, fs::path> roots) : roots_(std::move(roots)) {}

Result<Files::Resolved> Files::resolve(Operation& op, std::initializer_list<std::string_view> rights,
                                       std::string_view rel) const {
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
    if (op.sender() && std::ranges::find(rights, std::string_view("read")) != rights.end()) {
        op.ctx.runtime().mark(op.sender()->id, "root:" + std::string(root));
    }
    return Resolved{base, rel.empty() ? base : base / to_path(rel), std::string(rel)};
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

Result<files::FindReply> Files::find(const files::FindRequest& q, Operation& op) {
    auto r = resolve(op, "read", q.path);
    if (!r) return std::unexpected(r.error());
    if (q.pattern.empty() || q.pattern.size() > 256) return std::unexpected(abi::invalid_argument);
    auto dir = content_path(r->base, r->path);
    if (!dir) return std::unexpected(dir.error());
    std::error_code ec;
    if (!fs::is_directory(*dir, ec)) return std::unexpected(ec ? error_of(ec) : abi::invalid_argument);
    const auto pattern = glob::segments(q.pattern);
    if (std::ranges::count(pattern, std::string_view("**")) > 4) return std::unexpected(abi::invalid_argument);
    const std::size_t limit = std::clamp<std::size_t>(q.limit, 1, 10000);
    files::FindReply reply;
    // Directory links are not followed (the default of the iterator).
    fs::recursive_directory_iterator it(*dir, fs::directory_options::skip_permission_denied, ec);
    for (; !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
        const std::string rel = utf8_of(it->path().lexically_relative(*dir));
        if (!glob::match_segments(pattern, 0, glob::segments(rel), 0)) continue;
        auto e = entry_of(it->path(), join_relative(r->rel, rel));
        if (!e) continue;
        if (q.min_size && e->size < *q.min_size) continue;
        if (q.max_size && e->size > *q.max_size) continue;
        if (q.modified_after_ms && e->modified_ms < *q.modified_after_ms) continue;
        if (q.modified_before_ms && e->modified_ms >= *q.modified_before_ms) continue;
        if (reply.entries.size() == limit) {
            reply.truncated = true;
            break;
        }
        reply.entries.push_back(std::move(*e));
    }
    std::ranges::sort(reply.entries, {}, &files::Entry::path);
    return reply;
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

}  // namespace paglets::services::impl
