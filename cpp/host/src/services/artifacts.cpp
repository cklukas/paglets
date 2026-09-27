// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `artifacts` system paglet: a content-addressed blob store. Files are
// named by the SHA-256 of their content; a small side file keeps the media
// type and the creation time.

#include "services.hpp"

#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <fstream>

namespace paglets::services::impl {

namespace {

struct Meta {
    std::string media_type;
    std::int64_t created_ms = 0;
};

bool valid_hash(std::string_view hash) {
    return hash.size() == 64 && hash.find_first_not_of("0123456789abcdef") == std::string_view::npos;
}

}  // namespace

Artifacts::Artifacts(fs::path root, bool temporary, std::uint64_t limit)
    : root_(std::move(root)), temporary_(temporary), limit_(limit) {
    std::error_code ec;
    fs::create_directories(root_, ec);
}

Artifacts::~Artifacts() {
    if (temporary_) {
        std::error_code ec;
        fs::remove_all(root_, ec);
    }
}

Result<artifacts::Info> Artifacts::info_of(std::string_view hash) const {
    const fs::path data = root_ / std::string(hash);
    std::error_code ec;
    const auto size = fs::file_size(data, ec);
    if (ec) return std::unexpected(abi::not_found);
    Meta meta;
    if (auto bytes = read_all(root_ / (std::string(hash) + ".meta"))) (void)wire::from_msgpack(*bytes, meta);
    return artifacts::Info{std::string(hash), size, meta.media_type, meta.created_ms};
}

Result<std::string> Artifacts::lent_hash(Operation& op) const {
    if (op.lent().empty()) return std::unexpected(abi::invalid_argument);
    const runtime::Cap& cap = op.lent().front();
    if (auto c = op.ctx.check(cap, "artifact", "read"); c != abi::ok) return std::unexpected(c);
    if (!valid_hash(cap.resource)) return std::unexpected(abi::bad_handle);
    return cap.resource;
}

Result<artifacts::Info> Artifacts::put(const artifacts::PutRequest& q, Operation& op) {
    if (!caller_of(op.call)) return std::unexpected(abi::denied);
    if (q.data.size() > Files::max_transfer) return std::unexpected(abi::too_large);
    if (q.media_type.size() > 128) return std::unexpected(abi::invalid_argument);
    const std::string hash = to_hex(sha256(q.data));
    {
        std::lock_guard lock(mu_);
        const fs::path data = root_ / hash;
        std::error_code ec;
        if (!fs::exists(data, ec)) {
            std::uint64_t used = 0;
            for (const auto& entry : fs::directory_iterator(root_, ec)) {
                const auto size = entry.file_size(ec);
                if (!ec) used += size;
            }
            if (used + q.data.size() > limit_) return std::unexpected(abi::quota);
            if (!write_atomically(root_ / (hash + ".meta"), wire::to_msgpack(Meta{q.media_type, now_ms()})) ||
                !write_atomically(data, q.data)) {
                return std::unexpected(abi::internal);
            }
        }
    }
    op.give.push_back(op.ctx.resource("artifact", hash, {"read"}));
    return info_of(hash);
}

Result<artifacts::GetReply> Artifacts::get(const artifacts::GetRequest& q, Operation& op) {
    auto hash = lent_hash(op);
    if (!hash) return std::unexpected(hash.error());
    const fs::path data = root_ / *hash;
    std::error_code ec;
    const std::uint64_t size = fs::file_size(data, ec);
    if (ec) return std::unexpected(abi::not_found);
    if (q.offset > size) return std::unexpected(abi::invalid_argument);
    std::uint64_t length = std::min<std::uint64_t>(size - q.offset, Files::max_transfer);
    if (q.length) {
        if (*q.length > Files::max_transfer) return std::unexpected(abi::too_large);
        length = std::min(length, *q.length);
    }
    artifacts::GetReply reply;
    reply.size = size;
    reply.data.resize(static_cast<std::size_t>(length));
    std::ifstream in(data, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(q.offset));
    if (length > 0 && !in.read(reinterpret_cast<char*>(reply.data.data()), static_cast<std::streamsize>(length))) {
        return std::unexpected(abi::internal);
    }
    reply.eof = q.offset + length >= size;
    return reply;
}

Result<artifacts::Info> Artifacts::stat(const artifacts::StatRequest&, Operation& op) {
    auto hash = lent_hash(op);
    if (!hash) return std::unexpected(hash.error());
    return info_of(*hash);
}

}  // namespace paglets::services::impl
