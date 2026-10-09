// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `artifacts` system paglet: a content-addressed blob store. Files are
// named by the SHA-256 of their content; a small side file keeps the media
// type, the creation time and the data residency marks of the paglets that
// stored it (planning/cpp-residency.md): whoever reads it gets them too.

#include "services.hpp"

#include <paglets/runtime/runtime.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <fstream>

namespace paglets::services::impl {

namespace {

struct Meta {
    std::string media_type;
    std::int64_t created_ms = 0;
    std::vector<std::string> marks;
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
    auto hash = store(q.data, q.media_type, op.ctx.runtime().marks(op.sender()->id));
    if (!hash) return std::unexpected(hash.error());
    op.give.push_back(op.ctx.resource("artifact", *hash, {"read"}));
    return info_of(*hash);
}

Result<std::string> Artifacts::store(std::span<const std::uint8_t> content, std::string_view media_type,
                                     const std::vector<std::string>& marks) {
    if (media_type.size() > 128) return std::unexpected(abi::invalid_argument);
    const std::string hash = to_hex(sha256(content));
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
            std::uint64_t used = 0;
            for (const auto& entry : fs::directory_iterator(root_, ec)) {
                const auto size = entry.file_size(ec);
                if (!ec) used += size;
            }
            if (used + content.size() > limit_) return std::unexpected(abi::quota);
            if (!write_atomically(root_ / (hash + ".meta"),
                                  wire::to_msgpack(Meta{std::string(media_type), now_ms(), marks})) ||
                !write_atomically(data, content)) {
                return std::unexpected(abi::internal);
            }
        }
    }
    return hash;
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
    // The reader carries what the writers carried.
    if (op.sender()) {
        Meta meta;
        if (auto bytes = read_all(root_ / (*hash + ".meta"))) (void)wire::from_msgpack(*bytes, meta);
        for (auto& m : meta.marks) op.ctx.runtime().mark(op.sender()->id, std::move(m));
    }
    return reply;
}

Result<artifacts::Info> Artifacts::stat(const artifacts::StatRequest&, Operation& op) {
    auto hash = lent_hash(op);
    if (!hash) return std::unexpected(hash.error());
    return info_of(*hash);
}

}  // namespace paglets::services::impl
