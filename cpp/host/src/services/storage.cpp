// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The `storage` system paglet: key/value storage of the calling paglet in
// its storage directory (Runtime::storage_dir; without a state directory,
// its scratch directory). Keys become hex file names, so case-insensitive
// file systems (macOS, Windows) keep keys that differ only in case apart.

#include "services.hpp"

#include <paglets/sha256.hpp>

#include <algorithm>

namespace paglets::services::impl {

namespace {

constexpr std::string_view values_dir = "kv";

bool valid_key(std::string_view key) {
    return valid_name(key, 128, true);
}

std::string file_name(std::string_view key) {
    return to_hex(std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(key.data()), key.size()));
}

std::optional<std::string> key_of(const std::string& name) {
    if (name.size() % 2 != 0 || name.empty()) return std::nullopt;
    std::string key;
    for (std::size_t i = 0; i < name.size(); i += 2) {
        auto hex = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            return -1;
        };
        const int hi = hex(name[i]);
        const int lo = hex(name[i + 1]);
        if (hi < 0 || lo < 0) return std::nullopt;
        key += static_cast<char>(hi * 16 + lo);
    }
    if (!valid_key(key)) return std::nullopt;
    return key;
}

}  // namespace

Result<fs::path> Storage::space(Operation& op) const {
    auto caller = caller_of(op.call);
    if (!caller) return std::unexpected(abi::denied);
    auto dir = op.ctx.runtime().storage_dir(caller->id);
    if (!dir && dir.error() == abi::unsupported) dir = op.ctx.runtime().scratch_dir(caller->id);
    if (!dir) return std::unexpected(dir.error());
    const fs::path values = *dir / values_dir;
    std::error_code ec;
    fs::create_directories(values, ec);
    if (ec) return std::unexpected(abi::internal);
    return values;
}

storage::Usage Storage::usage(const fs::path& space) const {
    storage::Usage u;
    u.quota = quota_;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(space, ec)) {
        if (!key_of(entry.path().filename().string())) continue;
        const auto size = entry.file_size(ec);
        if (!ec) u.used += size;
        ++u.keys;
    }
    return u;
}

Result<storage::GetReply> Storage::get(const storage::GetRequest& q, Operation& op) {
    if (!valid_key(q.key)) return std::unexpected(abi::invalid_argument);
    auto dir = space(op);
    if (!dir) return std::unexpected(dir.error());
    const fs::path file = *dir / file_name(q.key);
    std::error_code ec;
    if (!fs::exists(file, ec)) return storage::GetReply{false, {}};
    auto data = read_all(file);
    if (!data) return std::unexpected(abi::internal);
    return storage::GetReply{true, std::move(*data)};
}

Result<storage::Usage> Storage::put(const storage::PutRequest& q, Operation& op) {
    if (!valid_key(q.key)) return std::unexpected(abi::invalid_argument);
    auto dir = space(op);
    if (!dir) return std::unexpected(dir.error());
    const fs::path file = *dir / file_name(q.key);
    auto u = usage(*dir);
    std::error_code ec;
    const std::uint64_t old = fs::exists(file, ec) ? fs::file_size(file, ec) : 0;
    if (u.used - old + q.value.size() > quota_) return std::unexpected(abi::quota);
    if (auto w = write_atomically(file, q.value); !w) return std::unexpected(abi::internal);
    return usage(*dir);
}

Result<storage::DeleteReply> Storage::remove(const storage::DeleteRequest& q, Operation& op) {
    if (!valid_key(q.key)) return std::unexpected(abi::invalid_argument);
    auto dir = space(op);
    if (!dir) return std::unexpected(dir.error());
    std::error_code ec;
    return storage::DeleteReply{fs::remove(*dir / file_name(q.key), ec)};
}

Result<storage::ListReply> Storage::list(const storage::ListRequest& q, Operation& op) {
    auto dir = space(op);
    if (!dir) return std::unexpected(dir.error());
    storage::ListReply reply;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(*dir, ec)) {
        auto key = key_of(entry.path().filename().string());
        if (key && key->starts_with(q.prefix)) reply.keys.push_back(std::move(*key));
    }
    std::ranges::sort(reply.keys);
    reply.usage = usage(*dir);
    return reply;
}

}  // namespace paglets::services::impl
