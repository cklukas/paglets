// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "services.hpp"

#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>

#include <array>
#include <chrono>

namespace paglets::services::impl {

bool valid_relative(std::string_view path, bool allow_empty) {
    if (path.empty()) return allow_empty;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = std::min(path.find('/', start), path.size());
        const std::string_view seg = path.substr(start, end - start);
        if (seg.empty() || seg == "." || seg == ".." || seg.find_first_of("\\:") != std::string_view::npos ||
            seg.find('\0') != std::string_view::npos) {
            return false;
        }
        start = end + 1;
    }
    return true;
}

std::string join_relative(std::string_view a, std::string_view b) {
    if (a.empty()) return std::string(b);
    if (b.empty()) return std::string(a);
    return std::string(a) + "/" + std::string(b);
}

bool valid_name(std::string_view name, std::size_t max, bool upper) {
    if (name.empty() || name.size() > max) return false;
    for (char c : name) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' ||
                        (upper && c >= 'A' && c <= 'Z');
        if (!ok) return false;
    }
    return true;
}

std::string random_hex(std::size_t bytes) {
    std::vector<std::uint8_t> buf(bytes);
    wasm::fill_random(buf);
    return to_hex(buf);
}

std::int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::expected<void, std::string> write_atomically(const fs::path& path, std::span<const std::uint8_t> data) {
    const fs::path temp = path.string() + ".tmp-" + random_hex(4);
    if (auto w = wasm::write_file(temp.string(), data); !w) return w;
    std::error_code ec;
    fs::rename(temp, path, ec);
    if (ec) {
        fs::remove(temp, ec);
        return std::unexpected("cannot write " + path.string());
    }
    return {};
}

std::expected<std::vector<std::uint8_t>, std::string> read_all(const fs::path& path) {
    return wasm::read_file(path.string());
}

std::optional<abi::SenderRecord> caller_of(const runtime::ServiceCall& call) {
    const auto& sender = call.sender();
    if (!sender || sender->id == "host" || sender->id.empty()) return std::nullopt;
    return sender;
}

}  // namespace paglets::services::impl
