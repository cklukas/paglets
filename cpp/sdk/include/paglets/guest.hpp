// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Guest SDK for paglets compiled to wasm32-wasi (M0 spike, ABI v0).
//
// A paglet implements paglets::guest::handle_message(). The SDK provides the
// exports the host calls (paglets_alloc, paglets_handle, paglets_free,
// paglets_abi_version) and wraps the host imports.
//
// Messages of the spike ABI are MessagePack arrays [name, body]; replies are
// [status, body] with status "ok" or "error".

#pragma once

#include <paglets/msgpack.hpp>

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace paglets::guest {

inline constexpr std::uint32_t abi_version = 0;

// Implemented by the paglet: handles one request and returns the reply bytes.
std::vector<std::uint8_t> handle_message(std::span<const std::uint8_t> request);

// Writes a line to the host log.
void log(std::string_view text);

// Reply helpers for the [status, body] convention.
template <class T>
std::vector<std::uint8_t> reply_ok(const T& body) {
    msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("ok");
    msgpack::write_value(w, body);
    return w.take();
}

std::vector<std::uint8_t> reply_error(std::string_view message);

}  // namespace paglets::guest
