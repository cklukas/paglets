// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <charconv>
#include <concepts>
#include <optional>
#include <string_view>

namespace paglets::cli {

// Consume the whole decimal argument and check its destination type before
// any command can open files, start a host or send a request.
template <std::integral T>
std::optional<T> decimal_integer(std::string_view text) {
    T value{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc() || end != text.data() + text.size()) return std::nullopt;
    return value;
}

}  // namespace paglets::cli
