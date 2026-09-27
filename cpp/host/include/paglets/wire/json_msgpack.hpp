// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Schema-less conversion between JSON and MessagePack, for tools that pass
// message bodies given on a command line and print replies. Binary strings
// become arrays of numbers in JSON.

#pragma once

#include <paglets/wire/json.hpp>

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace paglets::wire {

std::vector<std::uint8_t> json_to_msgpack(const Json& value);
std::optional<Json> msgpack_to_json(std::span<const std::uint8_t> bytes);

}  // namespace paglets::wire
