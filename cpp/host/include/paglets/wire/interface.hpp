// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Interface descriptions of paglet modules (WP33, docs/interface-descriptions.md).
//
// A module may carry a Wasm custom section `paglets.interface` holding a JSON
// description of the messages its paglets understand and of the arguments
// they are started with:
//
//     {"interface": 1, "name": "counter", "summary": "…",
//      "launch": {"type": "none"},
//      "messages": [{"name": "increment", "kind": "call", "summary": "…",
//                    "request": "Increment", "reply": "Status"}],
//      "types": [ … records and enums as in schema descriptors … ]}
//
// The section is covered by the module hash (and by its signatures), and a
// host reads it without running the module. Tools (paglets-web, the CLI)
// build forms and commands from it.

#pragma once

#include <paglets/wire/json.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::wire::interface {

inline constexpr std::string_view section_name = "paglets.interface";
inline constexpr std::int64_t format_version = 1;

// -- custom sections -----------------------------------------------------------

// The payload of the first custom section named `name`; nullopt when the
// module has none. Fails on bytes that are not a Wasm module.
std::expected<std::optional<std::string>, std::string> read_custom_section(std::span<const std::uint8_t> wasm,
                                                                           std::string_view name);

// The module with its custom sections named `name` replaced by one holding
// `payload` (appended at the end).
std::expected<std::vector<std::uint8_t>, std::string> write_custom_section(std::span<const std::uint8_t> wasm,
                                                                           std::string_view name,
                                                                           std::string_view payload);

// -- descriptions --------------------------------------------------------------

// --8<-- [start:api]
// Checks a description: the format version, message names and kinds, and
// that every type it names is a built-in type or one of its `types`.
std::expected<void, std::string> validate(const Json& description);

// Parses and validates a description given as JSON text.
std::expected<Json, std::string> parse(std::string_view text);

// Overlapping record/enum definitions must have the same wire shape.
// Description-only documentation and defaults may differ.
std::expected<void, std::string> validate_schema_types(const Json& description, const Json& schema);

// Adds the records and enums of a schema descriptor (wire/schema.hpp,
// <name>.schema.json) that the description does not define itself.
Json merge_schema_types(Json description, const Json& schema);

// A description made from a schema descriptor's services: one `call`
// message per operation.
Json from_schema(const Json& schema, std::string_view name);

// The interface of a module's bytes: the custom section, parsed; nullopt
// when the module has none.
std::expected<std::optional<Json>, std::string> of_module(std::span<const std::uint8_t> wasm);
// --8<-- [end:api]

// True for the built-in type names (bool, i8 … u64, f32, f64, string, bytes,
// none, any) and list<…>/optional<…> of known types; `known` holds the
// names defined in `types`.
bool is_known_type(std::string_view type, const std::vector<std::string>& known);

}  // namespace paglets::wire::interface
