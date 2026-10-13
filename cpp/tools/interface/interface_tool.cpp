// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Interface tool (WP33, docs/interface-descriptions.md): puts an interface
// description into a module's custom section `paglets.interface`, or shows
// the one a module carries. paglets_add_module(... INTERFACE <file>) runs it
// after linking; contract guests get a description made from their
// services. It links only paglets::wire, so projects using an installed
// paglets package compile it like the schema generator.
//
// Usage:
//   paglets-interface embed MODULE.wasm OUT.wasm [--description FILE.json]
//                     [--schema FILE.schema.json] [--name NAME]
//   paglets-interface show MODULE.wasm

#include <paglets/wire/interface.hpp>
#include <paglets/wire/json.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

namespace {

namespace wi = paglets::wire::interface;
using paglets::wire::Json;

int usage() {
    std::cerr << "usage: paglets-interface embed MODULE.wasm OUT.wasm [--description FILE.json]\n"
                 "                         [--schema FILE.schema.json] [--name NAME]\n"
                 "       paglets-interface show MODULE.wasm\n"
                 "embed writes OUT.wasm with the description (types of the schema merged in); without\n"
                 "--description, a schema with services gives one call message per operation, and a\n"
                 "schema without services leaves the module as it is.\n";
    return 2;
}

int fail(const std::string& message) {
    std::cerr << "paglets-interface: " << message << "\n";
    return 1;
}

std::optional<std::string> read_text(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::nullopt;
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

bool write_bytes(const std::string& path, const std::vector<std::uint8_t>& bytes) {
    // Next to the target, then renamed: the input may be the output.
    const std::string temp = path + ".tmp";
    {
        std::ofstream out(temp, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!out) return false;
    }
    std::error_code ec;
    std::filesystem::rename(temp, path, ec);
    return !ec;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) return usage();
    const std::string command = argv[1];
    if (command == "show") {
        if (argc != 3) return usage();
        auto text = read_text(argv[2]);
        if (!text) return fail(std::string("cannot read ") + argv[2]);
        const std::vector<std::uint8_t> bytes(text->begin(), text->end());
        auto d = wi::of_module(bytes);
        if (!d) return fail(d.error());
        if (!*d) return fail("the module has no interface description");
        std::cout << (*d)->dump() << "\n";
        return 0;
    }
    if (command != "embed" || argc < 4) return usage();
    const std::string input = argv[2];
    const std::string output = argv[3];
    std::optional<std::string> description_file, schema_file, name;
    for (int i = 4; i < argc; ++i) {
        const std::string a = argv[i];
        if (i + 1 >= argc) return usage();
        if (a == "--description") {
            description_file = argv[++i];
        } else if (a == "--schema") {
            schema_file = argv[++i];
        } else if (a == "--name") {
            name = argv[++i];
        } else {
            return usage();
        }
    }

    auto module_text = read_text(input);
    if (!module_text) return fail("cannot read " + input);
    const std::vector<std::uint8_t> module(module_text->begin(), module_text->end());

    std::optional<Json> schema;
    if (schema_file) {
        auto text = read_text(*schema_file);
        if (!text) return fail("cannot read " + *schema_file);
        schema = Json::parse(*text);
        if (!schema || !schema->is_object()) return fail(*schema_file + " is not a schema descriptor");
    }

    std::optional<Json> description;
    if (description_file) {
        auto text = read_text(*description_file);
        if (!text) return fail("cannot read " + *description_file);
        description = Json::parse(*text);
        if (!description) return fail(*description_file + " is not valid JSON");
        if (schema) description = wi::merge_schema_types(std::move(*description), *schema);
        if (name && description->find("name") == nullptr && description->is_object()) {
            Json::Object members;
            for (std::size_t i = 0; i < description->keys().size(); ++i) {
                members.emplace_back(description->keys()[i], description->as_array()[i]);
            }
            members.emplace_back("name", Json(*name));
            description = Json(std::move(members));
        }
    } else if (schema) {
        const Json* services = schema->find("services");
        if (services != nullptr && services->is_array() && !services->as_array().empty()) {
            description = wi::from_schema(*schema, name.value_or(""));
        }
    }

    if (!description) {
        // Nothing to describe: the module stays as it is.
        if (input != output && !write_bytes(output, module)) return fail("cannot write " + output);
        return 0;
    }
    if (auto ok = wi::validate(*description); !ok) {
        return fail((description_file ? *description_file : input) + ": " + ok.error());
    }
    auto described = wi::write_custom_section(module, wi::section_name, description->dump());
    if (!described) return fail(input + ": " + described.error());
    if (!write_bytes(output, *described)) return fail("cannot write " + output);
    return 0;
}
