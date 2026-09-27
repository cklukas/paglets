// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/module_trust.hpp>

#include <algorithm>

namespace paglets::mesh {

namespace {

constexpr std::string_view trust_classes[] = {"roaming", "resident", "system"};

bool valid_class(std::string_view c) {
    return std::ranges::find(trust_classes, c) != std::end(trust_classes);
}

std::optional<std::array<std::uint8_t, 32>> fixed32(const Value& v) {
    const Bytes* b = v.as_bin();
    if (b == nullptr || b->size() != 32) return std::nullopt;
    std::array<std::uint8_t, 32> out{};
    std::copy(b->begin(), b->end(), out.begin());
    return out;
}

// A list of 32-byte values (keys or hashes); absent is an empty list.
std::optional<std::vector<std::array<std::uint8_t, 32>>> fixed32_list(const Fields& f, std::string_view key) {
    std::vector<std::array<std::uint8_t, 32>> out;
    const Value* v = f.get(key);
    if (v == nullptr) return out;
    const Array* a = v->as_array();
    if (a == nullptr) return std::nullopt;
    for (const auto& item : *a) {
        auto x = fixed32(item);
        if (!x) return std::nullopt;
        out.push_back(*x);
    }
    return out;
}

template <class T>
Value bins_value(const std::vector<T>& list) {
    Array a;
    for (const auto& x : list) a.push_back(Value::bin(x));
    return Value(std::move(a));
}

}  // namespace

std::string_view to_string(RoamingModules r) {
    return r == RoamingModules::trusted ? "trusted" : "any";
}

std::optional<RoamingModules> parse_roaming_modules(std::string_view text) {
    if (text == "any") return RoamingModules::any;
    if (text == "trusted") return RoamingModules::trusted;
    return std::nullopt;
}

std::optional<ModuleTrust> parse_module_trust(const Record& r) {
    const Fields f = r.fields();
    auto name = f.str("name");
    auto signers = fixed32_list(f, "signers");
    auto modules = fixed32_list(f, "modules");
    auto classes = f.strings("classes");
    if (!name || !signers || !modules || !classes) return std::nullopt;
    if (signers->empty() && modules->empty()) return std::nullopt;
    if (classes->empty() || !std::ranges::all_of(*classes, valid_class)) return std::nullopt;
    ModuleTrust t;
    t.id = r.id();
    t.name = std::move(*name);
    t.signers = std::move(*signers);
    t.modules = std::move(*modules);
    t.classes = std::move(*classes);
    return t;
}

std::optional<ModuleSignature> parse_module_signature(const Record& r) {
    const Fields f = r.fields();
    auto module = f.fixed<32>("module");
    auto signer = f.fixed<32>("signer");
    auto name = f.str("name");
    std::optional<std::string> version = std::string();
    if (f.get("version") != nullptr) version = f.str("version");
    if (!module || !signer || !name || !version) return std::nullopt;
    ModuleSignature s;
    s.id = r.id();
    s.module = *module;
    s.signer = *signer;
    s.name = std::move(*name);
    s.version = std::move(*version);
    s.time = r.time();
    return s;
}

std::vector<PublicKey> module_signers(const LedgerState& state, const Digest& module) {
    std::vector<PublicKey> out;
    for (const auto& s : state.module_signatures) {
        if (s.module == module && !state.revoked_keys.contains(s.signer)) out.push_back(s.signer);
    }
    std::ranges::sort(out);
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

ModuleVerdict check_module(const LedgerState& state, const Digest& module, std::string_view trust_class) {
    if (!valid_class(trust_class)) return {false, std::nullopt, "unknown trust class " + std::string(trust_class)};
    if (state.revoked_modules.contains(module)) return {false, std::nullopt, "module revoked"};
    const auto signers = module_signers(state, module);
    for (const auto& t : state.module_trust) {
        if (std::ranges::find(t.classes, trust_class) == t.classes.end()) continue;
        const bool by_hash = std::ranges::find(t.modules, module) != t.modules.end();
        const bool by_signer =
            std::ranges::any_of(t.signers, [&](const PublicKey& k) { return std::ranges::binary_search(signers, k); });
        if (by_hash || by_signer) return {true, t.id, "trusted by '" + t.name + "'"};
    }
    if (trust_class == "roaming" && state.roaming_modules == RoamingModules::any) {
        return {true, std::nullopt, "roaming modules need no trust in this mesh"};
    }
    return {false, std::nullopt, "module not trusted for " + std::string(trust_class) + " paglets"};
}

namespace data {

Map module_sign(const Digest& module, const PublicKey& signer, std::string_view name, std::string_view version) {
    Map m{{"module", Value::bin(module)}, {"signer", Value::bin(signer)}, {"name", Value(name)}};
    if (!version.empty()) m.emplace_back("version", Value(version));
    return m;
}

Map module_trust(std::string_view name, const std::vector<PublicKey>& signers, const std::vector<Digest>& modules,
                 const std::vector<std::string>& classes) {
    Array c;
    for (const auto& s : classes) c.emplace_back(s);
    Map m{{"name", Value(name)}, {"classes", Value(std::move(c))}};
    if (!signers.empty()) m.emplace_back("signers", bins_value(signers));
    if (!modules.empty()) m.emplace_back("modules", bins_value(modules));
    return m;
}

Map module_policy(RoamingModules roaming) {
    return Map{{"roaming", Value(to_string(roaming))}};
}

Map revoke_module(const Digest& module, std::string_view reason) {
    Map m{{"module", Value::bin(module)}};
    if (!reason.empty()) m.emplace_back("reason", Value(reason));
    return m;
}

}  // namespace data

}  // namespace paglets::mesh
