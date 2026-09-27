// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Module trust in the mesh ledger (planning/cpp-modules.md, section 4).
//
// Records:
// - module-sign (signed by the signer key it names): a signer vouches for a
//   module hash, with a name and version;
// - module-trust (admin quorum): modules that may run as the listed trust
//   classes on hosts of the mesh, named by hash or by signer;
// - module-policy (admin quorum): whether roaming modules need trust too
//   (default: they do not; resident and system modules always do);
// - revoke with `module`: that hash is never trusted again (admin quorum).
//
// Every host decides from its ledger copy whether a module may run, so the
// decision is the same on every host with the same records.

#pragma once

#include <paglets/mesh/record.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::mesh {

struct LedgerState;

struct ModuleTrust {
    RecordId id{};
    std::string name;
    std::vector<PublicKey> signers;    // modules signed by any of these keys
    std::vector<Digest> modules;       // and these hashes
    std::vector<std::string> classes;  // roaming, resident, system
};

struct ModuleSignature {
    RecordId id{};
    Digest module{};
    PublicKey signer{};
    std::string name;
    std::string version;
    std::int64_t time = 0;
};

enum class RoamingModules { any, trusted };
std::string_view to_string(RoamingModules r);
std::optional<RoamingModules> parse_roaming_modules(std::string_view text);

// Why a module may run as a trust class, or why not.
struct ModuleVerdict {
    bool allowed = false;
    std::optional<RecordId> trust;  // the module-trust record that allows it
    std::string reason;
};

// Signers with a valid signature of the module (not revoked), sorted.
std::vector<PublicKey> module_signers(const LedgerState& state, const Digest& module);
// May the module run as `trust_class` (roaming, resident, system)?
ModuleVerdict check_module(const LedgerState& state, const Digest& module, std::string_view trust_class);

std::optional<ModuleTrust> parse_module_trust(const Record& r);
std::optional<ModuleSignature> parse_module_signature(const Record& r);

namespace data {
Map module_sign(const Digest& module, const PublicKey& signer, std::string_view name, std::string_view version);
Map module_trust(std::string_view name, const std::vector<PublicKey>& signers, const std::vector<Digest>& modules,
                 const std::vector<std::string>& classes);
Map module_policy(RoamingModules roaming);
Map revoke_module(const Digest& module, std::string_view reason);
}  // namespace data

}  // namespace paglets::mesh
