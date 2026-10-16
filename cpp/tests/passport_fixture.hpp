// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <paglets/mesh/passport.hpp>

namespace paglets::test {

// Sign explicit root bytes to exercise old saved passports and authenticated
// owners that deliberately claim a different owner's ID. Production issue()
// must never generate the unbound v1 format again.
inline mesh::Bytes sign_passport_root(const mesh::SigningKey& owner, mesh::Map body, bool bound) {
    auto bytes = mesh::encode(mesh::Value(std::move(body)));
    const std::string_view domain = bound ? "paglets passport v2" : "paglets passport v1";
    mesh::Bytes message(domain.begin(), domain.end());
    message.push_back(0);
    message.insert(message.end(), bytes.begin(), bytes.end());
    const auto signature = owner.sign(message);
    return mesh::encode(mesh::Value(mesh::Map{
        {"root", mesh::Value(mesh::Map{{"body", mesh::Value(std::move(bytes))},
                                       {"sig", mesh::Value::bin(signature)}})},
        {"links", mesh::Value(mesh::Array{})}}));
}

inline mesh::Bytes legacy_passport(const mesh::SigningKey& owner, const mesh::RecordId& mesh,
                                   const Digest& module, std::string id, std::int64_t now) {
    return sign_passport_root(owner, mesh::Map{{"type", mesh::Value("passport")},
                                               {"mesh", mesh::Value::bin(mesh)},
                                               {"owner", mesh::Value::bin(owner.public_key())},
                                               {"module", mesh::Value::bin(module)},
                                               {"paglet", mesh::Value(std::move(id))},
                                               {"manifest", mesh::Value()},
                                               {"issued", mesh::Value(now - 1000)},
                                               {"expires", mesh::Value(now + 600'000)}}, false);
}

}  // namespace paglets::test
