// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/record.hpp>

#include <algorithm>

namespace paglets::mesh {

namespace {

constexpr std::string_view signing_domain = "paglets ledger record v1";

}  // namespace

std::string record_id_hex(const RecordId& id) {
    return to_hex(std::span<const std::uint8_t>(id));
}

Bytes record_signing_message(const RecordId& id) {
    Bytes m(signing_domain.begin(), signing_domain.end());
    m.push_back(0);
    m.insert(m.end(), id.begin(), id.end());
    return m;
}

std::expected<Record, std::string> Record::make(Draft draft) {
    Map body{{"type", Value(draft.type)},
             {"clock", Value(draft.clock)},
             {"time", Value(draft.time)},
             {"data", Value(std::move(draft.data))}};
    if (draft.mesh) body.emplace_back("mesh", Value::bin(*draft.mesh));
    if (draft.epoch) body.emplace_back("epoch", Value::bin(*draft.epoch));
    Record r;
    r.body_ = mesh::encode(Value(std::move(body)));
    r.id_ = sha256(r.body_);
    if (auto parsed = r.parse_body(); !parsed) return std::unexpected(parsed.error());
    return r;
}

std::expected<void, std::string> Record::parse_body() {
    auto v = mesh::decode(body_);
    if (!v) return std::unexpected(v.error());
    body_value_ = std::move(*v);
    const Map* m = body_value_.as_map();
    if (m == nullptr) return std::unexpected(std::string("record body is not a map"));
    for (const auto& [key, value] : *m) {
        if (key != "type" && key != "mesh" && key != "clock" && key != "epoch" && key != "time" && key != "data") {
            return std::unexpected("unknown record member " + key);
        }
    }
    const Fields f{*m};
    auto type = f.str("type");
    auto clock = f.integer("clock");
    auto time = f.integer("time");
    if (!type || type->empty() || !clock || *clock < 0 || *clock > max_clock || !time || f.submap("data") == nullptr) {
        return std::unexpected(std::string("record body lacks type, clock, time or data"));
    }
    type_ = *type;
    clock_ = *clock;
    time_ = *time;
    if (f.get("mesh") != nullptr) {
        mesh_ = f.fixed<32>("mesh");
        if (!mesh_) return std::unexpected(std::string("record mesh is not a 32-byte ID"));
    }
    if (f.get("epoch") != nullptr) {
        epoch_ = f.fixed<32>("epoch");
        if (!epoch_) return std::unexpected(std::string("record epoch is not a 32-byte ID"));
    }
    return {};
}

std::expected<Record, std::string> Record::decode(std::span<const std::uint8_t> bytes) {
    auto v = mesh::decode(bytes);
    if (!v) return std::unexpected(v.error());
    const Map* m = v->as_map();
    if (m == nullptr || m->size() != 2) return std::unexpected(std::string("a record has a body and signatures"));
    const Fields f{*m};
    auto body = f.bin("body");
    const Array* sigs = f.array("sigs");
    if (!body || sigs == nullptr) return std::unexpected(std::string("a record has a body and signatures"));
    Record r;
    r.body_ = std::move(*body);
    r.id_ = sha256(r.body_);
    if (auto parsed = r.parse_body(); !parsed) return std::unexpected(parsed.error());
    const Bytes message = record_signing_message(r.id_);
    for (const auto& item : *sigs) {
        const Array* pair = item.as_array();
        if (pair == nullptr || pair->size() != 2 || (*pair)[0].as_bin() == nullptr || (*pair)[1].as_bin() == nullptr ||
            (*pair)[0].as_bin()->size() != 32 || (*pair)[1].as_bin()->size() != 64) {
            return std::unexpected(std::string("malformed record signature"));
        }
        Signed s;
        std::copy_n((*pair)[0].as_bin()->begin(), 32, s.key.begin());
        std::copy_n((*pair)[1].as_bin()->begin(), 64, s.signature.begin());
        // Canonical order: strictly ascending keys.
        if (!r.signatures_.empty() && !(r.signatures_.back().key < s.key)) {
            return std::unexpected(std::string("record signatures not sorted or repeated"));
        }
        if (!verify(s.key, message, s.signature)) {
            return std::unexpected("invalid signature by " + key_id(s.key) + " on record " + record_id_hex(r.id_));
        }
        r.signatures_.push_back(s);
    }
    return r;
}

Bytes Record::encode() const {
    Array sigs;
    sigs.reserve(signatures_.size());
    for (const auto& s : signatures_) sigs.emplace_back(Array{Value::bin(s.key), Value::bin(s.signature)});
    return mesh::encode(Value(Map{{"body", Value(body_)}, {"sigs", Value(std::move(sigs))}}));
}

bool Record::signed_by(const PublicKey& key) const {
    return std::ranges::any_of(signatures_, [&](const Signed& s) { return s.key == key; });
}

bool Record::add_signature(const Signed& s) {
    auto it = std::ranges::lower_bound(signatures_, s.key, {}, &Signed::key);
    if (it != signatures_.end() && it->key == s.key) return false;
    signatures_.insert(it, s);
    return true;
}

void Record::sign(const SigningKey& key) {
    add_signature(Signed{key.public_key(), key.sign(record_signing_message(id_))});
}

std::size_t Record::merge(const Record& other) {
    if (other.id_ != id_) return 0;
    std::size_t added = 0;
    for (const auto& s : other.signatures_) added += add_signature(s) ? 1 : 0;
    return added;
}

Digest Record::signature_digest() const {
    Sha256 h;
    for (const auto& s : signatures_) {
        h.update(s.key);
        h.update(s.signature);
    }
    return h.finish();
}

bool Record::before(const Record& other) const {
    if (clock_ != other.clock_) return clock_ < other.clock_;
    return id_ < other.id_;
}

}  // namespace paglets::mesh
