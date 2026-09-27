// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/gossip.hpp>

#include <algorithm>
#include <map>

namespace paglets::mesh {

namespace {

// Frame kinds (member `t`).
constexpr std::string_view kind_digest = "digest";        // d: ledger digest, n: record count
constexpr std::string_view kind_inventory = "inventory";  // items: [[id, signature digest]]
constexpr std::string_view kind_want = "want";            // ids: [id]
constexpr std::string_view kind_push = "push";            // records: [record bytes]

std::optional<Digest> as_digest(const Value& v) {
    const Bytes* b = v.as_bin();
    if (b == nullptr || b->size() != 32) return std::nullopt;
    Digest d{};
    std::copy(b->begin(), b->end(), d.begin());
    return d;
}

}  // namespace

Replica::Replica(Ledger& ledger, const PublicKey& self, GossipTransport& transport)
    : ledger_(ledger), self_(self), transport_(transport) {}

void Replica::add_seed(const PublicKey& peer) {
    if (peer != self_) seeds_.insert(peer);
}

std::vector<PublicKey> Replica::peers() const {
    std::set<PublicKey> all = seeds_;
    for (const auto& [key, host] : ledger_.state().hosts) {
        if (key != self_) all.insert(key);
    }
    return {all.begin(), all.end()};
}

void Replica::send(const PublicKey& to, const Value& frame) {
    ++stats_.frames_sent;
    transport_.send(to, encode(frame));
}

void Replica::push(const PublicKey& to, const std::vector<const Record*>& records) {
    for (std::size_t start = 0; start < records.size(); start += max_records_per_frame) {
        const std::size_t end = std::min(records.size(), start + max_records_per_frame);
        Array list;
        for (std::size_t i = start; i < end; ++i) list.emplace_back(records[i]->encode());
        stats_.records_sent += end - start;
        send(to, Value(Map{{"t", Value(kind_push)}, {"records", Value(std::move(list))}}));
    }
}

void Replica::push_to_all(const std::vector<const Record*>& records, const PublicKey* except) {
    if (records.empty()) return;
    for (const auto& peer : peers()) {
        if (except != nullptr && peer == *except) continue;
        push(peer, records);
    }
}

std::expected<AddResult, std::string> Replica::submit(const Record& record) {
    auto r = ledger_.add(record);
    if (!r) return r;
    if (*r != AddResult::known) {
        ++stats_.records_added;
        push_to_all({ledger_.find(record.id())}, nullptr);
        if (on_change_) on_change_();
    }
    return r;
}

void Replica::tick() {
    const auto all = peers();
    if (all.empty()) return;
    const PublicKey& peer = all[next_peer_++ % all.size()];
    send(peer, Value(Map{{"t", Value(kind_digest)},
                         {"d", Value::bin(ledger_.digest())},
                         {"n", Value(static_cast<std::int64_t>(ledger_.size()))}}));
}

void Replica::receive(const PublicKey& from, std::span<const std::uint8_t> frame) {
    ++stats_.frames_received;
    if (frame.size() > max_frame) return;
    auto v = decode(frame);
    if (!v || v->as_map() == nullptr) return;
    const Fields f{*v->as_map()};
    auto kind = f.str("t");
    if (!kind) return;
    if (*kind == kind_digest) {
        handle_digest(from, f);
    } else if (*kind == kind_inventory) {
        handle_inventory(from, f);
    } else if (*kind == kind_want) {
        handle_want(from, f);
    } else if (*kind == kind_push) {
        handle_push(from, f);
    }
}

void Replica::handle_digest(const PublicKey& from, const Fields& f) {
    const Value* d = f.get("d");
    auto digest = d == nullptr ? std::nullopt : as_digest(*d);
    if (!digest || *digest == ledger_.digest()) return;
    Array items;
    for (const Record* r : ledger_.records()) {
        items.emplace_back(Array{Value::bin(r->id()), Value::bin(r->signature_digest())});
    }
    send(from, Value(Map{{"t", Value(kind_inventory)}, {"items", Value(std::move(items))}}));
}

void Replica::handle_inventory(const PublicKey& from, const Fields& f) {
    const Array* items = f.array("items");
    if (items == nullptr) return;
    std::map<RecordId, Digest> theirs;
    for (const auto& item : *items) {
        const Array* pair = item.as_array();
        if (pair == nullptr || pair->size() != 2) return;
        auto id = as_digest((*pair)[0]);
        auto sigs = as_digest((*pair)[1]);
        if (!id || !sigs) return;
        theirs[*id] = *sigs;
    }
    // Send what they lack (or hold with other signatures), ask for what we
    // lack (or hold with other signatures): signature sets are merged, so
    // both directions are needed when they differ.
    std::vector<const Record*> give;
    Array want;
    for (const Record* r : ledger_.records()) {
        auto it = theirs.find(r->id());
        if (it == theirs.end() || it->second != r->signature_digest()) give.push_back(r);
    }
    for (const auto& [id, sigs] : theirs) {
        const Record* mine = ledger_.find(id);
        if (mine == nullptr || mine->signature_digest() != sigs) want.push_back(Value::bin(id));
    }
    push(from, give);
    if (!want.empty()) send(from, Value(Map{{"t", Value(kind_want)}, {"ids", Value(std::move(want))}}));
}

void Replica::handle_want(const PublicKey& from, const Fields& f) {
    const Array* ids = f.array("ids");
    if (ids == nullptr) return;
    std::vector<const Record*> give;
    for (const auto& item : *ids) {
        auto id = as_digest(item);
        if (!id) return;
        if (const Record* r = ledger_.find(*id)) give.push_back(r);
    }
    push(from, give);
}

void Replica::handle_push(const PublicKey& from, const Fields& f) {
    const Array* list = f.array("records");
    if (list == nullptr || list->size() > max_records_per_frame) return;
    std::vector<RecordId> changed;
    for (const auto& item : *list) {
        const Bytes* bytes = item.as_bin();
        if (bytes == nullptr) {
            ++stats_.records_rejected;
            continue;
        }
        auto r = Record::decode(*bytes);
        if (!r) {
            ++stats_.records_rejected;
            continue;
        }
        auto added = ledger_.add(*r);
        if (!added) {
            ++stats_.records_rejected;
            continue;
        }
        if (*added != AddResult::known) {
            ++stats_.records_added;
            changed.push_back(r->id());
        }
    }
    if (changed.empty()) return;
    std::vector<const Record*> forward;
    for (const auto& id : changed) forward.push_back(ledger_.find(id));
    push_to_all(forward, &from);
    if (on_change_) on_change_();
}

}  // namespace paglets::mesh
