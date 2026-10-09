// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The host registry and discovery (planning/cpp-mesh.md): every host signs
// an announcement of its address and versions; announcements spread by
// gossip and multicast beacons; the registry lists the enrolled hosts with
// their addresses, online state and versions, and feeds the addresses to
// the transport.

#include "node_impl.hpp"

#include <paglets/abi.hpp>
#include <paglets/net/channel.hpp>
#include <paglets/sha256.hpp>

#include <algorithm>

namespace paglets::node {

namespace {

constexpr std::string_view announcement_domain = "paglets host announcement v1";
constexpr std::int64_t max_clock_skew_ms = 5 * 60 * 1000;
constexpr std::size_t max_url = 512;
constexpr std::size_t max_announcements = 4096;

mesh::Bytes announcement_message(std::span<const std::uint8_t> body) {
    mesh::Bytes m(announcement_domain.begin(), announcement_domain.end());
    m.push_back(0);
    const auto digest = paglets::sha256(body);
    m.insert(m.end(), digest.begin(), digest.end());
    return m;
}

// The host part of an URL (`https://host:port/...`).
std::string url_host(std::string_view url) {
    const auto scheme = url.find("://");
    if (scheme == std::string_view::npos) return {};
    std::string_view rest = url.substr(scheme + 3);
    rest = rest.substr(0, rest.find('/'));
    if (rest.starts_with('[')) return std::string(rest.substr(0, rest.find(']') + 1));
    return std::string(rest.substr(0, rest.rfind(':')));
}

// An URL whose host means "any interface" says nothing about where the
// host is: the address it was seen from stands in.
std::string reachable(const std::string& url, const std::string& seen_from) {
    if (url.find("://") == std::string::npos) return url;  // none (no inbound port)
    const std::string host = url_host(url);
    if (!seen_from.empty() && (host == "0.0.0.0" || host == "[::]" || host.empty())) {
        const auto scheme = url.find("://");
        const auto start = scheme + 3;
        return url.substr(0, start) + seen_from + url.substr(start + host.size());
    }
    return url;
}

mesh::Value entry_value(const Node::Impl::HostEntry& e) {
    return mesh::Value(mesh::Array{mesh::Value(e.body), mesh::Value::bin(e.signature), mesh::Value(e.url)});
}

}  // namespace

// -- announcements ------------------------------------------------------------------------

void Node::Impl::announce_self() {
    const std::string url = transport.own_address();
    const std::vector<mesh::PublicKey> relays = url.empty() ? own_relays : std::vector<mesh::PublicKey>{};
    if (own_entry && own_entry->url == url && own_entry->relays == relays) return;
    HostEntry e;
    e.url = url;
    e.relays = relays;
    // Strictly later than the previous one: two announcements within one
    // millisecond (no relays yet, then the first relay) must not look the
    // same age, or hosts keep the first and cannot reach this one.
    e.announced = std::max(mesh::unix_ms(), own_entry ? own_entry->announced + 1 : std::int64_t{0});
    e.protocol = net::mesh_protocol;
    e.abi_major = abi::version;
    e.abi_minor = abi::minor_version;
    e.via = "self";
    e.body = mesh::encode(
        mesh::Value(mesh::Map{{"v", mesh::Value(std::int64_t{1})},
                              {"mesh", mesh::Value::bin(ledger.mesh())},
                              {"key", mesh::Value::bin(key.public_key())},
                              {"url", mesh::Value(url)},
                              {"proto", mesh::Value(e.protocol)},
                              {"abi", mesh::Value(mesh::Array{mesh::Value(static_cast<std::int64_t>(e.abi_major)),
                                                              mesh::Value(static_cast<std::int64_t>(e.abi_minor))})},
                              {"time", mesh::Value(e.announced)},
                              {"relays", mesh::Value([&] {
                                   mesh::Array a;
                                   for (const auto& k : e.relays) a.push_back(mesh::Value::bin(k));
                                   return a;
                               }())}}));
    e.signature = key.sign(announcement_message(e.body));
    own_entry = std::move(e);
    // The hosts that know this one hear about the change.
    for (const auto& k : view) {
        if (k != key.public_key()) send_registry(k);
    }
}

std::optional<Node::Impl::HostEntry> Node::Impl::verify_announcement(const mesh::Bytes& body,
                                                                     const mesh::Signature& signature,
                                                                     std::string& why) const {
    auto v = mesh::decode(body);
    if (!v || v->as_map() == nullptr) {
        why = "malformed announcement";
        return std::nullopt;
    }
    const mesh::Fields f{*v->as_map()};
    auto version = f.integer("v");
    auto mesh_id = f.fixed<32>("mesh");
    auto host = f.fixed<32>("key");
    auto url = f.str("url");
    auto proto = f.integer("proto");
    auto time = f.integer("time");
    const mesh::Array* abi_version = f.array("abi");
    if (!version || *version != 1 || !mesh_id || !host || !url || url->size() > max_url || !proto || !time ||
        abi_version == nullptr || abi_version->size() != 2 || !(*abi_version)[0].as_int() ||
        !(*abi_version)[1].as_int()) {
        why = "malformed announcement";
        return std::nullopt;
    }
    if (*mesh_id != ledger.mesh()) {
        why = "announcement of another mesh";
        return std::nullopt;
    }
    // Only enrolled hosts are in the registry.
    if (!ledger.state().is_host(*host)) {
        why = "announcement of a host that is not enrolled";
        return std::nullopt;
    }
    if (!mesh::verify(*host, announcement_message(body), signature)) {
        why = "announcement signature invalid";
        return std::nullopt;
    }
    if (*time > mesh::unix_ms() + max_clock_skew_ms) {
        why = "announcement from the future";
        return std::nullopt;
    }
    HostEntry e;
    if (const mesh::Array* relays = f.array("relays")) {
        if (relays->size() > 8) {
            why = "malformed announcement";
            return std::nullopt;
        }
        for (const auto& r : *relays) {
            const mesh::Bytes* k = r.as_bin();
            if (k == nullptr || k->size() != 32) {
                why = "malformed announcement";
                return std::nullopt;
            }
            mesh::PublicKey relay{};
            std::copy(k->begin(), k->end(), relay.begin());
            e.relays.push_back(relay);
        }
    }
    e.body = body;
    e.signature = signature;
    e.url = *url;
    e.announced = *time;
    e.protocol = *proto;
    e.abi_major = static_cast<std::uint32_t>(std::clamp<std::int64_t>(*(*abi_version)[0].as_int(), 0, 1'000'000));
    e.abi_minor = static_cast<std::uint32_t>(std::clamp<std::int64_t>(*(*abi_version)[1].as_int(), 0, 1'000'000));
    return e;
}

// Keeps a newer announcement; the transport learns the address. Returns
// true if it changed the registry.
bool Node::Impl::learn(const mesh::PublicKey& host, HostEntry entry, const std::string& fallback_url) {
    if (host == key.public_key()) return false;
    entry.url = reachable(entry.url, url_host(fallback_url).empty() ? fallback_url : url_host(fallback_url));
    auto it = registry.find(host);
    if (it != registry.end() && it->second.announced >= entry.announced) {
        // The same announcement: a better address (seen by a beacon) still helps.
        if (it->second.announced == entry.announced && it->second.url.empty() && !entry.url.empty()) {
            it->second.url = entry.url;
        } else {
            return false;
        }
    } else {
        if (registry.size() >= max_announcements && it == registry.end()) return false;
        registry[host] = std::move(entry);
        it = registry.find(host);
    }
    if (!it->second.url.empty()) {
        transport.set_address(host, it->second.url);
    } else {
        transport.set_relays(host, it->second.relays);  // no inbound port: through its relays
    }
    registry_dirty = true;
    return true;
}

// -- gossip ---------------------------------------------------------------------------------

// Sends every announcement this host has (its own first) to `to`.
void Node::Impl::send_registry(const mesh::PublicKey& to, const std::optional<mesh::PublicKey>& except_about) {
    mesh::Array list;
    if (own_entry) list.push_back(entry_value(*own_entry));
    for (const auto& [host, e] : registry) {
        if (host == to || (except_about && host == *except_about)) continue;
        list.push_back(entry_value(e));
    }
    send_frame(to, mesh::Map{{"t", mesh::Value("hosts")}, {"a", mesh::Value(std::move(list))}});
}

void Node::Impl::on_hosts(const mesh::PublicKey& from, const mesh::Fields& f) {
    const mesh::Array* list = f.array("a");
    if (list == nullptr || list->size() > max_announcements) return;
    std::vector<mesh::PublicKey> changed;
    for (const auto& item : *list) {
        const mesh::Array* a = item.as_array();
        if (a == nullptr || a->size() != 3) continue;
        const mesh::Bytes* body = (*a)[0].as_bin();
        const mesh::Bytes* sig = (*a)[1].as_bin();
        const std::string* seen = (*a)[2].as_str();
        if (body == nullptr || sig == nullptr || sig->size() != 64 || seen == nullptr || seen->size() > max_url) {
            continue;
        }
        mesh::Signature signature{};
        std::copy(sig->begin(), sig->end(), signature.begin());
        std::string why;
        auto entry = verify_announcement(*body, signature, why);
        if (!entry) continue;
        const mesh::PublicKey host = *mesh::Fields{*mesh::decode(*body)->as_map()}.fixed<32>("key");
        entry->via = "gossip";
        if (learn(host, std::move(*entry), *seen)) changed.push_back(host);
    }
    // Eager push: what was new here goes on to the other live hosts.
    if (changed.empty()) return;
    for (const auto& k : view) {
        if (k == key.public_key() || k == from) continue;
        mesh::Array news;
        for (const auto& host : changed) {
            if (host != k) news.push_back(entry_value(registry.at(host)));
        }
        if (!news.empty()) send_frame(k, mesh::Map{{"t", mesh::Value("hosts")}, {"a", mesh::Value(std::move(news))}});
    }
}

bool Node::Impl::compatible(const mesh::PublicKey& host) const {
    auto it = registry.find(host);
    if (it == registry.end()) return true;  // not announced yet: the channel gate still applies
    return it->second.protocol == net::mesh_protocol && it->second.abi_major == abi::version;
}

void Node::Impl::registry_tick() {
    announce_self();
    // Addresses known before the transport was attached (from the state).
    if (!addresses_applied) {
        addresses_applied = true;
        for (const auto& [host, e] : registry) {
            if (!e.url.empty()) {
                transport.set_address(host, e.url);
            } else {
                transport.set_relays(host, e.relays);
            }
        }
    }
    if (!transport.inbound()) choose_relays();
    const mesh::LedgerState& st = ledger.state();
    // Hosts that left the ledger leave the registry.
    const auto before = registry.size();
    std::erase_if(registry, [&](const auto& e) { return !st.is_host(e.first); });
    if (registry.size() != before) registry_dirty = true;
    // Anti-entropy: the registry goes to the next host with an address.
    const auto now = Clock::now();
    if (now >= next_exchange) {
        next_exchange = now + discovery.exchange;
        std::vector<mesh::PublicKey> reachable_hosts;
        for (const auto& [k, h] : st.hosts) {
            if (k == key.public_key()) continue;
            auto r = registry.find(k);
            if (transport.address(k) || (r != registry.end() && !r->second.relays.empty())) {
                reachable_hosts.push_back(k);
            }
        }
        if (!reachable_hosts.empty()) {
            send_registry(reachable_hosts[next_exchange_peer++ % reachable_hosts.size()]);
        }
    }
    if (registry_dirty) save_registry();
}

// A host without an inbound port keeps a few relays: hosts it can reach
// that are up; one that stops answering is replaced.
void Node::Impl::choose_relays() {
    const auto now = Clock::now();
    const auto live = transport.live_uplinks();
    for (const auto& k : live) relay_ok[k] = now;
    const mesh::LedgerState& st = ledger.state();
    std::vector<mesh::PublicKey> keep;
    for (const auto& k : own_relays) {
        auto ok = relay_ok.find(k);
        if (st.is_host(k) && ok != relay_ok.end() && now - ok->second < discovery.relay_timeout) keep.push_back(k);
    }
    std::vector<mesh::PublicKey> candidates;
    for (const auto& [k, h] : st.hosts) {
        if (k == key.public_key() || std::ranges::find(keep, k) != keep.end() || !transport.address(k)) continue;
        if (!std::ranges::binary_search(view, k) || !compatible(k)) continue;
        candidates.push_back(k);
    }
    // Hosts heard from most recently first.
    std::ranges::sort(candidates, [&](const auto& a, const auto& b) {
        auto sa = last_seen.find(a);
        auto sb = last_seen.find(b);
        return (sa == last_seen.end() ? 0 : sa->second) > (sb == last_seen.end() ? 0 : sb->second);
    });
    for (const auto& k : candidates) {
        if (keep.size() >= discovery.relays) break;
        keep.push_back(k);
        relay_ok[k] = now;  // a new relay gets its time to answer
    }
    if (keep == own_relays) return;
    own_relays = keep;
    std::vector<std::pair<mesh::PublicKey, std::string>> uplinks;
    for (const auto& k : own_relays) uplinks.emplace_back(k, *transport.address(k));
    transport.set_uplinks(std::move(uplinks));
    announce_self();  // the others learn the new relays
}

void Node::Impl::load_registry() {
    if (state_dir.empty()) return;
    auto bytes = wasm::read_file((state_dir / "hosts").string());
    if (!bytes) return;
    auto v = mesh::decode(*bytes);
    if (!v || v->as_array() == nullptr) return;
    for (const auto& item : *v->as_array()) {
        const mesh::Array* a = item.as_array();
        if (a == nullptr || a->size() != 3 || (*a)[0].as_bin() == nullptr || (*a)[1].as_bin() == nullptr ||
            (*a)[1].as_bin()->size() != 64 || (*a)[2].as_str() == nullptr) {
            continue;
        }
        mesh::Signature signature{};
        std::copy((*a)[1].as_bin()->begin(), (*a)[1].as_bin()->end(), signature.begin());
        std::string why;
        auto entry = verify_announcement(*(*a)[0].as_bin(), signature, why);
        if (!entry) continue;
        const mesh::PublicKey host = *mesh::Fields{*mesh::decode(entry->body)->as_map()}.fixed<32>("key");
        entry->url = *(*a)[2].as_str();
        entry->via = "state";
        registry[host] = std::move(*entry);
    }
}

void Node::Impl::save_registry() {
    registry_dirty = false;
    if (state_dir.empty()) return;
    mesh::Array list;
    for (const auto& [host, e] : registry) list.push_back(entry_value(e));
    std::error_code ec;
    fs::create_directories(state_dir, ec);
    const fs::path temp = state_dir / "hosts.tmp";
    if (wasm::write_file(temp.string(), mesh::encode(mesh::Value(std::move(list))))) {
        fs::rename(temp, state_dir / "hosts", ec);
    }
}

// -- Node API ------------------------------------------------------------------------------

void Node::set_discovery_timing(DiscoveryTiming timing) {
    std::lock_guard lock(impl_->mu);
    impl_->discovery = timing;
    impl_->next_exchange = {};
}

std::vector<Node::HostInfo> Node::hosts() const {
    std::lock_guard lock(impl_->mu);
    impl_->update_view();
    std::vector<HostInfo> out;
    for (const auto& [k, h] : impl_->ledger.state().hosts) {
        HostInfo info;
        info.key = k;
        info.name = h.name;
        info.labels = h.labels;
        info.self = k == impl_->key.public_key();
        info.online = std::ranges::binary_search(impl_->view, k);
        if (info.self) {
            info.url = impl_->transport.own_address();
            info.protocol = net::mesh_protocol;
            info.abi_major = abi::version;
            info.abi_minor = abi::minor_version;
            info.last_seen_ms = mesh::unix_ms();
            info.via = "self";
            info.relays = impl_->own_relays;
        } else {
            if (auto it = impl_->registry.find(k); it != impl_->registry.end()) {
                info.url = it->second.url;
                info.protocol = it->second.protocol;
                info.abi_major = it->second.abi_major;
                info.abi_minor = it->second.abi_minor;
                info.via = it->second.via;
                info.relays = it->second.relays;
            }
            if (info.url.empty()) info.url = impl_->transport.address(k).value_or("");
            if (auto s = impl_->last_seen.find(k); s != impl_->last_seen.end()) info.last_seen_ms = s->second;
            // Online means heard from, not merely not yet timed out.
            info.online = info.online && impl_->last_seen.contains(k);
            info.compatible = impl_->compatible(k);
            info.send_error = impl_->transport.send_failure(k).value_or("");
        }
        out.push_back(std::move(info));
    }
    return out;
}

void Node::joined(const mesh::PublicKey& contact) {
    std::lock_guard lock(impl_->mu);
    impl_->announce_self();
    impl_->greeted.insert(contact);
    impl_->send_registry(contact);
}

std::vector<std::uint8_t> Node::beacon() const {
    std::lock_guard lock(impl_->mu);
    if (!impl_->own_entry) return {};
    return mesh::encode(mesh::Value(mesh::Map{{"t", mesh::Value("paglets-beacon")},
                                              {"mesh", mesh::Value::bin(impl_->ledger.mesh())},
                                              {"a", entry_value(*impl_->own_entry)}}));
}

void Node::receive_beacon(std::span<const std::uint8_t> beacon, const std::string& sender_ip) {
    auto v = mesh::decode(beacon);
    if (!v || v->as_map() == nullptr) return;
    const mesh::Fields f{*v->as_map()};
    auto mesh_id = f.fixed<32>("mesh");
    const mesh::Array* a = f.array("a");
    if (f.str("t") != "paglets-beacon" || !mesh_id || a == nullptr || a->size() != 3) return;
    std::lock_guard lock(impl_->mu);
    if (*mesh_id != impl_->ledger.mesh()) return;  // another mesh on the same network
    const mesh::Bytes* body = (*a)[0].as_bin();
    const mesh::Bytes* sig = (*a)[1].as_bin();
    if (body == nullptr || sig == nullptr || sig->size() != 64) return;
    mesh::Signature signature{};
    std::copy(sig->begin(), sig->end(), signature.begin());
    std::string why;
    auto entry = impl_->verify_announcement(*body, signature, why);
    if (!entry) return;
    const mesh::PublicKey host = *mesh::Fields{*mesh::decode(*body)->as_map()}.fixed<32>("key");
    entry->via = "beacon";
    const bool fresh = !impl_->transport.address(host).has_value();
    if (impl_->learn(host, std::move(*entry), sender_ip) && fresh) {
        // A host found on the local network gets this host's registry.
        impl_->greeted.insert(host);
        impl_->send_registry(host);
    }
}

}  // namespace paglets::node
