// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The mesh (WP14, planning/cpp-mesh.md): hosts discover each other through
// any enrolled host, by gossip and by multicast beacons; the host registry
// knows their addresses, online state and versions; hosts of another
// protocol or ABI are kept out. The exit: three hosts discover each other
// and move paglets by host name.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"
#include "tls_fixture.hpp"

#include <paglets/abi.hpp>
#include <paglets/net/beacon.hpp>
#include <paglets/net/channel.hpp>
#include <paglets/sha256.hpp>

#include <filesystem>
#include <future>
#include <iostream>
#include <random>

using namespace paglets::test;
using HostInfo = paglets::node::Node::HostInfo;

namespace {

std::int64_t count(Fixture& f, const rt::PagletId& id) {
    auto r = f.call(id, "count");
    if (r.status != 0) throw std::runtime_error("count: " + std::string(abi::error_name(r.status)));
    return dec<std::int64_t>(r.payload);
}

// Hosts over HTTPS that know nothing about each other's addresses.
struct Trio {
    SigningKey admin = SigningKey::generate();
    SigningKey owner = SigningKey::generate();
    std::optional<Record> genesis;
    std::vector<std::unique_ptr<NetHost>> hosts;

    // The last `without_inbound` hosts have no inbound port (WP15);
    // `configure` adjusts a host's transport.
    explicit Trio(std::size_t n = 3, std::size_t without_inbound = 0,
                  const std::function<void(std::size_t, paglets::net::TransportConfig&)>& configure = {}) {
        auto g = make_genesis("discovery", {&admin}, {}, 1);
        REQUIRE_OK(g);
        genesis = *g;
        for (std::size_t i = 0; i < n; ++i) {
            paglets::net::TransportConfig tc;
            tc.poll_wait = 1000ms;
            tc.listen = i + without_inbound < n;
            if (configure) configure(i, tc);
            hosts.push_back(std::make_unique<NetHost>(*genesis, ps::ServicesConfig{}, tc));
        }
        // Every host has the ledger with the enrollments (an admin handed
        // it out), but no addresses.
        std::vector<Record> records;
        auto record = [&](const std::string& type, Map d) {
            auto r = hosts[0]->node->draft(type, std::move(d));
            REQUIRE_OK(r);
            r->sign(admin);
            records.push_back(*r);
            for (auto& h : hosts) REQUIRE_OK(h->node->submit(*r));
        };
        const char* names[] = {"a", "b", "c", "d", "e"};
        for (std::size_t i = 0; i < n; ++i)
            record("host-enroll", data::host_enroll(hosts[i]->node->host(), names[i], {}));
        record("owner-enroll", data::owner_enroll(owner.public_key(), "olga", {}));
        for (auto& h : hosts) {
            h->node->set_discovery_timing({.exchange = 200ms, .relays = 2, .relay_timeout = 2000ms});
            h->node->set_location_timing({.heartbeat = 100ms,
                                          .host_timeout = 3000ms,
                                          .refresh = std::chrono::minutes(5),
                                          .record_ttl = std::chrono::hours(1),
                                          .lookup_timeout = 1000ms});
        }
    }

    NetHost& operator[](std::size_t i) { return *hosts[i]; }

    bool eventually(const std::function<bool()>& done, int rounds = 1500) {
        for (int i = 0; i < rounds; ++i) {
            if (done()) return true;
            for (auto& h : hosts) h->node->tick();
            std::this_thread::sleep_for(10ms);
        }
        return done();
    }

    // Every host has heard from every other host, and can reach it (an
    // address, or the relays of a host without an inbound port).
    bool discovered_except_addresses() {
        for (auto& h : hosts) {
            for (const auto& info : h->node->hosts()) {
                if (info.self) continue;
                if (!info.online || (info.url.empty() && info.relays.empty())) return false;
            }
        }
        return true;
    }

    // Every host knows every other host's address and has heard from it.
    bool discovered() {
        for (auto& h : hosts) {
            for (const auto& info : h->node->hosts()) {
                if (info.self) continue;
                if (!info.online || info.url.empty() || !h->transport->address(info.key)) return false;
            }
        }
        return true;
    }
};

const HostInfo* info_of(const std::vector<HostInfo>& hosts, const PublicKey& key) {
    for (const auto& h : hosts) {
        if (h.key == key) return &h;
    }
    return nullptr;
}

}  // namespace

PAGLETS_TEST("mesh: three hosts discover each other through one contact each and move paglets by name (WP14 exit)") {
    Trio net;
    auto& a = net[0];
    auto& b = net[1];
    auto& c = net[2];
    // b knows only a's address, c only b's; a knows nobody.
    auto contact = b.transport->probe(a.transport->url());
    REQUIRE_OK(contact);
    CHECK(*contact == a.node->host());
    b.node->joined(*contact);
    contact = c.transport->probe(b.transport->url());
    REQUIRE_OK(contact);
    c.node->joined(*contact);
    REQUIRE(net.eventually([&] { return net.discovered(); }));

    // The registry: names, addresses, versions, online.
    const auto hosts = a.node->hosts();
    REQUIRE(hosts.size() == 3u);
    const HostInfo* seen_c = info_of(hosts, c.node->host());
    REQUIRE(seen_c != nullptr);
    CHECK(seen_c->name == "c");
    CHECK(seen_c->url == c.transport->url());
    CHECK(seen_c->online && seen_c->compatible);
    CHECK_EQ(seen_c->protocol, paglets::net::mesh_protocol);
    CHECK_EQ(seen_c->abi_major, abi::version);
    CHECK_EQ(seen_c->abi_minor, abi::minor_version);
    CHECK(seen_c->last_seen_ms > 0);
    CHECK(info_of(hosts, a.node->host())->self);
    CHECK(net.eventually([&] { return a.node->ledger_digest() == c.node->ledger_digest(); }));

    // Paglets move by host name between hosts that were introduced by nobody.
    const std::string module = a.f->module("conformance.wasm");
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(net.owner, net.genesis->id(), *parse_key_id(module), paglet_id('8'), Value(),
                                    now - 1000, now + 600'000);
    REQUIRE_OK(passport);
    auto id = a.node->create(module, *passport);
    REQUIRE_OK(id);
    CHECK_EQ(count(*a.f, *id), 1);
    REQUIRE_OK(a.node->dispatch(*id, "c"));
    REQUIRE(net.eventually([&] { return c.f->runtime->info(*id).has_value() && !a.f->runtime->info(*id); }));
    CHECK_EQ(count(*c.f, *id), 2);
    REQUIRE_OK(c.node->dispatch(*id, "b"));
    REQUIRE(net.eventually([&] { return b.f->runtime->info(*id).has_value() && !c.f->runtime->info(*id); }));
    CHECK_EQ(count(*b.f, *id), 3);
    REQUIRE_OK(b.node->dispatch(*id, "a"));
    REQUIRE(net.eventually([&] { return a.f->runtime->info(*id).has_value() && !b.f->runtime->info(*id); }));
    CHECK_EQ(count(*a.f, *id), 4);

    // A host that goes away is offline in the registry.
    net.hosts[2]->f->runtime->shutdown();
    net.hosts[2]->transport->stop();
    net.hosts.pop_back();
    REQUIRE(net.eventually([&] {
        const auto current = a.node->hosts();
        const HostInfo* h = info_of(current, seen_c->key);
        return h != nullptr && !h->online;
    }));
}

PAGLETS_TEST("mesh: a host that cannot be reached is one-way; CLI sessions hear how a move ended") {
    namespace fs = std::filesystem;
    // a checks certificates against a root CA; b has a certificate of
    // another CA: b reaches a, a does not reach b. c is reached by both.
    const fs::path dir = fs::temp_directory_path() / ("paglets-oneway-" + std::to_string(std::random_device{}()));
    fs::create_directories(dir);
    const TestCert root = make_test_cert("test root", nullptr, true);
    const TestCert other = make_test_cert("other root", nullptr, true);
    const TestCert a_cert = make_test_cert("a", &root, false);
    const TestCert b_cert = make_test_cert("b", &other, false);
    Trio net(3, 0, [&](std::size_t i, paglets::net::TransportConfig& tc) {
        if (i == 0) {
            tc.tls_cert = write_test_file(dir / "a.crt.pem", a_cert.cert);
            tc.tls_key = write_test_file(dir / "a.key.pem", a_cert.key);
            tc.tls_ca = write_test_file(dir / "root.pem", root.cert);
        } else if (i == 1) {
            tc.tls_cert = write_test_file(dir / "b.crt.pem", b_cert.cert);
            tc.tls_key = write_test_file(dir / "b.key.pem", b_cert.key);
        }
    });
    auto& a = net[0];
    auto& b = net[1];
    auto& c = net[2];
    for (auto* h : {&b, &c}) {
        auto contact = h->transport->probe(a.transport->url());
        REQUIRE_OK(contact);
        h->node->joined(*contact);
    }
    c.transport->set_address(b.node->host(), b.transport->url());
    b.transport->set_address(c.node->host(), c.transport->url());
    // a hears from b, but its frames to b fail: one-way, with the reason.
    REQUIRE(net.eventually([&] {
        const auto hosts = a.node->hosts();
        const HostInfo* h = info_of(hosts, b.node->host());
        return h != nullptr && h->online && !h->send_error.empty();
    }));
    const HostInfo seen_b = *info_of(a.node->hosts(), b.node->host());
    CHECK(seen_b.send_error.find("unable to get local issuer certificate") != std::string::npos);
    REQUIRE(net.eventually([&] {
        const auto hosts = b.node->hosts();
        const HostInfo* h = info_of(hosts, c.node->host());
        return h != nullptr && h->online && h->send_error.empty();
    }));

    // An owner's session asks b to move a paglet and waits for the outcome.
    const std::string module = b.f->module("conformance.wasm");
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(net.owner, net.genesis->id(), *parse_key_id(module), paglet_id('9'), Value(),
                                    now - 1000, now + 600'000);
    REQUIRE_OK(passport);
    auto id = b.node->create(module, *passport);
    REQUIRE_OK(id);
    b.node->set_move_timeout(1500ms);
    auto dispatch = [&](const std::string& destination) {
        auto answer = std::async(std::launch::async, [&, destination] {
            return b.node->answer_session(net.owner.public_key(), "owner",
                                          encode(Value(Map{{"t", Value("dispatch")},
                                                           {"paglet", Value(*id)},
                                                           {"destination", Value(destination)},
                                                           {"wait_ms", Value(std::int64_t{20000})}})));
        });
        REQUIRE(net.eventually([&] { return answer.wait_for(0ms) == std::future_status::ready; }));
        auto v = decode(answer.get());
        REQUIRE(v && v->as_map() != nullptr);
        return *v->as_map();
    };
    auto flag = [](const Map& m, std::string_view k) -> std::optional<bool> {
        const Value* v = Fields{m}.get(k);
        if (v == nullptr || v->as_bool() == nullptr) return std::nullopt;
        return *v->as_bool();
    };
    // a cannot answer b's offer: the move fails, and the session says why.
    Map failed = dispatch("a");
    CHECK(flag(failed, "ok").value_or(false));
    CHECK(!flag(failed, "moved").value_or(true));
    CHECK(Fields{failed}.str("reason").value_or("").find("no host took the paglet") != std::string::npos);
    CHECK(b.f->runtime->info(*id).has_value());
    // To c it moves; the answer names c.
    Map moved = dispatch("c");
    CHECK(flag(moved, "moved").value_or(false));
    CHECK(Fields{moved}.str("host_name") == std::optional<std::string>("c"));
    CHECK(Fields{moved}.fixed<32>("host") == std::optional<PublicKey>(c.node->host()));
    REQUIRE(net.eventually([&] { return c.f->runtime->info(*id).has_value(); }));
    fs::remove_all(dir);
}

PAGLETS_TEST("mesh: hosts on the local network find each other by multicast beacons") {
    Trio net(2);
    auto& a = net[0];
    auto& b = net[1];
    paglets::net::BeaconConfig config;
    config.port = 40000 + static_cast<int>(std::random_device{}() % 20000);
    config.interval = 200ms;
    std::vector<std::unique_ptr<paglets::net::Beacon>> beacons;
    for (auto& h : net.hosts) {
        auto* node = h->node.get();
        beacons.push_back(std::make_unique<paglets::net::Beacon>(
            config, [node] { return node->beacon(); },
            [node](std::vector<std::uint8_t> d, const std::string& ip) { node->receive_beacon(d, ip); }));
        if (auto ok = beacons.back()->start(); !ok) paglets::test::skip("no multicast: " + ok.error());
    }
    for (auto& h : net.hosts) h->node->tick();  // the announcements exist
    if (!net.eventually([&] { return beacons[0]->received() > 0 && beacons[1]->received() > 0; }, 300)) {
        paglets::test::skip("multicast datagrams do not arrive on this machine");
    }
    REQUIRE(net.eventually([&] { return net.discovered(); }));
    const auto registry = a.node->hosts();
    const HostInfo* seen = info_of(registry, b.node->host());
    REQUIRE(seen != nullptr);
    CHECK_EQ(seen->url, b.transport->url());
    CHECK(seen->via == "beacon" || seen->via == "gossip");

    // Beacons of another mesh, or forged ones, are ignored.
    Bytes forged = b.node->beacon();
    forged[forged.size() / 2] ^= 0x01;
    a.node->receive_beacon(forged, "127.0.0.1");
    CHECK(info_of(a.node->hosts(), b.node->host())->url == b.transport->url());
    for (auto& beacon : beacons) beacon->stop();
}

PAGLETS_TEST("mesh: the registry keeps hosts of another ABI out of moves and responsibilities") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    // b announces a paglet ABI major version this host does not run.
    const Bytes body =
        encode(Value(Map{{"v", Value(std::int64_t{1})},
                         {"mesh", Value::bin(net.genesis->id())},
                         {"key", Value::bin(b.key)},
                         {"url", Value("https://b.example:7443")},
                         {"proto", Value(paglets::net::mesh_protocol)},
                         {"abi", Value(Array{Value(std::int64_t{abi::version + 1}), Value(std::int64_t{0})})},
                         {"time", Value(unix_ms())}}));
    Bytes message{'p', 'a', 'g', 'l', 'e', 't', 's', ' ', 'h', 'o', 's', 't', ' ', 'a', 'n',
                  'n', 'o', 'u', 'n', 'c', 'e', 'm', 'e', 'n', 't', ' ', 'v', '1', 0};
    const auto digest = paglets::sha256(body);
    message.insert(message.end(), digest.begin(), digest.end());
    const Signature sig = b.node->host_key().sign(message);
    Bytes frame = encode(
        Value(Map{{"t", Value("hosts")}, {"a", Value(Array{Value(Array{Value(body), Value::bin(sig), Value("")})})}}));
    // Not signed by b: ignored.
    Bytes forged = frame;
    forged[forged.size() - 20] ^= 0x01;  // in the signature
    a.node->receive(c.key, forged);
    CHECK(info_of(a.node->hosts(), b.key)->compatible);
    a.node->receive(b.key, frame);
    const auto registry = a.node->hosts();
    const HostInfo* seen = info_of(registry, b.key);
    REQUIRE(seen != nullptr);
    CHECK(!seen->compatible);
    CHECK_EQ(seen->abi_major, abi::version + 1);
    const auto live = a.node->live_hosts();
    CHECK(std::ranges::find(live, b.key) == live.end());

    // Moves go elsewhere, or fail with the reason.
    const std::string module = a.f->module("conformance.wasm");
    auto id = a.node->create(module, net.passport(module, paglet_id('9')));
    REQUIRE_OK(id);
    REQUIRE_OK(a.node->dispatch(*id, "b"));
    REQUIRE(net.settle([&] { return a.node->move_stats().moves_failed == 1u; }));
    CHECK(journal_eventually(*a.f, *id, "move_failed:b:no host matches b; host b: runs an incompatible paglet ABI:"));
    REQUIRE_OK(a.node->dispatch(*id, "any"));
    REQUIRE(net.settle([&] { return c.f->runtime->info(*id).has_value(); }, 400));
}

namespace {

// Moves a paglet to the host named `to` and waits until it is there.
void move_to(Trio& net, NetHost& from, NetHost& to, const rt::PagletId& id, const std::string& name) {
    REQUIRE_OK(from.node->dispatch(id, name));
    REQUIRE(net.eventually([&] { return to.f->runtime->info(id).has_value() && !from.f->runtime->info(id); }));
}

std::vector<PublicKey> relays_of(NetHost& h) {
    for (const auto& info : h.node->hosts()) {
        if (info.self) return info.relays;
    }
    return {};
}

// What every host knows of the others (printed when a relay test waits in
// vain, to see which part of the picture is missing).
void dump_relays(Trio& net) {
    for (auto& h : net.hosts) {
        std::string self;
        for (const auto& info : h->node->hosts()) {
            if (info.self) self = info.name;
        }
        std::cerr << "    host " << self << ": " << relays_of(*h).size() << " relays, "
                  << h->transport->live_uplinks().size() << " live uplinks\n";
        for (const auto& info : h->node->hosts()) {
            if (info.self) continue;
            std::cerr << "      " << info.name << ": online " << info.online << ", compatible " << info.compatible
                      << ", address " << h->transport->address(info.key).has_value() << ", url '" << info.url
                      << "', relays " << info.relays.size() << ", via " << info.via << "\n";
        }
    }
}

}  // namespace

PAGLETS_TEST("relay: a host without an inbound port takes part in the mesh, also when a relay goes down (WP15 exit)") {
    Trio net(4, 1);  // a, b, c can be reached; d cannot
    auto& a = net[0];
    auto& d = net[3];
    CHECK(!d.transport->inbound());
    CHECK(d.transport->own_address().empty());
    for (std::size_t i = 1; i < 4; ++i) {
        auto contact = net[i].transport->probe(a.transport->url());
        REQUIRE_OK(contact);
        net[i].node->joined(*contact);
    }
    // d keeps two relays; every host reaches d through them.
    const bool two_relays =
        net.eventually([&] { return relays_of(d).size() == 2u && d.transport->live_uplinks().size() == 2u; });
    if (!two_relays) dump_relays(net);
    REQUIRE(two_relays);
    REQUIRE(net.eventually([&] { return net.discovered_except_addresses(); }));
    const auto relays = relays_of(d);
    // Every host learns d's announcement with them.
    REQUIRE(net.eventually([&] {
        for (std::size_t i = 0; i < 3; ++i) {
            const auto hosts = net[i].node->hosts();
            const HostInfo* seen = info_of(hosts, d.node->host());
            if (seen == nullptr || !seen->url.empty() || seen->relays != relays) return false;
        }
        return true;
    }));

    // The ledger reaches d (an admin record made on a).
    auto r = a.node->draft("owner-enroll", data::owner_enroll(SigningKey::generate().public_key(), "otto", {}));
    REQUIRE_OK(r);
    r->sign(net.admin);
    REQUIRE_OK(a.node->submit(*r));
    REQUIRE(net.eventually([&] { return d.node->ledger_digest() == a.node->ledger_digest(); }));

    // A paglet moves to d and on, by host name; frames to d went through relays.
    const std::string module = a.f->module("conformance.wasm");
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(net.owner, net.genesis->id(), *parse_key_id(module), paglet_id('a'), Value(),
                                    now - 1000, now + 600'000);
    REQUIRE_OK(passport);
    auto id = a.node->create(module, *passport);
    REQUIRE_OK(id);
    CHECK_EQ(count(*a.f, *id), 1);
    move_to(net, a, d, *id, "d");
    CHECK_EQ(count(*d.f, *id), 2);
    move_to(net, d, net[1], *id, "b");
    CHECK_EQ(count(*net[1].f, *id), 3);
    std::uint64_t relayed = 0;
    for (std::size_t i = 0; i < 3; ++i) relayed += net[i].transport->stats().frames_relayed;
    CHECK(relayed > 0);

    // One of d's relays goes down: d takes another, the others follow, and
    // d keeps taking part.
    std::size_t down = 0;
    for (std::size_t i = 0; i < 3; ++i) {
        if (net[i].node->host() == relays.front()) down = i;
    }
    net.hosts[down]->f->runtime->shutdown();
    net.hosts[down]->transport->stop();
    net.hosts.erase(net.hosts.begin() + static_cast<std::ptrdiff_t>(down));
    auto& d2 = *net.hosts.back();
    REQUIRE(net.eventually([&] {
        const auto now_relays = relays_of(d2);
        return now_relays.size() == 2u && std::ranges::find(now_relays, relays.front()) == now_relays.end() &&
               d2.transport->live_uplinks().size() == 2u;
    }));
    // The reachable host that holds the paglet sends it to d again; d sends
    // it to the other one.
    NetHost& source = net.hosts[0]->f->runtime->info(*id) ? *net.hosts[0] : *net.hosts[1];
    NetHost& back = &source == net.hosts[0].get() ? *net.hosts[1] : *net.hosts[0];
    REQUIRE(source.f->runtime->info(*id).has_value());
    move_to(net, source, d2, *id, "d");
    CHECK_EQ(count(*d2.f, *id), 4);
    const auto d_view = d2.node->hosts();
    move_to(net, d2, back, *id, info_of(d_view, back.node->host())->name);
    CHECK_EQ(count(*back.f, *id), 5);
}

PAGLETS_TEST("relay: two hosts without inbound ports reach each other through relays") {
    Trio net(4, 2);  // a and b can be reached; c and d cannot
    auto& a = net[0];
    auto& c = net[2];
    auto& d = net[3];
    for (std::size_t i = 1; i < 4; ++i) {
        auto contact = net[i].transport->probe(a.transport->url());
        REQUIRE_OK(contact);
        net[i].node->joined(*contact);
    }
    const bool relayed = net.eventually([&] { return relays_of(c).size() == 2u && relays_of(d).size() == 2u; });
    if (!relayed) dump_relays(net);
    REQUIRE(relayed);
    REQUIRE(net.eventually([&] { return net.discovered_except_addresses(); }));
    const std::string module = c.f->module("conformance.wasm");
    const std::int64_t now = unix_ms();
    auto passport = Passport::issue(net.owner, net.genesis->id(), *parse_key_id(module), paglet_id('b'), Value(),
                                    now - 1000, now + 600'000);
    REQUIRE_OK(passport);
    auto id = c.node->create(module, *passport);
    REQUIRE_OK(id);
    CHECK_EQ(count(*c.f, *id), 1);
    move_to(net, c, d, *id, "d");
    CHECK_EQ(count(*d.f, *id), 2);
    move_to(net, d, c, *id, "c");
    CHECK_EQ(count(*c.f, *id), 3);
    // Neither has an address; tunnels carried the moves end to end.
    CHECK(c.transport->stats().tunnels_opened > 0);
    CHECK(d.transport->stats().tunnels_opened > 0);
}
