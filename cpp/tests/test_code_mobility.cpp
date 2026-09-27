// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Code mobility (WP11, planning/cpp-modules.md, section 5): hosts fetch
// modules they lack from their sources and from other hosts, verify them,
// and run paglets launched from another host. The exit scenario: a host with
// no application code receives and runs a paglet.

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/mesh/gossip.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/passport.hpp>
#include <paglets/node/node.hpp>
#include <paglets/services/system_services.hpp>

#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <tuple>

using namespace paglets::test;
using namespace paglets::mesh;
namespace ps = paglets::services;
namespace fs = std::filesystem;
using Launch = paglets::node::Node::LaunchStatus;

namespace {

std::string frame_type(const Bytes& frame) {
    auto v = decode(frame);
    if (!v || v->as_map() == nullptr) return {};
    return Fields{*v->as_map()}.str("t").value_or("");
}

// Hosts connected by an in-memory network. `filter` sees every frame before
// delivery and may change or drop it.
struct Mesh {
    struct Port final : GossipTransport {
        Mesh* net = nullptr;
        PublicKey self{};
        void send(const PublicKey& to, Bytes frame) override { net->queue.emplace_back(self, to, std::move(frame)); }
    };

    struct Host {
        std::string name;
        PublicKey key{};
        Port port;
        std::unique_ptr<Fixture> f;
        std::shared_ptr<ps::SystemServices> services;
        std::unique_ptr<paglets::node::Node> node;
    };

    SigningKey admin = SigningKey::generate();
    SigningKey owner = SigningKey::generate();
    std::optional<Record> genesis;
    std::deque<std::tuple<PublicKey, PublicKey, Bytes>> queue;
    std::vector<std::unique_ptr<Host>> hosts;
    std::function<bool(const PublicKey& from, const PublicKey& to, Bytes& frame)> filter;
    std::map<std::string, int> sent;  // frames by type

    Mesh() {
        auto g = make_genesis("mobility", {&admin}, {}, 1);
        REQUIRE_OK(g);
        genesis = *g;
    }

    ~Mesh() {
        for (auto& h : hosts) h->node.reset();
    }

    Host& add_host(const std::string& name) {
        auto h = std::make_unique<Host>();
        h->name = name;
        SigningKey key = SigningKey::generate();
        h->key = key.public_key();
        h->port.net = this;
        h->port.self = h->key;
        h->f = std::make_unique<Fixture>();
        auto installed = ps::install_system_services(*h->f->runtime, ps::ServicesConfig{});
        REQUIRE_OK(installed);
        h->services = *installed;
        auto ledger = Ledger::create(*genesis);
        REQUIRE_OK(ledger);
        h->node = std::make_unique<paglets::node::Node>(*h->f->runtime, h->services, std::move(*ledger), std::move(key),
                                                        h->port);
        REQUIRE_OK(h->node->start());
        for (auto& other : hosts) {
            h->node->add_seed(other->key);
            other->node->add_seed(h->key);
        }
        hosts.push_back(std::move(h));
        return *hosts.back();
    }

    Host* find(const PublicKey& key) {
        for (auto& h : hosts) {
            if (h->key == key) return h.get();
        }
        return nullptr;
    }

    // An admin record made on the first host.
    void admin_record(const std::string& type, Map data) {
        auto r = hosts.front()->node->draft(type, std::move(data));
        REQUIRE_OK(r);
        r->sign(admin);
        REQUIRE_OK(hosts.front()->node->submit(*r));
    }

    void enroll_all() {
        for (auto& h : hosts) admin_record("host-enroll", data::host_enroll(h->key, h->name, {}));
        admin_record("owner-enroll", data::owner_enroll(owner.public_key(), "olga", {}));
        REQUIRE(settle([] { return false; }, 40) || converged());
    }

    bool converged() {
        for (auto& h : hosts) {
            if (h->node->ledger_digest() != hosts.front()->node->ledger_digest()) return false;
        }
        return true;
    }

    void deliver() {
        while (!queue.empty()) {
            auto [from, to, frame] = std::move(queue.front());
            queue.pop_front();
            ++sent[frame_type(frame)];
            if (filter && !filter(from, to, frame)) continue;
            if (Host* h = find(to)) h->node->receive(from, frame);
        }
    }

    // Delivers and ticks until `done` holds (or rounds run out).
    bool settle(const std::function<bool()>& done, int rounds = 200) {
        for (int i = 0; i < rounds; ++i) {
            deliver();
            if (done()) return true;
            for (auto& h : hosts) h->node->tick();
            std::this_thread::sleep_for(2ms);
        }
        deliver();
        return done();
    }

    Passport passport(const std::string& module, std::string paglet) {
        const std::int64_t now = unix_ms();
        auto p = Passport::issue(owner, genesis->id(), *parse_key_id(module), std::move(paglet), Value(), now - 1000,
                                 now + 600'000);
        REQUIRE_OK(p);
        return *p;
    }

    Launch launch(Host& from, Host& to, const Passport& p, Bytes args = {}) {
        auto id = from.node->launch(to.key, p, std::move(args));
        REQUIRE_OK(id);
        REQUIRE(settle([&] { return from.node->launch_status(*id)->state != Launch::State::pending; }));
        return *from.node->launch_status(*id);
    }
};

std::string paglet_id(char c) {
    return std::string(32, c);
}

}  // namespace

PAGLETS_TEST("code mobility: a host without application code receives and runs a paglet (WP11 exit)") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");
    CHECK(b.f->runtime->modules().list().empty());  // no application code on b

    const auto p = net.passport(module, paglet_id('1'));
    const Launch status = net.launch(a, b, p, text("hi"));
    REQUIRE(status.state == Launch::State::running);
    CHECK_EQ(status.paglet, p.paglet());
    const auto info = b.f->runtime->info(p.paglet());
    REQUIRE(info.has_value());
    CHECK_EQ(info->module, module);
    CHECK_EQ(info->owner, net.owner.id());
    CHECK(info->trust == rt::TrustClass::roaming);
    CHECK(b.node->passport(p.paglet()).has_value());
    CHECK_EQ(b.f->call(p.paglet(), "count").status, 0);
    CHECK(contains(b.f->journal(p.paglet()), "event:created:hi:0"));
    CHECK_EQ(b.f->runtime->modules().entry(module)->users, 1u);
    CHECK_EQ(net.sent["module-want"], 1);

    // The module is on b now: the next launch needs no transfer.
    const Launch again = net.launch(a, b, net.passport(module, paglet_id('2')));
    CHECK(again.state == Launch::State::running);
    CHECK_EQ(net.sent["module-want"], 1);
    CHECK(!a.f->runtime->info(p.paglet()).has_value());  // it runs on b only
}

PAGLETS_TEST("code mobility: modules come from other hosts when the source lacks them or lies") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    auto& c = net.add_host("c");
    net.enroll_all();
    const std::string module = c.f->module("conformance.wasm");
    a.f->module("hello.wasm");
    b.node->set_fetch_timeout(100ms);

    // a launches a paglet whose module it does not have: b asks a, then c.
    Launch s = net.launch(a, b, net.passport(module, paglet_id('1')));
    REQUIRE(s.state == Launch::State::running);
    CHECK(b.f->runtime->modules().contains(module));

    // A host that sends a damaged module is skipped for the next one.
    auto& d = net.add_host("d");
    net.admin_record("host-enroll", data::host_enroll(d.key, "d", {}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    a.f->module("conformance.wasm");
    net.filter = [&](const PublicKey& from, const PublicKey&, Bytes& frame) {
        if (from == a.key && frame_type(frame) == "module-part") {
            auto v = decode(frame);
            Map m = *v->as_map();
            for (auto& [k, value] : m) {
                if (k == "d") {
                    Bytes data = *value.as_bin();
                    data[data.size() / 2] ^= 0xff;
                    value = Value(std::move(data));
                }
            }
            frame = encode(Value(std::move(m)));
        }
        return true;
    };
    int wants = net.sent["module-want"];
    s = net.launch(a, d, net.passport(module, paglet_id('2')));
    REQUIRE(s.state == Launch::State::running);
    CHECK_EQ(net.sent["module-want"], wants + 2);  // a (damaged), then another host
    CHECK(d.f->runtime->modules().contains(module));
    auto stored = d.f->runtime->modules().bytes(module);
    REQUIRE_OK(stored);
    CHECK(paglets::to_hex(paglets::sha256(*stored)) == module);

    // A host that does not answer is skipped after the timeout.
    auto& e = net.add_host("e");
    net.admin_record("host-enroll", data::host_enroll(e.key, "e", {}));
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        return !(to == a.key && frame_type(frame) == "module-want");
    };
    REQUIRE(net.settle([&] { return net.converged(); }));
    e.node->set_fetch_timeout(50ms);
    wants = net.sent["module-want"];
    s = net.launch(a, e, net.passport(module, paglet_id('3')));
    REQUIRE(s.state == Launch::State::running);
    CHECK_EQ(net.sent["module-want"], wants + 2);  // a (silent), then another host
    CHECK(e.f->runtime->modules().contains(module));
    net.filter = nullptr;

    // Nobody has it: the launch fails and says why.
    const std::string unknown(64, 'a');
    auto p = Passport::issue(net.owner, net.genesis->id(), *parse_key_id(unknown), paglet_id('4'), Value(),
                             unix_ms() - 1000, unix_ms() + 600'000);
    REQUIRE_OK(p);
    s = net.launch(a, b, *p);
    REQUIRE(s.state == Launch::State::failed);
    CHECK(s.error.find("not found") != std::string::npos);
    CHECK(!b.node->fetching(*parse_key_id(unknown)));
}

PAGLETS_TEST("code mobility: launches are checked before any code moves") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const std::string module = a.f->module("conformance.wasm");

    // Untrusted modules are refused without a transfer.
    net.admin_record("module-policy", data::module_policy(RoamingModules::trusted));
    REQUIRE(net.settle([&] { return net.converged(); }));
    Launch s = net.launch(a, b, net.passport(module, paglet_id('1')));
    REQUIRE(s.state == Launch::State::failed);
    CHECK(s.error.find("not trusted") != std::string::npos);
    CHECK_EQ(net.sent["module-want"], 0);
    CHECK(!b.f->runtime->modules().contains(module));
    net.admin_record("module-trust", data::module_trust("conformance", {}, {*parse_key_id(module)}, {"roaming"}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    CHECK(net.launch(a, b, net.passport(module, paglet_id('1'))).state == Launch::State::running);

    // Passports of owners that are not enrolled, and existing paglet IDs.
    const SigningKey stranger = SigningKey::generate();
    auto p = Passport::issue(stranger, net.genesis->id(), *parse_key_id(module), paglet_id('2'), Value(),
                             unix_ms() - 1000, unix_ms() + 600'000);
    REQUIRE_OK(p);
    s = net.launch(a, b, *p);
    REQUIRE(s.state == Launch::State::failed);
    CHECK(s.error.find("passport") != std::string::npos);
    s = net.launch(a, b, net.passport(module, paglet_id('1')));
    REQUIRE(s.state == Launch::State::failed);
    CHECK(s.error.find("exists") != std::string::npos);

    // Only enrolled hosts launch paglets or receive modules.
    CHECK(!a.node->launch(SigningKey::generate().public_key(), net.passport(module, paglet_id('3'))));
    const SigningKey outsider = SigningKey::generate();
    std::vector<std::string> answers;
    net.filter = [&](const PublicKey&, const PublicKey& to, Bytes& frame) {
        if (to == outsider.public_key()) {
            auto v = decode(frame);
            answers.push_back(frame_type(frame) + ":" + Fields{*v->as_map()}.str("e").value_or(""));
        }
        return true;
    };
    b.node->receive(outsider.public_key(),
                    encode(Value(Map{{"t", Value("launch")},
                                     {"r", Value(std::int64_t{7})},
                                     {"p", Value(net.passport(module, paglet_id('3')).encode())}})));
    b.node->receive(outsider.public_key(), encode(Value(Map{{"t", Value("module-want")},
                                                            {"m", Value::bin(*parse_key_id(module))},
                                                            {"r", Value(std::int64_t{8})}})));
    net.deliver();
    const std::vector<std::string> expected{"launch-result:launch from a host that is not enrolled",
                                            "module-missing:not an enrolled host"};
    CHECK(answers == expected);
    CHECK(!b.f->runtime->info(paglet_id('3')).has_value());
}

PAGLETS_TEST("code mobility: module sources and launches on the host itself") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const auto path = guest_path("conformance.wasm");
    if (path.empty()) skip("conformance.wasm not available");
    const std::string module = paglets::to_hex(paglets::sha256(*paglets::wasm::read_file(path)));

    // b takes the module from a directory of modules; no host is asked.
    b.node->add_module_source(std::make_shared<rt::DirectorySource>(fs::path(path).parent_path()));
    Launch s = net.launch(b, b, net.passport(module, paglet_id('1')));
    REQUIRE(s.state == Launch::State::running);
    CHECK_EQ(net.sent["module-want"], 0);
    CHECK_EQ(b.f->call(s.paglet, "count").status, 0);

    // a has neither the module nor a source: it asks b.
    s = net.launch(a, a, net.passport(module, paglet_id('2')));
    REQUIRE(s.state == Launch::State::running);
    CHECK_EQ(net.sent["module-want"], 1);
    CHECK_EQ(a.f->call(s.paglet, "count").status, 0);

    // Fetching alone, without a launch.
    const std::string hello = b.f->module("hello.wasm");
    a.node->fetch_module(*parse_key_id(hello));
    REQUIRE(net.settle([&] { return a.f->runtime->modules().contains(hello); }));
    CHECK(!a.node->fetching(*parse_key_id(hello)));
}
