// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Fuzzing the decoders (WP21, planning/cpp-hardening.md): wire formats,
// ledger records, passports, travelling state, memory images, Wasm
// modules, contracts, the Noise handshake, frames between hosts, JSON, and
// the web gateway's URL and HTML handling. Each target runs for
// PAGLETS_FUZZ_SECONDS (default 2) on mutations of valid inputs; see
// fuzz.hpp.

#include "fuzz.hpp"
#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/gateway/gateway.hpp>
#include <paglets/mesh/passport.hpp>
#include <paglets/net/noise.hpp>
#include <paglets/runtime/mobility.hpp>
#include <paglets/services/ai.hpp>
#include <paglets/services/files.hpp>
#include <paglets/services/mesh_info.hpp>
#include <paglets/services/web.hpp>
#include <paglets/wasm/binary.hpp>
#include <paglets/wasm/direct.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wasm/snapshot.hpp>
#include <paglets/wire/json.hpp>
#include <paglets/wire/reflect.hpp>

using namespace paglets::test;
namespace fz = paglets::fuzz;
namespace wire = paglets::wire;
namespace pw = paglets::wasm;
using fz::Bytes;

namespace {

Bytes bytes_of(std::string_view s) {
    return Bytes(s.begin(), s.end());
}

// Values of every msgpack kind, nested.
Bytes sample_value() {
    Map inner{{"n", Value(std::int64_t{-12345678901})}, {"f", Value(true)}, {"s", Value("text")}};
    Array list{Value(std::int64_t{1}), Value(), Value(Bytes{1, 2, 3}), Value(std::move(inner))};
    return encode(Value(Map{{"list", Value(std::move(list))}, {"big", Value(std::int64_t{1} << 40)}}));
}

}  // namespace

PAGLETS_TEST("fuzz: msgpack and mesh values decode safely and round-trip") {
    const std::vector<Bytes> seeds{sample_value(), encode(Value(Array{})), encode(Value("x"))};
    fz::run("mesh-value", seeds, [](std::span<const std::uint8_t> in) {
        paglets::msgpack::Reader r(in);
        while (r.ok() && !r.at_end() && r.skip()) {
        }
        auto v = paglets::mesh::decode(in);
        if (!v) return;
        // What decodes, encodes to bytes that decode to the same value.
        auto again = paglets::mesh::decode(encode(*v));
        if (!again || !(*again == *v)) {
            std::cerr << "    round trip differs\n";
            std::abort();
        }
    });
}

PAGLETS_TEST("fuzz: ledger records, passports and ledger derivation") {
    const SigningKey admin = SigningKey::generate();
    const SigningKey owner = SigningKey::generate();
    auto genesis = make_genesis("fuzz", {&admin}, {}, 1);
    REQUIRE_OK(genesis);
    auto created = Ledger::create(*genesis);
    REQUIRE_OK(created);
    Ledger ledger = std::move(*created);
    std::vector<Bytes> seeds{genesis->encode()};
    auto add = [&](const std::string& type, Map data) {
        auto r = ledger.draft(type, std::move(data));
        REQUIRE_OK(r);
        r->sign(admin);
        REQUIRE_OK(ledger.add(*r));
        seeds.push_back(r->encode());
    };
    add("owner-enroll", data::owner_enroll(owner.public_key(), "olga", {"lab"}));
    add("host-enroll", data::host_enroll(SigningKey::generate().public_key(), "h", {"gpu"}));
    add("policy-rule", data::policy_rule({"r", Decision::allow, "files", {"read"}, {}, std::nullopt, std::nullopt, 0}));
    add("root-residency", data::root_residency("vault", "host-only"));
    auto passport = Passport::issue(owner, genesis->id(), *parse_key_id(std::string(64, 'c')), std::string(32, 'a'),
                                    Value(), 0, 1000);
    REQUIRE_OK(passport);
    seeds.push_back(passport->encode());
    const RecordId mesh = genesis->id();
    const Record genesis_record = *genesis;
    fz::run("records", seeds, [&](std::span<const std::uint8_t> in) {
        (void)Passport::decode(in);
        auto r = Record::decode(in);
        if (!r) return;
        (void)r->encode();
        // A forged or damaged record among valid ones: derivation must hold.
        std::vector<const Record*> records{&genesis_record, &*r};
        (void)derive_state(mesh, records);
    });
}

PAGLETS_TEST("fuzz: travelling state, capabilities and remote messages") {
    rt::TravelState s;
    s.id = std::string(32, 'b');
    s.module = std::string(64, 'c');
    s.owner = "owner";
    s.next_handle = 7;
    s.services = {{"files", 3}};
    rt::Cap cap;
    cap.kind = rt::Cap::Kind::resource;
    cap.target = "system.files";
    cap.resource_type = "dir";
    cap.resource = "data/docs";
    cap.ops = {"read"};
    cap.id = "c1";
    s.caps = {{3, cap}};
    s.pending = {{9, 1000}};
    s.marks = {"root:data"};
    rt::RemoteMessage msg;
    msg.target = std::string(32, 'd');
    msg.name = "hello";
    msg.payload = {1, 2, 3};
    const std::vector<Bytes> seeds{rt::encode_state(s), rt::encode_caps({cap, cap}), rt::encode_remote(msg)};
    fz::run("mobility", seeds, [](std::span<const std::uint8_t> in) {
        (void)rt::decode_state(in);
        (void)rt::decode_caps(in);
        (void)rt::decode_remote(in);
    });
}

PAGLETS_TEST("fuzz: Wasm modules and memory images") {
    const auto path = guest_path("counter.wasm");
    if (path.empty()) skip("counter.wasm not available");
    auto file = pw::read_file(path);
    REQUIRE_OK(file);
    auto module = pw::Module::load(*file, pw::ImportPolicy::standard());
    REQUIRE_OK(module);
    auto instance = pw::Instance::create(*module);
    REQUIRE_OK(instance);
    pw::DirectHarness harness(**instance);
    REQUIRE_OK(harness.start());
    auto snap = pw::capture(**instance);
    REQUIRE_OK(snap);
    const Bytes image = pw::serialize(*snap);
    fz::run("wasm-module", {*file}, [](std::span<const std::uint8_t> in) { (void)pw::parse_module(in); });
    // Damaged images are refused before anything runs.
    fz::run("memory-image", {image}, [&](std::span<const std::uint8_t> in) {
        auto s = pw::deserialize(in);
        if (!s) return;
        (void)pw::restore(*module, *s);
    });
}

PAGLETS_TEST("fuzz: contracts decode through reflection") {
    namespace mi = paglets::services::mesh_info;
    namespace web = paglets::services::web;
    namespace ai = paglets::services::ai;
    namespace files = paglets::services::files;
    mi::Snapshot snap;
    snap.host = "h";
    snap.offers = {mi::Offer{"ai", {"summarize"}, {mi::Attribute{"context", {}, 8000, true}}}};
    const std::vector<Bytes> seeds{wire::to_msgpack(snap),
                                   wire::to_msgpack(mi::OffersRequest{.service = "ai", .require = {{"x", ">=", {}, 1}}}),
                                   wire::to_msgpack(web::FetchRequest{"https://example.org/", "GET", 10}),
                                   wire::to_msgpack(ai::ExtractRequest{"text", {{"a", "b"}}, "m"}),
                                   wire::to_msgpack(files::FindRequest{})};
    fz::run("contracts", seeds, [](std::span<const std::uint8_t> in) {
        const Bytes b(in.begin(), in.end());
        mi::Snapshot s;
        (void)wire::from_msgpack(b, s);
        mi::OffersRequest o;
        (void)wire::from_msgpack(b, o);
        web::FetchRequest f;
        (void)wire::from_msgpack(b, f);
        ai::ExtractRequest e;
        (void)wire::from_msgpack(b, e);
        files::FindRequest q;
        (void)wire::from_msgpack(b, q);
        abi::Delivered d;
        (void)abi::decode(in, d);
        abi::SelfInfo si;
        (void)abi::decode(in, si);
    });
}

PAGLETS_TEST("fuzz: the Noise handshake refuses damaged messages") {
    namespace noise = paglets::net;
    const auto prologue = bytes_of("paglets fuzz");
    noise::Handshake initiator(noise::Handshake::Role::initiator, noise::KeyPair::generate(), prologue);
    auto m1 = initiator.write_message(bytes_of("hello"));
    REQUIRE_OK(m1);
    noise::Handshake responder(noise::Handshake::Role::responder, noise::KeyPair::generate(), prologue);
    REQUIRE_OK(responder.read_message(*m1));
    auto m2 = responder.write_message(bytes_of("welcome"));
    REQUIRE_OK(m2);
    const noise::KeyPair responder_key = noise::KeyPair::generate();
    fz::run("noise-message-1", {*m1}, [&](std::span<const std::uint8_t> in) {
        noise::Handshake r(noise::Handshake::Role::responder, noise::KeyPair::from_secret(responder_key.secret.bytes()),
                           prologue);
        if (r.read_message(in)) (void)r.write_message({});
    });
    fz::run("noise-message-2", {*m2}, [&](std::span<const std::uint8_t> in) {
        noise::Handshake i(noise::Handshake::Role::initiator, noise::KeyPair::generate(), prologue);
        (void)i.write_message({});
        if (i.read_message(in)) {
            (void)i.write_message({});
            (void)i.split();
        }
    });
}

PAGLETS_TEST("fuzz: JSON, URLs and HTML") {
    namespace gw = paglets::gateway;
    fz::run("json", {bytes_of(R"({"a":[1,2.5,-3e10,"xé\n",true,null,{"b":{}}],"n":18446744073709551615})")},
            [](std::span<const std::uint8_t> in) {
                auto j = wire::Json::parse(std::string_view(reinterpret_cast<const char*>(in.data()), in.size()));
                if (!j) return;
                auto again = wire::Json::parse(j->dump());
                if (!again || again->dump() != j->dump()) std::abort();
            });
    fz::run("url", {bytes_of("https://example.org:8443/a/b?c=d#e"), bytes_of("http://[::ffff:10.0.0.1]/"),
                    bytes_of("../x/./y?z")},
            [](std::span<const std::uint8_t> in) {
                const std::string_view s(reinterpret_cast<const char*>(in.data()), in.size());
                (void)gw::resolve_url("https://example.org/dir/page", s);
                (void)gw::internal_address(s);
                const std::string r = gw::resolve_url(s, "next/../page?q");
                // A resolved URL resolves to itself.
                if (!r.empty() && gw::resolve_url(r, "") != r) std::abort();
            });
    fz::run("html", {bytes_of("<html><title>t &amp; u</title><p>a<b>b</b></p><a href='/x'>l</a><script>s</script>")},
            [](std::span<const std::uint8_t> in) {
                (void)gw::extract_html(std::string_view(reinterpret_cast<const char*>(in.data()), in.size()),
                                       "https://example.org/");
            });
}

PAGLETS_TEST("fuzz: frames between hosts") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    // Real frames as seeds: gossip, mesh-info, a move there and back.
    std::vector<Bytes> seeds;
    net.filter = [&](const PublicKey&, const PublicKey&, Bytes& frame) {
        if (seeds.size() < 400) seeds.push_back(frame);
        return true;
    };
    for (auto& h : net.hosts) h->node->set_compute_timing({.sample = 20ms, .gossip = 20ms, .ttl = 2000ms, .redirect_after = 0ms});
    REQUIRE(net.settle([&] { return a.node->landscape().size() == 2u; }));
    const std::string module = a.f->module("conformance.wasm");
    auto id = a.node->create(module, net.passport(module, paglet_id('f')));
    REQUIRE_OK(id);
    REQUIRE_OK(a.node->dispatch(*id, "b"));
    REQUIRE(net.settle([&] { return b.f->runtime->info(*id).has_value(); }, 400));
    net.filter = nullptr;
    REQUIRE(!seeds.empty());
    // b must survive whatever a sends. a is an enrolled host, so what its
    // frames claim (locations, snapshots) may mislead b about the mesh, but
    // b keeps serving: the paglet it holds answers, and new paglets run.
    fz::run("node-frames", seeds, [&](std::span<const std::uint8_t> in) { b.node->receive(a.key, in); });
    auto r = b.f->runtime->call(*id, "count", {}, 10s);
    CHECK_EQ(r.status, 0);
    auto fresh = b.node->create(module, net.passport(module, paglet_id('e')));
    REQUIRE_OK(fresh);
    CHECK_EQ(b.f->runtime->call(*fresh, "count", {}, 10s).status, 0);
}
