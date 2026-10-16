// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Session requests for tools (WP32, planning/cpp-web-ui.md): status with
// trust classes and host facts, one-way sends, interface descriptions,
// termination by admins.

#include "mesh_fixture.hpp"
#include "passport_fixture.hpp"
#include "test.hpp"

#include <paglets/abi.hpp>
#include <paglets/client/client.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/interface.hpp>
#include <paglets/wire/json_msgpack.hpp>

using namespace paglets::test;
namespace wi = paglets::wire::interface;

#define CHECK_HAS_VALUE(r) CHECK((r).has_value())

namespace {

Map ask(paglets::node::Node& node, const SigningKey& who, std::string_view role, Map request) {
    auto v = decode(node.answer_session(who.public_key(), role, encode(Value(std::move(request)))));
    REQUIRE(v && v->as_map() != nullptr);
    return *v->as_map();
}

bool ok(const Map& m) {
    const Value* v = Fields{m}.get("ok");
    return v != nullptr && v->as_bool() != nullptr && *v->as_bool();
}

Bytes json_payload(std::string_view text) {
    auto j = paglets::wire::Json::parse(text);
    REQUIRE(j.has_value());
    return paglets::wire::json_to_msgpack(*j);
}

constexpr std::string_view counter_interface = R"({"interface":1,"name":"counter","summary":"Counts",
  "launch":{"type":"none"},
  "messages":[{"name":"increment","kind":"both","request":"Increment","reply":"Status"},
              {"name":"status","kind":"call","request":"none","reply":"Status"}],
  "types":[{"name":"Mode","kind":"enum","values":["add","multiply"]},
           {"name":"Increment","kind":"record","fields":[{"name":"by","type":"i64"},{"name":"note","type":"string"},
                                                        {"name":"mode","type":"Mode"}]},
           {"name":"Status","kind":"record","fields":[{"name":"value","type":"i64"}]}]})";

}  // namespace

PAGLETS_TEST("session: move counters are readable but clearing cached pages requires an admin") {
    Mesh m;
    auto& a = m.add_host("a");
    m.enroll_all();
    const auto read = ask(*a.node, m.owner, "owner", Map{{"t", Value("moves")}});
    REQUIRE(ok(read));
    CHECK_EQ(Fields{read}.integer("memory_limit_pages").value_or(0), 65536);
    CHECK_EQ(Fields{read}.integer("arrival_acks").value_or(-1), 0);
    CHECK(!ok(ask(*a.node, m.owner, "owner", Map{{"t", Value("moves")}, {"clear_cache", Value(true)}})));
    CHECK(ok(ask(*a.node, m.admin, "admin", Map{{"t", Value("moves")}, {"clear_cache", Value(true)}})));
}

PAGLETS_TEST("session: status, send, describe and terminate for tools") {
    Mesh m;
    auto& a = m.add_host("a");
    m.enroll_all();

    // The counter module with an interface description.
    const auto path = guest_path("counter.wasm");
    if (path.empty()) skip("counter.wasm not available");
    auto plain = paglets::wasm::read_file(path);
    REQUIRE_OK(plain);
    auto described = wi::write_custom_section(*plain, wi::section_name, counter_interface);
    REQUIRE_OK(described);
    const paglets::Digest hash = paglets::sha256(*described);
    const Passport passport = m.passport(paglets::to_hex(hash), paglet_id('7'));
    const std::string id = passport.paglet();
    Map launched = ask(*a.node, m.owner, "owner",
                       Map{{"t", Value("launch")},
                           {"passport", Value(passport.encode())},
                           {"module", Value(*described)},
                           {"args", Value(Bytes{})}});
    REQUIRE(ok(launched));
    CHECK(Fields{launched}.str("paglet") == std::optional<std::string>(id));

    // status lists the paglet with its trust class, the native system
    // paglets by name, and facts about the host.
    Map status = ask(*a.node, m.owner, "owner", Map{{"t", Value("status")}});
    REQUIRE(ok(status));
    const Fields sf{status};
    bool listed = false;
    for (const auto& p : *sf.array("paglets")) {
        const Array& e = *p.as_array();
        if (*e[0].as_str() == id) {
            listed = true;
            REQUIRE(e.size() == 5u);
            CHECK_EQ(*e[4].as_str(), std::string("roaming"));
        }
    }
    CHECK(listed);
    auto system = sf.strings("system");
    REQUIRE(system.has_value());
    CHECK(std::ranges::find(*system, std::string("system.mesh-info")) != system->end());
    CHECK(!sf.str("version").value_or("").empty());
    CHECK(!sf.str("os").value_or("").empty());
    CHECK(sf.integer("cpus").value_or(0) > 0);
    CHECK(sf.integer("uptime_s").has_value());

    // describe: of the paglet, of the module (also listed by `modules`).
    Map of_paglet = ask(*a.node, m.owner, "owner", Map{{"t", Value("describe")}, {"paglet", Value(id)}});
    REQUIRE(ok(of_paglet));
    auto text = Fields{of_paglet}.str("interface");
    REQUIRE(text.has_value());
    auto parsed = wi::parse(*text);
    REQUIRE_OK(parsed);
    CHECK_EQ(parsed->find("name")->as_string(), std::string("counter"));
    Map of_module =
        ask(*a.node, m.owner, "owner", Map{{"t", Value("describe")}, {"module", Value(paglets::to_hex(hash))}});
    CHECK(Fields{of_module}.str("interface") == text);
    Map modules = ask(*a.node, m.owner, "owner", Map{{"t", Value("modules")}});
    bool module_listed = false;
    for (const auto& e : *Fields{modules}.array("modules")) {
        const Fields mf{*e.as_map()};
        if (mf.str("hash") == std::optional<std::string>(paglets::to_hex(hash))) {
            module_listed = true;
            CHECK(mf.str("interface") == text);
        }
    }
    CHECK(module_listed);

    // A native system paglet describes its service.
    Map of_system =
        ask(*a.node, m.owner, "owner", Map{{"t", Value("describe")}, {"paglet", Value("system.server-info")}});
    REQUIRE(ok(of_system));
    auto system_interface = wi::parse(Fields{of_system}.str("interface").value_or(""));
    REQUIRE_OK(system_interface);
    CHECK(!system_interface->find("messages")->as_array().empty());

    // A one-way send, then a call sees its effect.
    Map sent = ask(*a.node, m.owner, "owner",
                   Map{{"t", Value("send")},
                       {"paglet", Value(id)},
                       {"name", Value("increment")},
                       {"payload", Value(json_payload(R"({"by":5,"note":"web","mode":"add"})"))},
                       {"priority", Value(std::int64_t{5})}});
    REQUIRE(ok(sent));
    Map called = ask(*a.node, m.owner, "owner",
                     Map{{"t", Value("call")}, {"paglet", Value(id)}, {"name", Value("status")}});
    REQUIRE(ok(called));
    auto reply = paglets::wire::msgpack_to_json(*Fields{called}.bin("payload"));
    REQUIRE(reply.has_value());
    CHECK(reply->dump().find("\"value\":5") != std::string::npos);

    // Another owner may not send; owners may not terminate; admins may.
    const SigningKey stranger = SigningKey::generate();
    CHECK(!ok(ask(*a.node, stranger, "owner",
                  Map{{"t", Value("send")}, {"paglet", Value(id)}, {"name", Value("status")}})));
    CHECK(!ok(ask(*a.node, m.owner, "owner", Map{{"t", Value("terminate")}, {"paglet", Value(id)}})));
    Map terminated = ask(*a.node, m.admin, "admin",
                         Map{{"t", Value("terminate")}, {"paglet", Value(id)}, {"reason", Value("test")}});
    REQUIRE(ok(terminated));
    CHECK(m.settle([&] { return !a.f->runtime->info(id).has_value(); }));
}

PAGLETS_TEST("session: current membership is required after admin removal or owner revocation") {
    Mesh m;
    auto& a = m.add_host("a");
    m.enroll_all();
    const auto next_admin = SigningKey::generate();
    const auto records = ask(*a.node, m.admin, "admin", Map{{"t", Value("records")}});
    Array keep;
    for (const auto& value : *Fields{records}.array("records")) {
        auto record = Record::decode(*value.as_bin());
        REQUIRE_OK(record);
        if (record->signed_by(m.admin.public_key())) keep.push_back(Value::bin(record->id()));
    }
    m.admin_record("admin-set", Map{{"add", Value(Array{Value::bin(next_admin.public_key())})},
        {"remove", Value(Array{Value(Map{{"key", Value::bin(m.admin.public_key())}, {"keep", Value(keep)}})})}});
    REQUIRE(!a.node->state().is_admin(m.admin.public_key()));
    REQUIRE(a.node->state().is_admin(next_admin.public_key()));
    CHECK(!ok(ask(*a.node, m.admin, "admin", Map{{"t", Value("status")}})));
    CHECK(ok(ask(*a.node, next_admin, "admin", Map{{"t", Value("status")}})));
    REQUIRE(a.node->state().is_owner(m.owner.public_key()));
    auto revoke = a.node->draft("revoke", data::revoke_key(m.owner.public_key(), "left"));
    REQUIRE_OK(revoke);
    revoke->sign(next_admin);
    REQUIRE_OK(a.node->submit(*revoke));
    REQUIRE(!a.node->state().is_owner(m.owner.public_key()));
    CHECK(!ok(ask(*a.node, m.owner, "owner", Map{{"t", Value("status")}})));
    CHECK(!ok(ask(*a.node, next_admin, "invalid", Map{{"t", Value("status")}})));
    CHECK(ok(ask(*a.node, next_admin, "admin", Map{{"t", Value("status")}})));
}

PAGLETS_TEST("session: tool sender identifies the authenticated actor without system trust") {
    namespace pc = paglets::client;
    std::array<std::uint8_t, 32> admin_seed{}, owner_seed{}, other_seed{};
    admin_seed[0] = 31;
    owner_seed[0] = 32;
    other_seed[0] = 33;
    auto admin = SigningKey::from_seed(admin_seed);
    auto owner = SigningKey::from_seed(owner_seed);
    auto other = SigningKey::from_seed(other_seed);
    REQUIRE_OK(admin);
    REQUIRE_OK(owner);
    REQUIRE_OK(other);
    auto genesis = make_genesis("tool-sender", {&*admin}, {}, 1);
    REQUIRE_OK(genesis);
    NetHost host(*genesis);
    std::vector<Record> records;
    auto enroll = [&](std::string type, Map data) {
        auto record = host.node->draft(type, std::move(data));
        REQUIRE_OK(record);
        record->sign(*admin);
        REQUIRE_OK(host.node->submit(*record));
        records.push_back(*record);
    };
    enroll("host-enroll", data::host_enroll(host.node->host(), "host", {}));
    enroll("owner-enroll", data::owner_enroll(owner->public_key(), "owner", {}));
    enroll("owner-enroll", data::owner_enroll(other->public_key(), "other", {}));
    auto identity = [&](const auto& seed, paglets::net::PeerRole role) {
        auto ledger = Ledger::create(*genesis);
        REQUIRE_OK(ledger);
        for (const auto& record : records) REQUIRE_OK(ledger->add(record));
        auto key = SigningKey::from_seed(seed);
        REQUIRE_OK(key);
        return pc::Identity{std::move(*ledger), std::move(*key), role, "test"};
    };
    auto owner_identity = identity(owner_seed, paglets::net::PeerRole::owner);
    auto admin_identity = identity(admin_seed, paglets::net::PeerRole::admin);
    auto other_identity = identity(other_seed, paglets::net::PeerRole::owner);
    auto owner_session = pc::Session::open(host.transport->url(), owner_identity);
    auto admin_session = pc::Session::open(host.transport->url(), admin_identity);
    auto other_session = pc::Session::open(host.transport->url(), other_identity);
    REQUIRE_OK(owner_session);
    REQUIRE_OK(admin_session);
    REQUIRE_OK(other_session);
    auto bytes = paglets::wasm::read_file(guest_path("conformance.wasm"));
    REQUIRE_OK(bytes);
    const auto hash = paglets::sha256(*bytes);
    auto launched = owner_session->launch(owner_identity, hash, std::move(*bytes), {});
    REQUIRE_OK(launched);
    const auto id = *launched;
    auto check = [&](const abi::SenderRecord& sender, const std::string& actor) {
        CHECK_EQ(sender.id, std::string("host"));
        CHECK_EQ(sender.trust, std::string("host"));
        CHECK_EQ(sender.owner, actor);
        CHECK(sender.module.empty());
        CHECK_EQ(sender.host, key_id(host.node->host()));
    };
    for (const auto& [session, actor] : std::array{
             std::pair{&*owner_session, key_id(owner->public_key())},
             std::pair{&*admin_session, key_id(admin->public_key())}}) {
        auto called = session->call(id, "sender_now", {});
        REQUIRE_OK(called);
        REQUIRE(called->status == 0);
        check(dec<abi::SenderRecord>(called->payload), actor);
        REQUIRE_OK(session->send(id, "remember_sender", {}));
        auto stored = session->call(id, "stored_sender", {});
        REQUIRE_OK(stored);
        check(dec<abi::SenderRecord>(stored->payload), actor);
        auto forged = session->ask(Map{{"t", Value("call")}, {"paglet", Value(id)},
                                      {"name", Value("sender_now")}, {"owner", Value("forged")},
                                      {"sender", Value(Map{{"trust", Value("system")}, {"owner", Value("forged")}})}});
        REQUIRE_OK(forged);
        auto payload = Fields{*forged}.bin("payload");
        REQUIRE(payload.has_value());
        check(dec<abi::SenderRecord>(*payload), actor);
    }
    CHECK(!other_session->call(id, "sender_now", {}));
    CHECK(!other_session->send(id, "remember_sender", {}));
    // An embedder has a local tool identity; native services remain system.
    check(dec<abi::SenderRecord>(host.f->call(id, "sender_now").payload), "local");
    REQUIRE_OK(host.f->runtime->send(id, "remember_sender"));
    check(dec<abi::SenderRecord>(host.f->call(id, "stored_sender").payload), "local");
    struct NativeSender final : rt::SystemPaglet {
        std::string_view name() const override { return "sender-test"; }
        void handle(rt::SystemContext& ctx, rt::ServiceCall& call) override {
            const std::string target(call.payload().begin(), call.payload().end());
            auto sent = ctx.send(target, "remember_sender", {});
            (void)call.reply(sent ? abi::ok : sent.error(), {});
        }
    };
    auto service = host.f->runtime->add_system_paglet(std::make_shared<NativeSender>());
    REQUIRE_OK(service);
    REQUIRE(host.f->call(*service, "send", text(id)).status == 0);
    const auto native = dec<abi::SenderRecord>(host.f->call(id, "stored_sender").payload);
    CHECK_EQ(native.id, std::string("system.sender-test"));
    CHECK_EQ(native.trust, std::string("system"));
}


PAGLETS_TEST("owner identities: launch nonces cannot claim another owner's global ID") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const auto other = SigningKey::generate();
    net.admin_record("owner-enroll", data::owner_enroll(other.public_key(), "other", {}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    const auto module = a.f->module("conformance.wasm");
    CHECK_EQ(b.f->module("conformance.wasm"), module);
    const auto first = net.passport(module, "same-launch-nonce");
    auto id = a.node->create(module, first);
    REQUIRE_OK(id);
    const auto now = unix_ms();
    auto second = Passport::issue(other, net.genesis->id(), *parse_key_id(module), "same-launch-nonce",
                                   Value(), now - 1000, now + 600'000);
    REQUIRE_OK(second);
    CHECK(second->paglet() != *id);
    const auto launched = ask(*b.node, other, "owner",
                              Map{{"t", Value("launch")}, {"passport", Value(second->encode())}});
    REQUIRE(ok(launched));
    CHECK(Fields{launched}.str("paglet") == std::optional<std::string>(second->paglet()));
    REQUIRE(a.f->runtime->info(*id).has_value());
    REQUIRE(b.f->runtime->info(second->paglet()).has_value());
    CHECK_EQ(a.f->runtime->info(*id)->owner, net.owner.id());
    CHECK_EQ(b.f->runtime->info(second->paglet())->owner, other.id());
    CHECK(!b.f->runtime->info(*id));
    CHECK(!ok(ask(*a.node, other, "owner", Map{{"t", Value("dispose")}, {"paglet", Value(*id)}})));
    auto targeted = Passport::issue(other, net.genesis->id(), *parse_key_id(module), *id,
                                     Value(), now - 1000, now + 600'000);
    REQUIRE_OK(targeted);
    CHECK(targeted->paglet() != *id);  // Even the victim's ID is just a nonce.
    auto repeated = Passport::issue(net.owner, net.genesis->id(), paglets::sha256(Bytes{1}), "same-launch-nonce",
                                     Value(), now, now + 700'000);
    REQUIRE_OK(repeated);
    CHECK_EQ(repeated->paglet(), *id);  // module/expiry changes do not change identity
    CHECK(!a.node->create(module, first));  // local nonce reuse still refuses duplicates
}

PAGLETS_TEST("owner identities: valid signatures cannot forge a bound ID or downgrade a new launch") {
    Mesh net;
    auto& a = net.add_host("a");
    auto& b = net.add_host("b");
    net.enroll_all();
    const auto module = a.f->module("conformance.wasm");
    const auto hash = *parse_key_id(module);
    const auto now = unix_ms();
    const auto valid = net.passport(module, "valid");
    auto envelope = decode(valid.encode());
    REQUIRE_OK(envelope);
    auto body = decode(*envelope->find("root")->find("body")->as_bin());
    REQUIRE_OK(body);
    const auto changed = [&](std::string_view key, Value value, bool bound = true) {
        Map fields = *body->as_map();
        for (auto& [name, old] : fields) if (name == key) old = std::move(value);
        return Passport::decode(sign_passport_root(net.owner, std::move(fields), bound));
    };
    CHECK(!changed("paglet", Value(paglet_id('a'))));
    CHECK(!changed("owner", Value::bin(SigningKey::generate().public_key())));
    CHECK(!changed("mesh", Value::bin(paglets::sha256(Bytes{1}))));
    CHECK(!changed("nonce", Value("a different nonce")));
    CHECK(!changed("nonce", Value("")));
    CHECK(!changed("nonce", Value(std::string(129, 'x'))));
    CHECK(!changed("v", Value(std::int64_t{1})));
    CHECK(!changed("v", Value(std::int64_t{3})));
    CHECK(!changed("v", Value("2")));
    CHECK(!changed("v", Value(std::int64_t{2}), false));  // wrong signature domain
    CHECK(!Passport::issue(net.owner, net.genesis->id(), hash, "", Value(), now, now + 1000));
    CHECK(!Passport::issue(net.owner, net.genesis->id(), hash, std::string(129, 'x'), Value(), now, now + 1000));
    const auto old_bytes = legacy_passport(net.owner, net.genesis->id(), hash, paglet_id('a'), now);
    auto old = Passport::decode(old_bytes);
    REQUIRE_OK(old);  // decoding old saved state remains supported
    REQUIRE_OK(verify_passport(*old, a.node->state(), old->paglet(), hash, now));
    CHECK(!a.node->create(module, *old));
    CHECK(!ok(ask(*a.node, net.owner, "owner", Map{{"t", Value("launch")}, {"passport", Value(old_bytes)}})));
    const auto remote = net.launch(a, b, *old);
    CHECK(remote.state == Launch::State::failed);
    CHECK(remote.error.find("owner-bound") != std::string::npos);
    CHECK(b.f->runtime->modules().list().empty());  // refused before fetching code
}

PAGLETS_TEST("owner identities: saved v1 paglets keep their IDs through restart and moves") {
    namespace fs = std::filesystem;
    const auto directory = fs::temp_directory_path() / ("paglets-old-identity-" + std::to_string(std::random_device{}()));
    {
        Mesh net;
        net.configure = [](const std::string&, rt::Config& config) { config.checkpoint_interval = 0ms; };
        auto& a = net.add_host("a", {}, directory / "a");
        auto& b = net.add_host("b", {}, directory / "b");
        net.enroll_all();
        const auto module = a.f->module("conformance.wasm");
        const auto id = paglet_id('a');
        const auto bytes = legacy_passport(net.owner, net.genesis->id(), *parse_key_id(module), id, unix_ms());
        // Simulate state saved by a v1 host, using the low-level local runtime.
        auto created = a.f->runtime->create(module, rt::CreateOptions{.owner = net.owner.id(), .id = id});
        REQUIRE_OK(created);
        const auto passports = a.state / "node" / "passports";
        fs::create_directories(passports);
        REQUIRE_OK(paglets::wasm::write_file((passports / (id + ".passport")).string(), bytes));
        CHECK(!fs::exists(a.state / "node" / "node.state"));
        CHECK_EQ(dec<std::int64_t>(a.f->call(id, "count").payload), 1);
        REQUIRE(a.f->runtime->wait_idle(5s));
        net.restart(a);
        REQUIRE(net.settle([&] { return net.converged(); }));
        REQUIRE(a.node->passport(id).has_value());
        CHECK(!a.node->passport(id)->root().nonce);
        CHECK_EQ(dec<std::int64_t>(a.f->call(id, "count").payload), 2);
        REQUIRE_OK(a.node->dispatch(id, "b"));
        REQUIRE(net.settle([&] { return b.f->runtime->info(id).has_value() && !a.f->runtime->info(id); }, 2500));
        CHECK_EQ(b.f->runtime->info(id)->owner, net.owner.id());
        CHECK_EQ(dec<std::int64_t>(b.f->call(id, "count").payload), 3);
        REQUIRE(b.f->runtime->wait_idle(5s));
        CHECK(fs::exists(b.state / "node" / "node.state"));
        net.restart(b);
        REQUIRE(net.settle([&] { return net.converged(); }));
        REQUIRE(b.node->passport(id).has_value());
        CHECK(!b.node->passport(id)->root().nonce);
        CHECK_EQ(dec<std::int64_t>(b.f->call(id, "count").payload), 4);
        const auto located = b.node->locate(id);
        REQUIRE(net.settle([&] { return b.node->located(located)->state != paglets::node::Node::Located::State::pending; }));
        CHECK_EQ(b.node->located(located)->moves, 1u);
        REQUIRE_OK(b.node->dispatch(id, "a"));
        REQUIRE(net.settle([&] { return a.f->runtime->info(id).has_value() && !b.f->runtime->info(id); }, 2500));
        CHECK_EQ(dec<std::int64_t>(a.f->call(id, "count").payload), 5);
    }
    fs::remove_all(directory);
}


PAGLETS_TEST("owner identities: a new root saves holder metadata before any grant or outgoing move") {
    namespace fs = std::filesystem;
    const auto directory = fs::temp_directory_path() / ("paglets-new-identity-" + std::to_string(std::random_device{}()));
    {
        Mesh net;
        net.configure = [](const std::string&, rt::Config& config) { config.checkpoint_interval = 0ms; };
        auto& a = net.add_host("a", {}, directory / "a");
        net.add_host("b");
        net.enroll_all();
        const auto module = a.f->module("conformance.wasm");
        auto id = a.node->create(module, net.passport(module, "new-durable-root"));
        REQUIRE_OK(id);
        CHECK(fs::exists(a.state / "node" / "node.state"));
        CHECK_EQ(dec<std::int64_t>(a.f->call(*id, "count").payload), 1);
        REQUIRE(a.f->runtime->wait_idle(5s));
        net.restart(a);
        REQUIRE(net.settle([&] { return net.converged(); }));
        REQUIRE(a.node->passport(*id).has_value());
        CHECK(a.node->passport(*id)->root().nonce == std::optional<std::string>("new-durable-root"));
        CHECK_EQ(dec<std::int64_t>(a.f->call(*id, "count").payload), 2);
    }
    fs::remove_all(directory);
}
