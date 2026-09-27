// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Policy and grants (WP9): rule evaluation, grant validity in the ledger,
// manifests, and the exit scenario with two hosts: a roaming paglet gets
// file access only after a matching rule or an admin approval given from a
// different host; access ends on expiry or revocation everywhere; every
// decision is audited.

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <explorer_msgs.hpp>
#include <paglets/mesh/gossip.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/policy.hpp>
#include <paglets/node/node.hpp>
#include <paglets/services/system_services.hpp>
#include <paglets/wire/reflect.hpp>

#include <deque>
#include <filesystem>
#include <fstream>
#include <random>
#include <tuple>

using namespace paglets::test;
using namespace paglets::mesh;
namespace ps = paglets::services;
namespace wire = paglets::wire;
namespace fs = std::filesystem;

namespace {

struct Keys {
    SigningKey admin = SigningKey::generate();
    SigningKey host_a = SigningKey::generate();
    SigningKey host_b = SigningKey::generate();
    SigningKey owner = SigningKey::generate();
    SigningKey other_owner = SigningKey::generate();
};

Record signed_record(const Ledger& ledger, const std::string& type, Map data,
                     const std::vector<const SigningKey*>& signers, bool with_epoch = true) {
    auto r = ledger.draft(type, std::move(data), with_epoch);
    REQUIRE_OK(r);
    for (const SigningKey* k : signers) r->sign(*k);
    return *r;
}

Record add(Ledger& ledger, const std::string& type, Map data, const std::vector<const SigningKey*>& signers,
           bool with_epoch = true) {
    Record r = signed_record(ledger, type, std::move(data), signers, with_epoch);
    REQUIRE_OK(ledger.add(r));
    return r;
}

// A ledger with an admin, two enrolled hosts (a: linux, b: gpu) and two
// owners (owner in group staff).
Ledger enrolled_ledger(const Keys& k) {
    auto genesis = make_genesis("policy", {&k.admin}, {}, 1);
    REQUIRE_OK(genesis);
    auto ledger = Ledger::create(*genesis);
    REQUIRE_OK(ledger);
    add(*ledger, "host-enroll", data::host_enroll(k.host_a.public_key(), "a", {"linux"}), {&k.admin});
    add(*ledger, "host-enroll", data::host_enroll(k.host_b.public_key(), "b", {"gpu"}), {&k.admin});
    add(*ledger, "owner-enroll", data::owner_enroll(k.owner.public_key(), "olga", {"staff"}), {&k.admin});
    add(*ledger, "owner-enroll", data::owner_enroll(k.other_owner.public_key(), "otto", {}), {&k.admin});
    return std::move(*ledger);
}

Record rule(Ledger& ledger, const Keys& k, data::RuleSpec spec) {
    return add(ledger, "policy-rule", data::policy_rule(spec), {&k.admin});
}

Principal principal_of(const SigningKey& owner, std::string paglet = std::string(32, 'a')) {
    return Principal{std::move(paglet), owner.id(), std::string(64, 'f'), "roaming"};
}

Item files(std::vector<std::string> ops, std::string path, std::string root = "data") {
    return Item{"files", std::move(ops), std::move(root), std::move(path)};
}

bool ignored(const LedgerState& st, const Record& r, std::string_view reason) {
    return std::ranges::any_of(st.ignored, [&](const IgnoredRecord& i) {
        return i.id == r.id() && i.reason.find(reason) != std::string::npos;
    });
}

}  // namespace

PAGLETS_TEST("policy: the most specific rule wins, the default is deny") {
    Keys k;
    Ledger ledger = enrolled_ledger(k);
    const auto olga = principal_of(k.owner);
    const auto otto = principal_of(k.other_owner);
    const PublicKey a = k.host_a.public_key();
    const PublicKey b = k.host_b.public_key();
    CHECK(evaluate(ledger.state(), olga, a, files({"read"}, "docs")).decision == Decision::deny);
    CHECK(!evaluate(ledger.state(), olga, a, files({"read"}, "docs")).rule);

    const Record everyone = rule(ledger, k, {"read files", Decision::allow, "files", {"read"}, {}, {}, {}, 0});
    const Record staff = rule(ledger, k,
                              {"staff asks",
                               Decision::ask,
                               "files",
                               {"read", "write"},
                               Match{.groups = std::vector<std::string>{"staff"}},
                               {},
                               {},
                               0});
    const Record docs = rule(ledger, k,
                             {"olga's docs",
                              Decision::allow,
                              "files",
                              {"*"},
                              Match{.owners = std::vector<PublicKey>{k.owner.public_key()}},
                              Scope{std::vector<std::string>{"data"}, std::vector<std::string>{"docs/**"}},
                              std::int64_t{60'000},
                              0});
    const LedgerState& st = ledger.state();
    REQUIRE(st.rules.size() == 3);
    // Otto: only the general rule matches.
    auto e = evaluate(st, otto, a, files({"read"}, "docs"));
    CHECK(e.decision == Decision::allow && e.rule == everyone.id());
    CHECK(evaluate(st, otto, a, files({"write"}, "docs")).decision == Decision::deny);
    // Olga in docs: owner match plus scope beats the group rule.
    e = evaluate(st, olga, a, files({"write"}, "docs/reports"));
    CHECK(e.decision == Decision::allow && e.rule == docs.id() && e.max_duration_ms == 60'000);
    // Olga elsewhere: the group rule is more specific than the general one.
    e = evaluate(st, olga, a, files({"read"}, "other"));
    CHECK(e.decision == Decision::ask && e.rule == staff.id());
    CHECK(evaluate(st, olga, a, files({"read"}, "docs", "archive")).decision == Decision::ask);  // other root

    // Host selectors and priorities.
    rule(ledger, k,
         {"no gpu hosts", Decision::deny, "*", {"*"}, Match{.hosts = HostSelector{{}, {"gpu"}}}, {}, {}, 10});
    CHECK(evaluate(ledger.state(), olga, b, files({"read"}, "docs")).decision == Decision::deny);
    CHECK(evaluate(ledger.state(), olga, a, files({"read"}, "docs")).decision == Decision::allow);
    // Equal specificity: deny wins over allow.
    rule(ledger, k,
         {"otto no",
          Decision::deny,
          "files",
          {"read"},
          Match{.owners = std::vector<PublicKey>{k.other_owner.public_key()}},
          {},
          {},
          0});
    rule(ledger, k,
         {"otto yes",
          Decision::allow,
          "files",
          {"read"},
          Match{.owners = std::vector<PublicKey>{k.other_owner.public_key()}},
          {},
          {},
          0});
    CHECK(evaluate(ledger.state(), otto, a, files({"read"}, "x")).decision == Decision::deny);
    // Revoked rules no longer count.
    add(ledger, "revoke", data::revoke_record(docs.id(), "done"), {&k.admin});
    CHECK(evaluate(ledger.state(), olga, a, files({"write"}, "docs/reports")).decision == Decision::ask);
    // Rules need admins.
    const Record rogue = add(ledger, "policy-rule",
                             data::policy_rule({"rogue", Decision::allow, "*", {"*"}, {}, {}, {}, 100}), {&k.host_a});
    CHECK(ignored(ledger.state(), rogue, "admin quorum"));
}

PAGLETS_TEST("policy: grants from hosts stay within their rules; revocations and releases end grants") {
    Keys k;
    Ledger ledger = enrolled_ledger(k);
    const Record allow = rule(ledger, k,
                              {"read docs",
                               Decision::allow,
                               "files",
                               {"read"},
                               {},
                               Scope{{}, std::vector<std::string>{"docs/**"}},
                               std::int64_t{60'000},
                               0});
    const auto p = principal_of(k.owner);
    const PublicKey a = k.host_a.public_key();
    const std::int64_t now = unix_ms();
    auto host_grant = [&](const Item& item, std::int64_t expires, const SigningKey& signer,
                          std::optional<RecordId> by_rule) {
        return add(ledger, "grant",
                   data::grant(p, item, HostSelector{{a}, {}}, expires, std::nullopt, by_rule, signer.public_key()),
                   {&signer}, false);
    };
    const Record good = host_grant(files({"read"}, "docs/a"), now + 30'000, k.host_a, allow.id());
    const Record wider = host_grant(files({"write"}, "docs/a"), now + 30'000, k.host_a, allow.id());
    const Record longer = host_grant(files({"read"}, "docs/a"), now + 600'000, k.host_a, allow.id());
    const Record outside = host_grant(files({"read"}, "private"), now + 30'000, k.host_a, allow.id());
    const Record stranger = host_grant(files({"read"}, "docs/a"), now + 30'000, k.owner, allow.id());
    const Record ruleless = host_grant(files({"read"}, "docs/a"), now + 30'000, k.host_a, std::nullopt);
    const LedgerState& st = ledger.state();
    CHECK(st.grants.contains(good.id()));
    CHECK(!st.grants.at(good.id()).by_admin);
    CHECK(ignored(st, wider, "outside its rule"));
    CHECK(ignored(st, longer, "longer than its rule"));
    CHECK(ignored(st, outside, "outside its rule"));
    CHECK(ignored(st, stranger, "not an enrolled host"));
    CHECK(ignored(st, ruleless, "without a rule"));

    // An admin may grant anything; revocations and releases end grants.
    const Record approved =
        add(ledger, "grant", data::grant(p, files({"write"}, "private"), {}, now + 30'000, {}, {}, {}), {&k.admin});
    CHECK(ledger.state().grants.at(approved.id()).by_admin);
    add(ledger, "revoke", data::revoke_record(approved.id(), "no longer"), {&k.admin});
    add(ledger, "grant-release", data::grant_release(good.id()), {&k.host_a}, false);
    CHECK(ledger.state().ended_grants == std::set<RecordId>({approved.id(), good.id()}));
    CHECK(ledger.state().grants.empty());
    // Revoking the rule invalidates grants derived from it.
    const Record again = host_grant(files({"read"}, "docs/b"), now + 30'000, k.host_a, allow.id());
    CHECK(ledger.state().grants.contains(again.id()));
    add(ledger, "revoke", data::revoke_record(allow.id(), "rule withdrawn"), {&k.admin});
    CHECK(!ledger.state().grants.contains(again.id()));
}

PAGLETS_TEST("policy: requests stay pending until an admin decides; audit records need enrolled hosts") {
    Keys k;
    Ledger ledger = enrolled_ledger(k);
    const auto p = principal_of(k.owner);
    const PublicKey a = k.host_a.public_key();
    const Item item = files({"read"}, "docs");
    const Record request =
        add(ledger, "grant-request", data::grant_request(p, a, {item}, 60'000, "report"), {&k.host_a}, false);
    const Record forged =
        add(ledger, "grant-request", data::grant_request(p, a, {item}, 60'000, "forged"), {&k.host_b}, false);
    const Record early =
        add(ledger, "grant-request", data::grant_request(p, k.owner.public_key(), {item}, 60'000, "mission"),
            {&k.owner}, false);
    REQUIRE(ledger.state().grant_requests.size() == 2);
    CHECK(ignored(ledger.state(), forged, "not signed by its requester"));
    CHECK(std::ranges::any_of(ledger.state().grant_requests,
                              [&](const auto& g) { return g.id == early.id() && g.by_owner; }));

    add(ledger, "grant", data::grant(p, item, {}, unix_ms() + 60'000, request.id(), {}, {}), {&k.admin});
    add(ledger, "request-deny", data::request_deny(early.id(), "not now"), {&k.admin});
    CHECK(ledger.state().grant_requests.empty());

    add(ledger, "audit", data::audit(a, p, item, Decision::ask, std::nullopt, std::nullopt, request.id()), {&k.host_a},
        false);
    const Record fake = add(ledger, "audit", data::audit(k.owner.public_key(), p, item, Decision::allow, {}, {}, {}),
                            {&k.owner}, false);
    REQUIRE(ledger.state().audit.size() == 1);
    CHECK(ledger.state().audit[0].decision == Decision::ask && ledger.state().audit[0].request == request.id());
    CHECK(ignored(ledger.state(), fake, "not signed by an enrolled host"));
}

PAGLETS_TEST("policy: manifests and preflight for a target host") {
    Keys k;
    Ledger ledger = enrolled_ledger(k);
    rule(ledger, k,
         {"linux reads", Decision::allow, "files", {"read"}, Match{.hosts = HostSelector{{}, {"linux"}}}, {}, {}, 0});
    rule(ledger, k, {"load anywhere", Decision::allow, "server-info", {"load", "volumes"}, {}, {}, {}, 0});
    const std::vector<Item> manifest{files({"read"}, "shared"), Item{"server-info", {"load"}, {}, {}}};
    // The manifest travels in the passport as a canonical value.
    const Value value = manifest_value(manifest);
    auto parsed = parse_manifest(decode(encode(value)).value());
    REQUIRE(parsed.has_value());
    CHECK(*parsed == manifest);
    const auto p = principal_of(k.owner);
    auto on_a = preflight(ledger.state(), p, *parsed, k.host_a.public_key());
    auto on_b = preflight(ledger.state(), p, *parsed, k.host_b.public_key());
    CHECK(on_a[0].decision == Decision::allow && on_a[1].decision == Decision::allow);
    CHECK(on_b[0].decision == Decision::deny && on_b[1].decision == Decision::allow);
    CHECK(!parse_manifest(Value(Array{Value("not an item")})));
    CHECK(parse_manifest(Value())->empty());
}

// ---------------------------------------------------------------------------
// Two hosts

namespace {

// Host A runs paglets (runtime, system paglets, node); host B is where the
// admin works (a ledger replica). Gossip travels through an in-memory queue.
struct TwoHosts {
    struct Port final : GossipTransport {
        TwoHosts* net = nullptr;
        PublicKey self{};
        void send(const PublicKey& to, Bytes frame) override { net->queue.emplace_back(self, to, std::move(frame)); }
    };

    Keys k;
    SigningKey host_a_key = SigningKey::generate();
    const PublicKey host_a = host_a_key.public_key();
    fs::path root;
    std::unique_ptr<Fixture> f;
    std::shared_ptr<ps::SystemServices> services;
    std::deque<std::tuple<PublicKey, PublicKey, Bytes>> queue;
    Port port_a;
    Port port_b;
    std::unique_ptr<paglets::node::Node> a;
    std::unique_ptr<Ledger> ledger_b;
    std::unique_ptr<Replica> b;

    TwoHosts() {
        static std::mt19937_64 rng(std::random_device{}());
        root = fs::temp_directory_path() / ("paglets-policy-" + std::to_string(rng()));
        fs::create_directories(root / "docs");
        fs::create_directories(root / "public");
        std::ofstream(root / "docs" / "report.txt") << "quarterly numbers";
        std::ofstream(root / "public" / "notice.txt") << "open to all";

        auto genesis = make_genesis("two hosts", {&k.admin}, {}, 1);
        REQUIRE_OK(genesis);
        auto la = Ledger::create(*genesis);
        auto lb = Ledger::create(*genesis);
        REQUIRE_OK(la);
        REQUIRE_OK(lb);
        ledger_b = std::make_unique<Ledger>(std::move(*lb));
        port_a.net = this;
        port_a.self = host_a;
        port_b.net = this;
        port_b.self = k.host_b.public_key();
        b = std::make_unique<Replica>(*ledger_b, k.host_b.public_key(), port_b);
        b->add_seed(host_a);

        f = std::make_unique<Fixture>();
        ps::ServicesConfig sc;
        sc.roots["data"] = root;
        auto installed = ps::install_system_services(*f->runtime, sc);
        REQUIRE_OK(installed);
        services = *installed;
        // The node owns host A's key from now on.
        a = std::make_unique<paglets::node::Node>(*f->runtime, services, std::move(*la), std::move(host_a_key), port_a);
        a->add_seed(k.host_b.public_key());
        REQUIRE_OK(a->start());

        // The admin enrolls both hosts and the owner, working on host B.
        admin("host-enroll", data::host_enroll(host_a, "a", {"linux"}));
        admin("host-enroll", data::host_enroll(k.host_b.public_key(), "b", {}));
        admin("owner-enroll", data::owner_enroll(k.owner.public_key(), "olga", {"staff"}));
        settle();
    }

    ~TwoHosts() {
        a.reset();
        f.reset();
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    Record admin(const std::string& type, Map data) {
        Record r = signed_record(*ledger_b, type, std::move(data), {&k.admin});
        REQUIRE_OK(b->submit(r));
        return r;
    }

    void deliver() {
        while (!queue.empty()) {
            auto [from, to, frame] = std::move(queue.front());
            queue.pop_front();
            if (to == host_a) {
                a->receive(from, frame);
            } else if (to == k.host_b.public_key()) {
                b->receive(from, frame);
            }
        }
    }

    void settle() {
        for (int round = 0; round < 20; ++round) {
            deliver();
            if (a->ledger_digest() == ledger_b->digest()) return;
            a->tick();
            b->tick();
        }
        deliver();
        REQUIRE(a->ledger_digest() == ledger_b->digest());
    }

    rt::PagletId explorer() {
        auto id = f->runtime->create(f->module("explorer.wasm"),
                                     rt::CreateOptions{{}, rt::TrustClass::roaming, k.owner.id()});
        REQUIRE_OK(id);
        return *id;
    }

    template <class Reply = std::string, class Request>
    Reply ask(const rt::PagletId& paglet, std::string_view op, const Request& request) {
        auto r = f->runtime->call(paglet, op, wire::to_msgpack(request), 20s);
        if (r.status != 0) throw std::runtime_error(std::string(op) + ": " + std::string(abi::error_name(r.status)));
        Reply reply{};
        if (!wire::from_msgpack(r.payload, reply)) throw std::runtime_error(std::string(op) + ": bad reply");
        return reply;
    }

    // Waits until the paglet's journal has an entry starting with `prefix`.
    std::string journal_entry(const rt::PagletId& paglet, std::string_view prefix) {
        const auto until = std::chrono::steady_clock::now() + 10s;
        while (std::chrono::steady_clock::now() < until) {
            f->runtime->wait_idle();
            auto j = dec<std::vector<std::string>>(f->runtime->call(paglet, "journal", {}, 10s).payload);
            for (const auto& e : j) {
                if (e.starts_with(prefix)) return e;
            }
            std::this_thread::sleep_for(10ms);
        }
        return {};
    }
};

}  // namespace

PAGLETS_TEST(
    "policy: file access only after a rule or an approval from another host; revocation and expiry end it (WP9 exit)") {
    TwoHosts net;
    const auto id = net.explorer();
    const explorer::Access docs{"files", {"read"}, "data", "docs", 3'600'000};

    // No rule: denied (and audited).
    CHECK(net.ask(id, "request_access", docs) == "denied:");

    // The admin, on host B, lets staff ask for documents.
    net.admin("policy-rule", data::policy_rule({"staff may ask",
                                                Decision::ask,
                                                "files",
                                                {"read"},
                                                Match{.groups = std::vector<std::string>{"staff"}},
                                                Scope{{}, std::vector<std::string>{"docs/**"}},
                                                {},
                                                0}));
    net.settle();
    const std::string pending = net.ask(id, "request_access", docs);
    REQUIRE(pending.starts_with("pending:"));
    net.settle();
    // Host B sees the request and the admin approves it there.
    REQUIRE(net.ledger_b->state().grant_requests.size() == 1);
    const GrantRequest request = net.ledger_b->state().grant_requests.front();
    CHECK(record_id_hex(request.id) == pending.substr(8));
    CHECK(request.principal.paglet == id && request.host == net.host_a);
    const Record approval = net.admin(
        "grant", data::grant(request.principal, request.items.front(), {}, unix_ms() + 3'600'000, request.id, {}, {}));
    net.settle();
    // The approval reaches host A, which hands the capability to the paglet.
    const std::string late = net.journal_entry(id, "granted-late:");
    REQUIRE(!late.empty());
    CHECK(late.ends_with(":" + record_id_hex(approval.id())));
    const auto handle = std::stoi(late.substr(std::string("granted-late:").size()));
    CHECK(net.ask(id, "read_file", explorer::Explore{handle, "report.txt"}) == "content:quarterly numbers");
    CHECK(net.ask(id, "read_file", explorer::Explore{handle, "../public/notice.txt"}) == "read:invalid_argument");

    // Revoked on host B: the capability stops working on host A.
    net.admin("revoke", data::revoke_record(approval.id(), "project over"));
    net.settle();
    CHECK(net.ask(id, "read_file", explorer::Explore{handle, "report.txt"}) == "read:revoked");

    // An allow rule grants at once, for at most its duration.
    net.admin("policy-rule", data::policy_rule({"public, briefly",
                                                Decision::allow,
                                                "files",
                                                {"read"},
                                                {},
                                                Scope{{}, std::vector<std::string>{"public/**"}},
                                                std::int64_t{400},
                                                0}));
    net.settle();
    const std::string granted = net.ask(id, "request_access", explorer::Access{"files", {"read"}, "data", "public"});
    REQUIRE(granted.starts_with("granted:"));
    const auto brief = std::stoi(granted.substr(8));
    CHECK(net.ask(id, "read_file", explorer::Explore{brief, "notice.txt"}) == "content:open to all");
    std::this_thread::sleep_for(600ms);
    CHECK(net.ask(id, "read_file", explorer::Explore{brief, "notice.txt"}) == "read:expired");
    // The host-derived grant is valid on host B too (it stays within its rule).
    net.settle();
    const std::string grant_id = granted.substr(granted.rfind(':') + 1);
    CHECK(std::ranges::any_of(net.ledger_b->state().grants,
                              [&](const auto& g) { return record_id_hex(g.first) == grant_id && !g.second.by_admin; }));

    // Every decision is in the ledger of both hosts.
    std::vector<Decision> decisions;
    for (const auto& e : net.ledger_b->state().audit) {
        if (e.principal.paglet == id) decisions.push_back(e.decision);
    }
    CHECK(decisions == std::vector<Decision>({Decision::deny, Decision::ask, Decision::allow}));
    CHECK(net.a->state().audit.size() == net.ledger_b->state().audit.size());
}

PAGLETS_TEST("policy: denials reach the paglet; server-info only with a rule; releasing a grant") {
    TwoHosts net;
    const auto id = net.explorer();
    net.admin("policy-rule", data::policy_rule({"ask for anything", Decision::ask, "files", {"*"}, {}, {}, {}, 0}));
    net.settle();
    const std::string pending = net.ask(id, "request_access", explorer::Access{"files", {"write"}, "data", "docs"});
    REQUIRE(pending.starts_with("pending:"));
    net.settle();
    net.admin("request-deny", data::request_deny(net.ledger_b->state().grant_requests.front().id, "not today"));
    net.settle();
    CHECK(net.journal_entry(id, "denied-late:") == "denied-late:" + pending.substr(8) + ":not today");

    // server-info has ambient authority: the directory hands out only what rules allow.
    CHECK(net.ask(id, "service_ops", explorer::Text{"server-info"}) == "lookup:denied");
    CHECK(net.ask(id, "service_ops", explorer::Text{"files"}).starts_with("service:files:"));
    net.admin("policy-rule",
              data::policy_rule({"load for all", Decision::allow, "server-info", {"load"}, {}, {}, {}, 0}));
    net.settle();
    CHECK(net.ask(id, "service_ops", explorer::Text{"server-info"}) == "service:server-info:load,fixed");

    // A grant released by its paglet ends everywhere.
    net.admin("policy-rule", data::policy_rule({"read docs",
                                                Decision::allow,
                                                "files",
                                                {"read"},
                                                {},
                                                Scope{{}, std::vector<std::string>{"docs/**"}},
                                                {},
                                                0}));
    net.settle();
    const std::string granted = net.ask(id, "request_access", explorer::Access{"files", {"read"}, "data", "docs"});
    REQUIRE(granted.starts_with("granted:"));
    const auto handle = std::stoi(granted.substr(8));
    const std::string grant_id = granted.substr(granted.rfind(':') + 1);
    CHECK(net.ask(id, "release", explorer::Text{grant_id}) == "released");
    CHECK(net.ask(id, "read_file", explorer::Explore{handle, "report.txt"}) == "read:revoked");
    net.settle();
    CHECK(std::ranges::any_of(net.ledger_b->state().ended_grants,
                              [&](const RecordId& g) { return record_id_hex(g) == grant_id; }));
}

PAGLETS_TEST("policy: early approval of a manifest before the paglet exists") {
    TwoHosts net;
    // The owner asks for the mission's access before launching the paglet.
    const std::string planned(32, 'c');
    const Principal principal{planned, net.k.owner.id(), net.f->module("explorer.wasm"), "roaming"};
    Record request = signed_record(*net.ledger_b, "grant-request",
                                   data::grant_request(principal, net.k.owner.public_key(),
                                                       {Item{"files", {"read"}, "data", "docs"}}, 3'600'000, "mission"),
                                   {&net.k.owner}, false);
    REQUIRE_OK(net.b->submit(request));
    net.admin("grant", data::grant(principal, Item{"files", {"read"}, "data", "docs"}, {}, unix_ms() + 3'600'000,
                                   request.id(), {}, {}));
    net.settle();
    // The owner's passport for the paglet names the same ID; the host
    // creates it from the passport and delivers the grant.
    const std::string module = net.f->module("explorer.wasm");
    auto passport =
        Passport::issue(net.k.owner, net.a->mesh(), *parse_key_id(module), planned,
                        manifest_value({Item{"files", {"read"}, "data", "docs"}}), unix_ms(), unix_ms() + 3'600'000);
    REQUIRE_OK(passport);
    auto id = net.a->create(module, *passport);
    REQUIRE_OK(id);
    CHECK(*id == planned);
    const std::string late = net.journal_entry(planned, "granted-late:");
    REQUIRE(!late.empty());
    const auto handle = std::stoi(late.substr(std::string("granted-late:").size()));
    CHECK(net.ask(planned, "read_file", explorer::Explore{handle, "report.txt"}) == "content:quarterly numbers");
}

PAGLETS_TEST("passports: a mesh host creates root paglets only from valid passports") {
    TwoHosts net;
    const std::string module = net.f->module("explorer.wasm");
    const paglets::Digest hash = *parse_key_id(module);
    const std::int64_t now = unix_ms();
    auto issue = [&](const SigningKey& owner, const paglets::Digest& h, std::string id, std::int64_t expires) {
        auto p = Passport::issue(owner, net.a->mesh(), h, std::move(id), Value(), now - 1000, expires);
        REQUIRE_OK(p);
        return *p;
    };
    const auto good = issue(net.k.owner, hash, std::string(32, 'd'), now + 60'000);
    auto id = net.a->create(module, good);
    REQUIRE_OK(id);
    const auto info = net.f->runtime->info(*id);
    REQUIRE(info.has_value());
    CHECK(info->owner == net.k.owner.id() && info->trust == rt::TrustClass::roaming);
    CHECK(net.a->passport(*id).has_value());
    CHECK(!net.a->create(module, good));  // the ID exists

    CHECK(!net.a->create(module, issue(net.k.other_owner, hash, std::string(32, 'e'), now + 60'000)));  // not enrolled
    CHECK(!net.a->create(module, issue(net.k.owner, paglets::sha256(Bytes{1}), std::string(32, 'e'), now + 60'000)));
    CHECK(!net.a->create(module, issue(net.k.owner, hash, std::string(32, 'e'), now - 1)));  // expired
    auto child = good.extend(net.k.host_b, "child", std::string(32, 'f'), hash, now);
    REQUIRE_OK(child);
    CHECK(!net.a->create(module, *child));  // not a root passport
    CHECK(!net.f->runtime->info(std::string(32, 'e')));
}

PAGLETS_TEST("module trust: a mesh host runs only modules the ledger allows; paglets end when trust ends") {
    TwoHosts net;
    const std::string module = net.f->module("explorer.wasm");
    const paglets::Digest hash = *parse_key_id(module);
    const SigningKey signer = SigningKey::generate();
    auto ended_because = [&](const rt::PagletId& id, std::string_view text) {
        for (int i = 0; i < 200; ++i) {
            if (auto e = net.f->runtime->ending(id)) return e->failed && e->reason.find(text) != std::string::npos;
            std::this_thread::sleep_for(10ms);
        }
        return false;
    };

    // By default roaming modules need no trust.
    const auto early = net.explorer();
    // Once the mesh requires it, untrusted paglets end and new ones are refused.
    net.admin("module-policy", data::module_policy(RoamingModules::trusted));
    net.settle();
    CHECK(ended_because(early, "module no longer trusted: module not trusted for roaming paglets"));
    auto refused = net.f->runtime->create(module, rt::CreateOptions{{}, rt::TrustClass::roaming, net.k.owner.id()});
    REQUIRE(!refused.has_value());
    CHECK(refused.error().find("not trusted") != std::string::npos);

    // A trusted signer's module runs as roaming, not as resident.
    REQUIRE_OK(
        net.b->submit(signed_record(*net.ledger_b, "module-sign",
                                    data::module_sign(hash, signer.public_key(), "explorer", "1"), {&signer}, false)));
    net.admin("module-trust", data::module_trust("explorers", {signer.public_key()}, {}, {"roaming"}));
    net.settle();
    const auto trusted = net.explorer();
    CHECK_EQ(net.f->runtime->call(trusted, "journal", {}, 10s).status, 0);
    CHECK(!net.f->runtime->create(module, rt::CreateOptions{{}, rt::TrustClass::resident, net.k.owner.id()}));

    // Revoking the module ends its paglets on every host.
    net.admin("revoke", data::revoke_module(hash, "vulnerable"));
    net.settle();
    CHECK(ended_because(trusted, "module revoked"));
}
