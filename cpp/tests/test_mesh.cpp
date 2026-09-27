// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// WP8: keys, canonical values, ledger records, state derivation, gossip
// replication across partitions, enrollment and passports.

#include "test.hpp"

#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/gossip.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/passport.hpp>
#include <paglets/mesh/record.hpp>
#include <paglets/mesh/value.hpp>
#include <paglets/wasm/engine.hpp>

#include <algorithm>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <tuple>

#ifndef _WIN32
#include <sys/stat.h>
#endif

using namespace paglets::mesh;
namespace fs = std::filesystem;

namespace {

fs::path temp_dir(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    auto dir = fs::temp_directory_path() / ("paglets-mesh-" + name + "-" + std::to_string(rng()));
    fs::create_directories(dir);
    return dir;
}

Bytes bytes_of(std::initializer_list<int> values) {
    Bytes b;
    for (int v : values) b.push_back(static_cast<std::uint8_t>(v));
    return b;
}

// Drafts, signs and adds a record; returns it.
Record issue(Ledger& ledger, const std::string& type, Map data, const std::vector<const SigningKey*>& signers,
             bool with_epoch = true) {
    auto r = ledger.draft(type, std::move(data), with_epoch);
    REQUIRE_OK(r);
    for (const SigningKey* k : signers) r->sign(*k);
    auto added = ledger.add(*r);
    REQUIRE_OK(added);
    return *r;
}

struct Admins {
    SigningKey a = SigningKey::generate();
    SigningKey b = SigningKey::generate();
    SigningKey c = SigningKey::generate();
};

Ledger make_ledger(const std::vector<const SigningKey*>& admins, Quorum quorum = {}, const fs::path& directory = {}) {
    auto genesis = make_genesis("test mesh", admins, quorum, 1000);
    REQUIRE_OK(genesis);
    auto ledger = Ledger::create(*genesis, directory);
    REQUIRE_OK(ledger);
    return std::move(*ledger);
}

}  // namespace

// ---------------------------------------------------------------------------
// Canonical values

PAGLETS_TEST("canonical values: round trip and sorted maps") {
    Value v(Map{{"b", Value(1)}, {"a", Value(Array{Value("x"), Value(Bytes{1, 2})})}, {"c", Value(-300)}});
    const Bytes bytes = encode(v);
    auto back = decode(bytes);
    REQUIRE_OK(back);
    CHECK(*back == v);
    CHECK(encode(*back) == bytes);
    REQUIRE(v.as_map() != nullptr);
    CHECK(v.as_map()->front().first == "a");  // sorted on construction
    CHECK(v.find("c") != nullptr && *v.find("c")->as_int() == -300);
    // A repeated key: the last member wins.
    Value dup(Map{{"k", Value(1)}, {"k", Value(2)}});
    CHECK(dup.as_map()->size() == 1 && *dup.find("k")->as_int() == 2);
}

PAGLETS_TEST("canonical values: non-canonical input is rejected") {
    CHECK(decode(bytes_of({0x82, 0xa1, 'b', 0x01, 0xa1, 'a', 0x02})).error().find("sorted") != std::string::npos);
    CHECK(!decode(bytes_of({0x82, 0xa1, 'a', 0x01, 0xa1, 'a', 0x02})));                // repeated key
    CHECK(!decode(bytes_of({0xcc, 0x05})));                                            // 5 as uint8
    CHECK(!decode(bytes_of({0xd0, 0x05})));                                            // 5 as int8
    CHECK(!decode(bytes_of({0xd9, 0x01, 'a'})));                                       // str8 for a short string
    CHECK(!decode(bytes_of({0xcb, 0, 0, 0, 0, 0, 0, 0, 0})));                          // float
    CHECK(!decode(bytes_of({0x81, 0x01, 0x01})));                                      // integer key
    CHECK(!decode(bytes_of({0x01, 0x02})));                                            // trailing byte
    CHECK(!decode(bytes_of({0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})));  // above INT64_MAX
    CHECK(decode(bytes_of({0x91, 0x91, 0x90}), 1).has_value() == false);               // too deep for max_depth 1
    CHECK(decode(bytes_of({0x91, 0x90}), 1).has_value());
}

// ---------------------------------------------------------------------------
// Keys

PAGLETS_TEST("keys: sign, verify, derive from seed, key IDs") {
    const SigningKey k = SigningKey::generate();
    const Bytes message{1, 2, 3};
    const Signature s = k.sign(message);
    CHECK(verify(k.public_key(), message, s));
    CHECK(!verify(k.public_key(), Bytes{1, 2, 4}, s));
    CHECK(!verify(SigningKey::generate().public_key(), message, s));
    const Bytes seed(32, 7);
    auto a = SigningKey::from_seed(seed);
    auto b = SigningKey::from_seed(seed);
    REQUIRE_OK(a);
    REQUIRE_OK(b);
    CHECK(a->public_key() == b->public_key());
    CHECK(!SigningKey::from_seed(Bytes(31, 7)));
    CHECK(k.id().size() == 64);
    CHECK(parse_key_id(k.id()) == k.public_key());
    CHECK(!parse_key_id("xyz"));
}

PAGLETS_TEST("keys: encrypted admin key files") {
    const auto dir = temp_dir("keys");
    const Bytes seed{11,  22,  33,  44,  55,  66,  77,  88, 99, 110, 121, 132, 143, 154, 165, 176,
                     187, 198, 209, 220, 231, 242, 253, 8,  19, 30,  41,  52,  63,  74,  85,  96};
    auto from_seed = SigningKey::from_seed(seed);
    REQUIRE_OK(from_seed);
    const SigningKey& k = *from_seed;
    const auto path = dir / "admin.key";
    CHECK(!save_key(path, k, KeyRole::admin, "alice", std::nullopt));  // needs a passphrase
    REQUIRE_OK(save_key(path, k, KeyRole::admin, "alice", "correct horse", true));
    CHECK(!save_key(path, k, KeyRole::admin, "alice", "again", true));  // never overwrites
#ifndef _WIN32
    struct stat st {};
    REQUIRE(::stat(path.c_str(), &st) == 0);
    CHECK((st.st_mode & 0777) == 0600);
#endif
    auto info = read_key_info(path);
    REQUIRE_OK(info);
    CHECK(info->role == KeyRole::admin && info->name == "alice" && info->encrypted);
    CHECK(info->public_key == k.public_key());
    auto loaded = load_key(path, "correct horse");
    REQUIRE_OK(loaded);
    CHECK(loaded->public_key() == k.public_key());
    CHECK(verify(k.public_key(), Bytes{9}, loaded->sign(Bytes{9})));
    auto wrong = load_key(path, "wrong horse");
    CHECK(!wrong && wrong.error().find("wrong passphrase") != std::string::npos);
    CHECK(!load_key(path, std::nullopt));
    // The file does not contain the seed in plain text.
    auto bytes = paglets::wasm::read_file(path.string());
    REQUIRE_OK(bytes);
    CHECK(std::search(bytes->begin(), bytes->end(), seed.begin(), seed.end()) == bytes->end());
    fs::remove_all(dir);
}

PAGLETS_TEST("keys: tampered key files are refused") {
    const auto dir = temp_dir("tamper");
    const SigningKey k = SigningKey::generate();
    const auto path = dir / "owner.key";
    REQUIRE_OK(save_key(path, k, KeyRole::owner, "bob", "pw", true));
    auto bytes = paglets::wasm::read_file(path.string());
    REQUIRE_OK(bytes);
    // Change the role from owner to admin: the role is bound to the
    // ciphertext, so decryption fails.
    std::string text(bytes->begin(), bytes->end());
    const auto at = text.find("owner");
    REQUIRE(at != std::string::npos);
    text.replace(at, 5, "admin");
    const auto tampered = dir / "tampered.key";
    REQUIRE_OK(paglets::wasm::write_file(tampered.string(), Bytes(text.begin(), text.end())));
    CHECK(!load_key(tampered, "pw"));
    // Host keys may be stored unencrypted; admin keys may not be loaded so.
    const auto host_path = dir / "host.key";
    REQUIRE_OK(save_key(host_path, k, KeyRole::host, "h1", std::nullopt));
    auto host = load_key(host_path, std::nullopt);
    REQUIRE_OK(host);
    CHECK(host->public_key() == k.public_key());
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Records

PAGLETS_TEST("records: IDs, signatures and merging") {
    const SigningKey a = SigningKey::generate();
    const SigningKey b = SigningKey::generate();
    RecordId mesh{};
    mesh[0] = 1;
    auto r = Record::make(Record::Draft{"host-remove", mesh, 5, mesh, 1234, data::key_removal(a.public_key())});
    REQUIRE_OK(r);
    const RecordId id = r->id();
    Record ra = *r;
    Record rb = *r;
    ra.sign(a);
    rb.sign(b);
    CHECK(ra.id() == id && rb.id() == id);  // signatures do not change the ID
    CHECK(ra.merge(rb) == 1);
    CHECK(ra.merge(rb) == 0);
    CHECK(ra.signed_by(a.public_key()) && ra.signed_by(b.public_key()));
    auto back = Record::decode(ra.encode());
    REQUIRE_OK(back);
    CHECK(back->id() == id && back->signatures().size() == 2 && back->clock() == 5);
    CHECK(back->type() == "host-remove" && back->mesh() == mesh);
    // A flipped bit in a signature is rejected.
    Bytes damaged = ra.encode();
    damaged[damaged.size() - 1] ^= 1;
    CHECK(!Record::decode(damaged));
    CHECK(!Record::make(Record::Draft{"", mesh, 1, std::nullopt, 0, {}}));
    CHECK(!Record::make(Record::Draft{"x", mesh, max_clock + 1, std::nullopt, 0, {}}));
}

// ---------------------------------------------------------------------------
// Ledger state

PAGLETS_TEST("ledger: genesis needs every initial admin") {
    Admins ad;
    auto genesis = make_genesis("m", {&ad.a, &ad.b}, Quorum{2, 1}, 1);
    REQUIRE_OK(genesis);
    CHECK(genesis->signatures().size() == 2);
    auto r = Record::make(
        Record::Draft{"genesis", std::nullopt, 0, std::nullopt, 1,
                      Map{{"name", Value("m")},
                          {"admins", Value(Array{Value::bin(ad.a.public_key()), Value::bin(ad.b.public_key())})},
                          {"quorum", Value(Map{{"admin_set", Value(1)}, {"default", Value(1)}})}}});
    REQUIRE_OK(r);
    r->sign(ad.a);
    CHECK(!Ledger::create(*r));                           // lacks b's signature
    CHECK(!make_genesis("m", {&ad.a}, Quorum{2, 1}, 1));  // quorum above the admin count
}

PAGLETS_TEST("ledger: enrollment by request and admin approval") {
    Admins ad;
    Ledger ledger = make_ledger({&ad.a});
    const SigningKey host = SigningKey::generate();
    const SigningKey owner = SigningKey::generate();
    const Record request = issue(ledger, "host-enroll-request",
                                 data::host_enroll_request(host.public_key(), "h1", {"linux"}), {&host}, false);
    const Record owner_request =
        issue(ledger, "owner-enroll-request", data::owner_enroll_request(owner.public_key(), "olga"), {&owner}, false);
    REQUIRE(ledger.state().pending.size() == 2);
    CHECK(!ledger.state().is_host(host.public_key()));
    issue(ledger, "host-enroll", data::host_enroll(host.public_key(), "h1", {"linux", "gpu"}, request.id()), {&ad.a});
    issue(ledger, "request-deny", data::request_deny(owner_request.id(), "unknown person"), {&ad.a});
    const LedgerState& st = ledger.state();
    CHECK(st.pending.empty());
    REQUIRE(st.is_host(host.public_key()));
    CHECK(st.hosts.at(host.public_key()).labels == std::vector<std::string>({"linux", "gpu"}));
    CHECK(!st.is_owner(owner.public_key()));
    // A request must be signed by the key it names.
    const SigningKey other = SigningKey::generate();
    issue(ledger, "host-enroll-request", data::host_enroll_request(other.public_key(), "h2", {}), {&host}, false);
    CHECK(ledger.state().pending.empty());
    // Enrollments need an admin signature.
    issue(ledger, "owner-enroll", data::owner_enroll(owner.public_key(), "olga", {}), {&host});
    CHECK(!ledger.state().is_owner(owner.public_key()));
    issue(ledger, "owner-enroll", data::owner_enroll(owner.public_key(), "olga", {"staff"}), {&ad.b, &ad.a});
    CHECK(ledger.state().is_owner(owner.public_key()));
    issue(ledger, "owner-remove", data::key_removal(owner.public_key()), {&ad.a});
    CHECK(!ledger.state().is_owner(owner.public_key()));
}

PAGLETS_TEST("ledger: revocations win regardless of order") {
    Admins ad;
    Ledger ledger = make_ledger({&ad.a});
    const SigningKey host = SigningKey::generate();
    const Record enroll = issue(ledger, "host-enroll", data::host_enroll(host.public_key(), "h1", {}), {&ad.a});
    issue(ledger, "revoke", data::revoke_key(host.public_key(), "stolen"), {&ad.a});
    CHECK(!ledger.state().is_host(host.public_key()));
    // A later enrollment of the same key has no effect.
    issue(ledger, "host-enroll", data::host_enroll(host.public_key(), "h1 again", {}), {&ad.a});
    CHECK(!ledger.state().is_host(host.public_key()));
    CHECK(ledger.state().revoked_keys.contains(host.public_key()));
    // Revoking a record removes its effect; later enrollments still work.
    const SigningKey h2 = SigningKey::generate();
    const Record e2 = issue(ledger, "host-enroll", data::host_enroll(h2.public_key(), "h2", {}), {&ad.a});
    CHECK(ledger.state().is_host(h2.public_key()));
    issue(ledger, "revoke", data::revoke_record(e2.id(), "mistake"), {&ad.a});
    CHECK(!ledger.state().is_host(h2.public_key()));
    issue(ledger, "host-enroll", data::host_enroll(h2.public_key(), "h2", {}), {&ad.a});
    CHECK(ledger.state().is_host(h2.public_key()));
    (void)enroll;
}

PAGLETS_TEST("ledger: admin changes need the admin quorum; co-signatures merge") {
    Admins ad;
    Ledger ledger = make_ledger({&ad.a, &ad.b}, Quorum{2, 1});
    const SigningKey d = SigningKey::generate();
    auto change = ledger.draft("admin-set", data::admin_set(ledger, {d.public_key()}, {}));
    REQUIRE_OK(change);
    Record by_a = *change;
    by_a.sign(ad.a);
    REQUIRE_OK(ledger.add(by_a));
    CHECK(!ledger.state().is_admin(d.public_key()));  // one of two signatures
    Record by_b = *change;
    by_b.sign(ad.b);
    auto merged = ledger.add(by_b);
    REQUIRE_OK(merged);
    CHECK(*merged == AddResult::merged);
    CHECK(ledger.state().is_admin(d.public_key()));
    CHECK(ledger.state().epoch == change->id());
    // The new admin signs normal records in the new epoch.
    const SigningKey host = SigningKey::generate();
    issue(ledger, "host-enroll", data::host_enroll(host.public_key(), "h", {}), {&d});
    CHECK(ledger.state().is_host(host.public_key()));
}

PAGLETS_TEST("ledger: records a removed admin signs later are ignored") {
    Admins ad;
    Ledger ledger = make_ledger({&ad.a, &ad.b});
    const SigningKey early = SigningKey::generate();
    // b enrolls a host while still an admin: stays valid after b's removal.
    issue(ledger, "host-enroll", data::host_enroll(early.public_key(), "early", {}), {&ad.b});
    const RecordId genesis_epoch = ledger.state().epoch;
    issue(ledger, "admin-set", data::admin_set(ledger, {}, {ad.b.public_key()}), {&ad.a});
    REQUIRE(!ledger.state().is_admin(ad.b.public_key()));
    CHECK(ledger.state().is_host(early.public_key()));
    // b backdates a record into the genesis epoch with a small clock.
    const SigningKey late = SigningKey::generate();
    auto backdated = Record::make(Record::Draft{"host-enroll", ledger.mesh(), 1, genesis_epoch, 2000,
                                                data::host_enroll(late.public_key(), "late", {})});
    REQUIRE_OK(backdated);
    backdated->sign(ad.b);
    REQUIRE_OK(ledger.add(*backdated));
    CHECK(!ledger.state().is_host(late.public_key()));
    bool listed = false;
    for (const auto& i : ledger.state().ignored) listed = listed || i.id == backdated->id();
    CHECK(listed);
    // b cannot fork the admin chain either: a backdated admin change in the
    // genesis epoch that removes a conflicts with a's removal of b; removals
    // win, and b's addition does not apply.
    const SigningKey friend_key = SigningKey::generate();
    auto fork = Record::make(Record::Draft{"admin-set", ledger.mesh(), 1, genesis_epoch, 2000,
                                           data::admin_set(ledger, {friend_key.public_key()}, {})});
    REQUIRE_OK(fork);
    fork->sign(ad.b);
    // a signed a record in the epoch of b's removal before the fork arrived:
    // it stays valid in the merged epoch.
    const SigningKey third = SigningKey::generate();
    auto by_a = ledger.draft("host-enroll", data::host_enroll(third.public_key(), "third", {}));
    REQUIRE_OK(by_a);
    by_a->sign(ad.a);
    REQUIRE_OK(ledger.add(*fork));
    REQUIRE_OK(ledger.add(*by_a));
    CHECK(!ledger.state().is_admin(ad.b.public_key()));
    CHECK(!ledger.state().is_admin(friend_key.public_key()));
    CHECK(ledger.state().is_admin(ad.a.public_key()));
    CHECK(ledger.state().is_host(third.public_key()));
}

PAGLETS_TEST("ledger: conflicting admin removals remove both") {
    Admins ad;
    Ledger ledger = make_ledger({&ad.a, &ad.b, &ad.c});
    const RecordId epoch = ledger.state().epoch;
    auto remove_b = Record::make(
        Record::Draft{"admin-set", ledger.mesh(), 1, epoch, 1, data::admin_set(ledger, {}, {ad.b.public_key()})});
    auto remove_a = Record::make(
        Record::Draft{"admin-set", ledger.mesh(), 1, epoch, 1, data::admin_set(ledger, {}, {ad.a.public_key()})});
    REQUIRE_OK(remove_b);
    REQUIRE_OK(remove_a);
    remove_b->sign(ad.a);
    remove_a->sign(ad.b);
    REQUIRE_OK(ledger.add(*remove_b));
    REQUIRE_OK(ledger.add(*remove_a));
    const LedgerState& st = ledger.state();
    CHECK(st.admins == std::set<PublicKey>{ad.c.public_key()});
    CHECK(st.epoch != remove_a->id() && st.epoch != remove_b->id());  // a merged epoch
    // c continues in the merged epoch.
    const SigningKey host = SigningKey::generate();
    issue(ledger, "host-enroll", data::host_enroll(host.public_key(), "h", {}), {&ad.c});
    CHECK(ledger.state().is_host(host.public_key()));
}

PAGLETS_TEST("ledger: state is independent of arrival order and survives reopening") {
    Admins ad;
    const auto dir = temp_dir("ledger");
    std::vector<Record> all;
    RecordId mesh{};
    {
        Ledger ledger = make_ledger({&ad.a, &ad.b}, Quorum{1, 1}, dir);
        mesh = ledger.mesh();
        std::vector<SigningKey> hosts;
        for (int i = 0; i < 6; ++i) hosts.push_back(SigningKey::generate());
        for (int i = 0; i < 6; ++i) {
            all.push_back(issue(ledger, "host-enroll", data::host_enroll(hosts[i].public_key(), "h", {}),
                                {i % 2 == 0 ? &ad.a : &ad.b}));
        }
        all.push_back(issue(ledger, "host-remove", data::key_removal(hosts[1].public_key()), {&ad.a}));
        all.push_back(issue(ledger, "revoke", data::revoke_key(hosts[2].public_key(), "x"), {&ad.b}));
        const paglets::Digest expected = ledger.state().digest();
        CHECK(ledger.state().hosts.size() == 4);

        auto reopened = Ledger::open(dir, mesh);
        REQUIRE_OK(reopened);
        CHECK(reopened->state().digest() == expected);
        CHECK(reopened->digest() == ledger.digest());
        CHECK(!Ledger::open(dir, RecordId{}));  // another trust anchor

        // Shuffled insertion into a fresh ledger gives the same state.
        auto genesis = ledger.find(mesh);
        REQUIRE(genesis != nullptr);
        auto fresh = Ledger::create(*genesis);
        REQUIRE_OK(fresh);
        std::mt19937 rng(7);
        std::shuffle(all.begin(), all.end(), rng);
        for (const auto& r : all) REQUIRE_OK(fresh->add(r));
        CHECK(fresh->state().digest() == expected);
    }
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Gossip

namespace {

// An in-memory network of replicas with partitions.
struct Network {
    struct Port : GossipTransport {
        Network* net = nullptr;
        PublicKey self{};
        void send(const PublicKey& to, Bytes frame) override { net->queue.emplace_back(self, to, std::move(frame)); }
    };
    struct Node {
        SigningKey key = SigningKey::generate();
        std::unique_ptr<Ledger> ledger;
        std::unique_ptr<Port> port;
        std::unique_ptr<Replica> replica;
    };

    std::vector<std::unique_ptr<Node>> nodes;
    std::deque<std::tuple<PublicKey, PublicKey, Bytes>> queue;
    std::map<PublicKey, int> group;  // nodes in different groups cannot talk
    std::size_t dropped = 0;

    Node& add(const Record& genesis) {
        auto node = std::make_unique<Node>();
        auto ledger = Ledger::create(genesis);
        REQUIRE_OK(ledger);
        node->ledger = std::make_unique<Ledger>(std::move(*ledger));
        node->port = std::make_unique<Port>();
        node->port->net = this;
        node->port->self = node->key.public_key();
        node->replica = std::make_unique<Replica>(*node->ledger, node->key.public_key(), *node->port);
        for (auto& other : nodes) {
            other->replica->add_seed(node->key.public_key());
            node->replica->add_seed(other->key.public_key());
        }
        nodes.push_back(std::move(node));
        return *nodes.back();
    }
    Node* find(const PublicKey& key) {
        for (auto& n : nodes) {
            if (n->key.public_key() == key) return n.get();
        }
        return nullptr;
    }
    void deliver() {
        while (!queue.empty()) {
            auto [from, to, frame] = std::move(queue.front());
            queue.pop_front();
            if (group[from] != group[to]) {
                ++dropped;
                continue;
            }
            if (Node* n = find(to)) n->replica->receive(from, frame);
        }
    }
    bool converged() const {
        for (const auto& n : nodes) {
            if (n->ledger->digest() != nodes.front()->ledger->digest()) return false;
        }
        return true;
    }
    // Anti-entropy rounds until every node holds the same records.
    int settle(int max_rounds = 20) {
        deliver();
        for (int round = 0; round < max_rounds; ++round) {
            if (converged()) return round;
            for (auto& n : nodes) n->replica->tick();
            deliver();
        }
        return converged() ? max_rounds : -1;
    }
};

Record signed_draft(const Ledger& ledger, const std::string& type, Map data,
                    const std::vector<const SigningKey*>& signers, bool with_epoch = true) {
    auto r = ledger.draft(type, std::move(data), with_epoch);
    REQUIRE_OK(r);
    for (const SigningKey* k : signers) r->sign(*k);
    return *r;
}

}  // namespace

PAGLETS_TEST("gossip: pushes reach every replica") {
    Admins ad;
    auto genesis = make_genesis("g", {&ad.a}, {}, 1);
    REQUIRE_OK(genesis);
    Network net;
    auto& n1 = net.add(*genesis);
    net.add(*genesis);
    net.add(*genesis);
    const SigningKey owner = SigningKey::generate();
    REQUIRE_OK(n1.replica->submit(
        signed_draft(*n1.ledger, "owner-enroll", data::owner_enroll(owner.public_key(), "o", {}), {&ad.a})));
    net.deliver();
    CHECK(net.converged());
    for (auto& n : net.nodes) CHECK(n->ledger->state().is_owner(owner.public_key()));
    // Garbage and records of other meshes are rejected.
    auto other = make_genesis("other", {&ad.a}, {}, 2);
    REQUIRE_OK(other);
    Ledger foreign = make_ledger({&ad.a});
    Record alien = signed_draft(foreign, "owner-enroll", data::owner_enroll(owner.public_key(), "o", {}), {&ad.a});
    Array list{Value(alien.encode()), Value(Bytes{1, 2, 3})};
    n1.replica->receive(net.nodes[1]->key.public_key(),
                        encode(Value(Map{{"t", Value("push")}, {"records", Value(std::move(list))}})));
    CHECK(n1.replica->stats().records_rejected == 2);
    n1.replica->receive(net.nodes[1]->key.public_key(), Bytes{0xc1});
}

PAGLETS_TEST("gossip: three hosts converge after partitions (WP8 exit)") {
    Admins ad;
    auto genesis = make_genesis("exit", {&ad.a, &ad.b, &ad.c}, Quorum{1, 1}, 1);
    REQUIRE_OK(genesis);
    Network net;
    auto& h1 = net.add(*genesis);
    auto& h2 = net.add(*genesis);
    auto& h3 = net.add(*genesis);
    // The three hosts enroll themselves: requests from their own host,
    // approved by an admin working through h1.
    for (auto* n : {&h1, &h2, &h3}) {
        const Record request =
            signed_draft(*n->ledger, "host-enroll-request", data::host_enroll_request(n->key.public_key(), "host", {}),
                         {&n->key}, false);
        REQUIRE_OK(n->replica->submit(request));
        REQUIRE(net.settle() >= 0);
        REQUIRE_OK(h1.replica->submit(signed_draft(
            *h1.ledger, "host-enroll", data::host_enroll(n->key.public_key(), "host", {}, request.id()), {&ad.a})));
    }
    REQUIRE(net.settle() >= 0);
    CHECK(h3.ledger->state().hosts.size() == 3);
    CHECK(h3.ledger->state().pending.empty());

    // Partition: h1 | h2 h3.
    net.group[h1.key.public_key()] = 1;
    // On h1: admin a removes admin b and enrolls an owner.
    const SigningKey o1 = SigningKey::generate();
    REQUIRE_OK(h1.replica->submit(
        signed_draft(*h1.ledger, "admin-set", data::admin_set(*h1.ledger, {}, {ad.b.public_key()}), {&ad.a})));
    REQUIRE_OK(h1.replica->submit(
        signed_draft(*h1.ledger, "owner-enroll", data::owner_enroll(o1.public_key(), "o1", {}), {&ad.a})));
    // On h3: a new owner asks to be enrolled; admin c approves it on h2.
    // Admin b, not yet aware of the removal, enrolls another owner on h2.
    const SigningKey o2 = SigningKey::generate();
    const SigningKey o3 = SigningKey::generate();
    const Record o2_request = signed_draft(*h3.ledger, "owner-enroll-request",
                                           data::owner_enroll_request(o2.public_key(), "o2"), {&o2}, false);
    REQUIRE_OK(h3.replica->submit(o2_request));
    net.settle();
    REQUIRE(h2.ledger->state().pending.size() == 1);
    REQUIRE_OK(h2.replica->submit(signed_draft(
        *h2.ledger, "owner-enroll", data::owner_enroll(o2.public_key(), "o2", {}, o2_request.id()), {&ad.c})));
    REQUIRE_OK(h2.replica->submit(
        signed_draft(*h2.ledger, "owner-enroll", data::owner_enroll(o3.public_key(), "o3", {}), {&ad.b})));
    net.settle();
    CHECK(!net.converged());
    CHECK(h1.ledger->state().digest() != h2.ledger->state().digest());
    CHECK(h2.ledger->state().digest() == h3.ledger->state().digest());
    CHECK(h3.ledger->state().is_owner(o3.public_key()));  // b is still an admin there

    // Heal. A second partition (h1 h2 | h3) while more happens, then heal.
    net.group.clear();
    REQUIRE(net.settle() >= 0);
    net.group[h3.key.public_key()] = 2;
    const SigningKey h4 = SigningKey::generate();
    REQUIRE_OK(h2.replica->submit(
        signed_draft(*h2.ledger, "host-enroll", data::host_enroll(h4.public_key(), "h4", {}), {&ad.c})));
    REQUIRE_OK(
        h3.replica->submit(signed_draft(*h3.ledger, "revoke", data::revoke_key(o1.public_key(), "left"), {&ad.a})));
    net.settle();
    CHECK(!net.converged());
    net.group.clear();
    REQUIRE(net.settle() >= 0);

    // Same records, same state everywhere.
    CHECK(net.converged());
    const paglets::Digest state = h1.ledger->state().digest();
    CHECK(h2.ledger->state().digest() == state);
    CHECK(h3.ledger->state().digest() == state);
    const LedgerState& st = h3.ledger->state();
    CHECK(!st.is_admin(ad.b.public_key()));
    CHECK(st.admins.size() == 2);
    CHECK(!st.is_owner(o1.public_key()));  // revoked
    CHECK(st.is_owner(o2.public_key()));   // approved by c on another host than the request
    CHECK(!st.is_owner(o3.public_key()));  // b signed after its removal: ignored
    CHECK(st.hosts.size() == 4);
    CHECK(st.pending.empty());
    CHECK(net.dropped > 0);
}

// ---------------------------------------------------------------------------
// Passports

PAGLETS_TEST("passports: owner-signed roots and host-signed links") {
    Admins ad;
    Ledger ledger = make_ledger({&ad.a});
    const SigningKey owner = SigningKey::generate();
    const SigningKey host = SigningKey::generate();
    issue(ledger, "owner-enroll", data::owner_enroll(owner.public_key(), "o", {}), {&ad.a});
    issue(ledger, "host-enroll", data::host_enroll(host.public_key(), "h", {}), {&ad.a});
    const auto module = paglets::sha256(Bytes{1, 2, 3});
    const auto other_module = paglets::sha256(Bytes{4});
    auto p =
        Passport::issue(owner, ledger.mesh(), module, "root-1", Value(Map{{"files", Value("read")}}), 1000, 100000);
    REQUIRE_OK(p);
    const LedgerState& st = ledger.state();
    REQUIRE_OK(verify_passport(*p, st, "root-1", module, 2000));
    CHECK(!verify_passport(*p, st, "root-2", module, 2000));
    CHECK(!verify_passport(*p, st, "root-1", other_module, 2000));
    CHECK(!verify_passport(*p, st, "root-1", module, 100000));  // expired

    auto child = p->extend(host, "child", "child-1", other_module, 3000);
    REQUIRE_OK(child);
    auto clone = child->extend(host, "clone", "clone-1", other_module, 4000);
    REQUIRE_OK(clone);
    auto decoded = Passport::decode(clone->encode());
    REQUIRE_OK(decoded);
    CHECK(decoded->links().size() == 2 && decoded->paglet() == "clone-1");
    REQUIRE_OK(verify_passport(*decoded, st, "clone-1", other_module, 5000));

    // A link cannot be moved onto another chain.
    auto p2 = Passport::issue(owner, ledger.mesh(), module, "root-2", Value(), 1000, 100000);
    REQUIRE_OK(p2);
    auto v1 = decode(child->encode());
    auto v2 = decode(p2->encode());
    REQUIRE_OK(v1);
    REQUIRE_OK(v2);
    Value spliced(Map{{"root", *v2->find("root")}, {"links", *v1->find("links")}});
    CHECK(!Passport::decode(encode(spliced)));

    // Unknown hosts and owners, and revoked keys, fail verification.
    const SigningKey stranger = SigningKey::generate();
    auto foreign_link = p->extend(stranger, "child", "c2", module, 3000);
    REQUIRE_OK(foreign_link);
    CHECK(!verify_passport(*foreign_link, st, "c2", module, 5000));
    auto unenrolled = Passport::issue(stranger, ledger.mesh(), module, "r", Value(), 1000, 100000);
    REQUIRE_OK(unenrolled);
    CHECK(!verify_passport(*unenrolled, st, "r", module, 2000));
    issue(ledger, "revoke", data::revoke_key(host.public_key(), "retired"), {&ad.a});
    CHECK(!verify_passport(*decoded, ledger.state(), "clone-1", other_module, 5000));
    REQUIRE_OK(verify_passport(*p, ledger.state(), "root-1", module, 2000));
}
