// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Content exchange between hosts (planning/cpp-content-exchange.md):
// artifact capabilities move with paglets, and hosts that lack the content
// fetch it from the host that holds it, within data residency.

#include "mesh_fixture.hpp"
#include "runtime_fixture.hpp"
#include "test.hpp"

#include <exchange_msgs.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <fstream>
#include <random>

using namespace paglets::test;
namespace wire = paglets::wire;
namespace fs = std::filesystem;

namespace {

fs::path temp_root(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    auto dir = fs::temp_directory_path() / ("paglets-exchange-" + name + "-" + std::to_string(rng()));
    fs::create_directories(dir);
    return dir;
}

void write_text(const fs::path& p, const std::string& text) {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary) << text;
}

std::string read_text(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

std::string sha(const std::string& s) {
    return paglets::to_hex(paglets::sha256(std::span(reinterpret_cast<const std::uint8_t*>(s.data()), s.size())));
}

Mesh::Host* holder(Mesh& net, const rt::PagletId& id) {
    for (auto& h : net.hosts) {
        if (h->f->runtime->info(id)) return h.get();
    }
    return nullptr;
}

// Runs one step of the exchange paglet and waits for its outcome.
exchange_msgs::Outcome step(Mesh& net, const rt::PagletId& id, const std::string& name, exchange_msgs::Step s) {
    Mesh::Host* h = holder(net, id);
    REQUIRE(h != nullptr);
    auto r = h->f->runtime->call(id, name, wire::to_msgpack(s), 20s);
    REQUIRE(r.status == 0);
    exchange_msgs::Outcome out;
    REQUIRE(net.settle(
        [&] {
            Mesh::Host* at = holder(net, id);
            if (at == nullptr) return false;
            auto x = at->f->runtime->call(id, "outcome", {}, 10s);
            return x.status == 0 && wire::from_msgpack(x.payload, out) && out.step == name && out.done;
        },
        3000));
    return out;
}

void allow_files(Mesh& net, const std::string& name, std::vector<std::string> ops, std::vector<std::string> roots) {
    net.admin_record("policy-rule",
                     data::policy_rule({name, Decision::allow, "files", std::move(ops), {},
                                        Scope{std::move(roots), std::nullopt}, std::nullopt, 0}));
}

}  // namespace

PAGLETS_TEST("exchange: a large shared file travels as a capability") {
    const fs::path data = temp_root("data"), inbox = temp_root("inbox");
    std::string big(20u * 1024 * 1024 + 4321, '\0');
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<char>((i * 40503u) >> 7);
    write_text(data / "field.bin", big);
    ps::ServicesConfig sa, sb;
    sa.roots["data"] = data;
    sb.roots["inbox"] = inbox;
    Mesh net;
    auto& a = net.add_host("a", sa);
    auto& b = net.add_host("b", sb);
    auto& c = net.add_host("c");
    net.enroll_all();
    allow_files(net, "read data", {"read"}, {"data"});
    allow_files(net, "fill inbox", {"write", "create"}, {"inbox"});
    REQUIRE(net.settle([&] { return net.converged(); }));
    const std::string module = a.f->module("exchange.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('a')));
    REQUIRE_OK(created);
    const auto id = *created;

    // Sharing reads nothing into the paglet: it is not marked.
    auto shared = step(net, id, "share", {"data", "field.bin"});
    REQUIRE(shared.status == 0);
    CHECK_EQ(shared.hash, sha(big));
    CHECK_EQ(shared.size, big.size());
    CHECK(a.f->runtime->marks(id).empty());

    // On b, the content goes from a to b directly and is checked.
    REQUIRE(step(net, id, "go", {.host = "b"}).status == 0);
    CHECK(holder(net, id) == &b);
    auto saved = step(net, id, "save", {"inbox", "field.bin"});
    REQUIRE(saved.status == 0);
    CHECK_EQ(saved.size, big.size());
    CHECK(read_text(inbox / "field.bin") == big);
    CHECK(b.f->runtime->marks(id).empty());

    // On c, reading it fetches it into c's store; the reader takes the
    // content's marks.
    REQUIRE(step(net, id, "go", {.host = "c"}).status == 0);
    auto read = step(net, id, "read", {.offset = 10u * 1024 * 1024, .length = 100});
    REQUIRE(read.status == 0);
    CHECK_EQ(read.size, big.size());
    CHECK(read.data == big.substr(10u * 1024 * 1024, 100));
    ::paglets::test::report("file_courier", "a 20 MB file shared, moved as a capability, saved and read on two other hosts", "yes");
    const auto marks = c.f->runtime->marks(id);
    CHECK(std::ranges::find(marks, std::string("root:data")) != marks.end());

    // A changed file is no longer served.
    write_text(data / "field.bin", big + "!");
    REQUIRE(step(net, id, "go", {.host = "b"}).status == 0);
    CHECK_EQ(step(net, id, "save", {"inbox", "again.bin"}).status, paglets::abi::not_found);
    CHECK(!fs::exists(inbox / "again.bin"));
    std::error_code ec;
    for (const auto& p : {data, inbox}) fs::remove_all(p, ec);
}

PAGLETS_TEST("exchange: artifacts work on every host, and content stays within data residency") {
    const fs::path clinic = temp_root("clinic"), inbox_b = temp_root("inbox-b"), inbox_c = temp_root("inbox-c");
    write_text(clinic / "scan.txt", "confidential");
    ps::ServicesConfig sa, sb, sc;
    sa.roots["clinic"] = clinic;
    sb.roots["inbox"] = inbox_b;
    sc.roots["inbox"] = inbox_c;
    Mesh net;
    auto& a = net.add_host("a", sa);
    auto& b = net.add_host("b", sb);
    net.add_host("c", sc);
    net.enroll_all();
    allow_files(net, "read clinic", {"read"}, {"clinic"});
    allow_files(net, "fill inboxes", {"write", "create"}, {"inbox"});
    net.admin_record("root-residency", data::root_residency("clinic", "hosts", HostSelector{{b.key}, {}}));
    REQUIRE(net.settle([&] { return net.converged(); }));
    const std::string module = a.f->module("exchange.wasm");
    auto created = a.node->create(module, net.passport(module, paglet_id('b')));
    REQUIRE_OK(created);
    const auto id = *created;

    REQUIRE(step(net, id, "share", {"clinic", "scan.txt"}).status == 0);
    // c may not have the clinic's content: a refuses it.
    REQUIRE(step(net, id, "go", {.host = "c"}).status == 0);
    CHECK_EQ(step(net, id, "save", {"inbox", "scan.txt"}).status, paglets::abi::denied);
    CHECK(!fs::exists(inbox_c / "scan.txt"));
    CHECK_EQ(step(net, id, "read", {.length = 100}).status, paglets::abi::denied);
    // b may.
    REQUIRE(step(net, id, "go", {.host = "b"}).status == 0);
    REQUIRE(step(net, id, "save", {"inbox", "scan.txt"}).status == 0);
    CHECK_EQ(read_text(inbox_b / "scan.txt"), std::string("confidential"));

    // An artifact put on b is read on c.
    auto put = step(net, id, "put", {.text = "hello from b"});
    REQUIRE(put.status == 0);
    REQUIRE(step(net, id, "go", {.host = "c"}).status == 0);
    auto read = step(net, id, "read", {.length = 100});
    REQUIRE(read.status == 0);
    CHECK_EQ(read.data, std::string("hello from b"));
    (void)a;
    std::error_code ec;
    for (const auto& p : {clinic, inbox_b, inbox_c}) fs::remove_all(p, ec);
}
