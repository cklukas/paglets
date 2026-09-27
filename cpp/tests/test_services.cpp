// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The standard system paglets (WP10), driven by the explorer guest through
// the generated service clients. The first test is the WP10 exit criterion:
// the same roaming paglet finds files, reads content and reports load and
// free space, with the same schemas on macOS, Linux and Windows (CI runs
// this file on all three).

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <explorer_msgs.hpp>
#include <paglets/services/system_services.hpp>
#include <paglets/sha256.hpp>
#include <paglets/wire/reflect.hpp>

#include <filesystem>
#include <fstream>
#include <random>

using namespace paglets::test;
namespace ps = paglets::services;
namespace wire = paglets::wire;
namespace fs = std::filesystem;

namespace {

fs::path temp_dir(const std::string& name) {
    static std::mt19937_64 rng(std::random_device{}());
    auto dir = fs::temp_directory_path() / ("paglets-services-" + name + "-" + std::to_string(rng()));
    fs::create_directories(dir);
    return dir;
}

void write_text(const fs::path& path, std::string_view text) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary);
    out << text;
}

std::string read_text(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

// A runtime with the standard system paglets and a `data` root.
struct Services {
    explicit Services(fs::path data_root, fs::path state = {}, std::uint64_t storage_quota = 16u * 1024 * 1024)
        : root(std::move(data_root)) {
        rt::Config config;
        config.state_dir = state;
        f = std::make_unique<Fixture>(config);
        ps::ServicesConfig sc;
        sc.roots["data"] = root;
        sc.state_dir = state;
        sc.storage_quota = storage_quota;
        auto installed = ps::install_system_services(*f->runtime, sc);
        REQUIRE_OK(installed);
        services = *installed;
    }

    rt::PagletId explorer(std::string owner = "local") {
        auto module = f->module("explorer.wasm");
        auto id = f->runtime->create(module, rt::CreateOptions{{}, rt::TrustClass::roaming, std::move(owner)});
        REQUIRE_OK(id);
        return *id;
    }

    // A `dir` capability in the paglet's table.
    std::int32_t grant_dir(const rt::PagletId& paglet, std::string_view path, std::vector<std::string> rights) {
        auto cap = services->directory_capability("data", path, std::move(rights));
        REQUIRE_OK(cap);
        auto handle = f->runtime->add_capability(paglet, std::move(*cap));
        REQUIRE_OK(handle);
        return *handle;
    }

    template <class Reply, class Request>
    Reply ask(const rt::PagletId& paglet, std::string_view op, const Request& request) {
        auto r = f->runtime->call(paglet, op, wire::to_msgpack(request), 20s);
        if (r.status != 0) throw std::runtime_error(std::string(op) + ": " + std::string(abi::error_name(r.status)));
        Reply reply{};
        if (!wire::from_msgpack(r.payload, reply))
            throw std::runtime_error(std::string(op) + ": reply does not decode");
        return reply;
    }

    std::vector<std::string> journal(const rt::PagletId& paglet) {
        f->runtime->wait_idle();
        return dec<std::vector<std::string>>(f->runtime->call(paglet, "journal", {}, 10s).payload);
    }

    fs::path root;
    std::unique_ptr<Fixture> f;
    std::shared_ptr<ps::SystemServices> services;
};

}  // namespace

PAGLETS_TEST("services: a roaming paglet finds and reads files and reports load and space (WP10 exit)") {
    const auto root = temp_dir("exit");
    write_text(root / "docs" / "a.csv", "x,y\n1,2\n");
    write_text(root / "docs" / "notes.txt", "not a table");
    write_text(root / "docs" / "sub" / "c.csv", "z\n3\n");
    write_text(root / "elsewhere.csv", "outside the lent directory");
    {
        Services s(root);
        const auto id = s.explorer();
        const auto dir = s.grant_dir(id, "docs", {"read"});
        const auto report = s.ask<explorer::Report>(id, "explore", explorer::Explore{dir, "**/*.csv"});
        CHECK(report.error.empty());
        CHECK(report.found == std::vector<std::string>({"a.csv", "sub/c.csv"}));
        CHECK(report.first_content == "x,y\n1,2\n");
#if defined(_WIN32)
        CHECK(report.os == "windows");
#elif defined(__APPLE__)
        CHECK(report.os == "macos");
#else
        CHECK(report.os == "linux");
#endif
        CHECK(!report.host_name.empty());
        CHECK(report.architecture == "x86_64" || report.architecture == "arm64");
        CHECK(report.cpu_count > 0);
        CHECK(report.memory_total > 0);
        CHECK(report.memory_available > 0 && report.memory_available <= report.memory_total);
        CHECK(report.cpu_percent >= 0 && report.cpu_percent <= 100);
        CHECK(report.cores > 0);
        CHECK(report.volumes > 0);
        CHECK(report.largest_volume > 0 && report.largest_volume_free <= report.largest_volume);
        CHECK(report.processes > 0);
        CHECK(report.processes_listed > 0 && report.processes_listed <= 5);
        if (!report.error.empty()) std::cerr << "    explore failed at " << report.error << "\n";

        // Without a lent directory there is nothing to read.
        const auto none = s.ask<explorer::Report>(id, "explore", explorer::Explore{0, "**"});
        CHECK(none.error == "find:bad_handle");
    }
    fs::remove_all(root);
}

PAGLETS_TEST("services: files writes, lists, moves and deletes within the rights of the capability") {
    const auto root = temp_dir("ops");
    fs::create_directories(root / "work");
    {
        Services s(root);
        const auto id = s.explorer();
        const auto all = s.grant_dir(id, "work", {"read", "write", "create", "delete"});
        const auto log = s.ask<std::vector<std::string>>(id, "file_ops", explorer::FileOps{all});
        CHECK(log ==
              std::vector<std::string>({"mkdir:a", "write:a/b.txt:11", "list:b.txt,", "move:c.txt", "remove:1"}));
        CHECK(read_text(root / "work" / "c.txt") == "hello files");
        CHECK(!fs::exists(root / "work" / "a"));

        const auto read_only = s.grant_dir(id, "work", {"read"});
        const auto denied = s.ask<std::vector<std::string>>(id, "file_ops", explorer::FileOps{read_only});
        REQUIRE(!denied.empty());
        CHECK(denied.front() == "mkdir:denied");
        CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{read_only, "c.txt"}) == "content:hello files");
        for (const char* bad : {"../x", "/etc/passwd", "a/../../x", "C:\\x", "x/./y"}) {
            CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{read_only, bad}) == "read:invalid_argument");
        }
        CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{read_only, "missing"}) == "read:not_found");
    }
    fs::remove_all(root);
}

#ifndef _WIN32
PAGLETS_TEST("services: symbolic links never lead out of the lent directory") {
    const auto root = temp_dir("links");
    const auto outside = temp_dir("outside");
    write_text(outside / "secret.txt", "secret");
    write_text(root / "box" / "inside.txt", "inside");
    fs::create_directory_symlink(outside, root / "box" / "escape");
    fs::create_symlink(outside / "secret.txt", root / "box" / "secret-link");
    fs::create_symlink(root / "box" / "inside.txt", root / "box" / "inside-link");
    {
        Services s(root);
        const auto id = s.explorer();
        const auto dir = s.grant_dir(id, "box", {"read"});
        CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{dir, "inside-link"}) == "content:inside");
        CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{dir, "secret-link"}) == "read:denied");
        CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{dir, "escape/secret.txt"}) == "read:denied");
        const auto report = s.ask<explorer::Report>(id, "explore", explorer::Explore{dir, "**"});
        // Links are listed, but directory links are not entered.
        CHECK(contains(report.found, "escape"));
        CHECK(!contains(report.found, "escape/secret.txt"));
        // A capability for a linked directory resolves outside its root.
        const auto via_link = s.grant_dir(id, "box/escape", {"read"});
        CHECK(s.ask<std::string>(id, "read_file", explorer::Explore{via_link, "secret.txt"}) == "read:denied");
    }
    fs::remove_all(root);
    fs::remove_all(outside);
}
#endif

PAGLETS_TEST("services: storage keeps values per paglet, within the quota, across restarts") {
    const auto root = temp_dir("storage");
    const auto state = temp_dir("storage-state");
    rt::PagletId id;
    {
        Services s(root, state, 64 * 1024);
        id = s.explorer();
        CHECK(s.ask<std::string>(id, "store", explorer::Store{"Key", text("upper")}) == "used:5:keys:1");
        CHECK(s.ask<std::string>(id, "store", explorer::Store{"key", text("lower")}) == "used:10:keys:2");
        CHECK(s.ask<std::string>(id, "store", explorer::Store{"bad key", text("x")}) == "put:invalid_argument");
        CHECK(s.ask<std::string>(id, "store", explorer::Store{"big", Bytes(100u * 1024, 1)}) == "put:quota");
        CHECK(s.ask<std::string>(id, "store", explorer::Store{"fits", Bytes(1000, 1)}) == "used:1010:keys:3");
        // Another paglet has its own space.
        const auto other = s.explorer();
        CHECK(s.ask<std::string>(other, "fetch", explorer::Store{"key", {}}) == "missing");
        REQUIRE(s.f->runtime->wait_idle());
    }
    {
        Services s(root, state);
        CHECK(s.ask<std::string>(id, "fetch", explorer::Store{"Key", {}}) == "value:upper");
        CHECK(s.ask<std::string>(id, "fetch", explorer::Store{"key", {}}) == "value:lower");
    }
    fs::remove_all(root);
    fs::remove_all(state);
}

PAGLETS_TEST("services: artifacts, pubsub and user-info") {
    const auto root = temp_dir("misc");
    {
        Services s(root);
        const auto id = s.explorer();
        const std::string hash = paglets::to_hex(paglets::sha256(text("stored content")));
        // The guest reads the artifact from offset 7.
        CHECK(s.ask<std::string>(id, "artifact", explorer::Text{"stored content"}) == hash + ":content");

        CHECK(s.ask<std::string>(id, "topic", explorer::Text{"rain"}) == "delivered:1");
        REQUIRE(s.f->runtime->wait_idle());
        CHECK(contains(s.journal(id), "news:rain:weather"));

        const auto reply = s.ask<std::string>(id, "notify", explorer::Text{"done"});
        CHECK(reply.starts_with("notified:"));
        const auto notes = s.services->notifications("local");
        REQUIRE(notes.size() == 1);
        CHECK(notes[0].title == "done" && notes[0].paglet == id && notes[0].level == ps::user_info::Level::success);
        CHECK(s.services->notifications("nobody").empty());
    }
    fs::remove_all(root);
}

PAGLETS_TEST("services: the directory publishes names and applies the service policy") {
    const auto root = temp_dir("directory");
    {
        Services s(root);
        const auto a = s.explorer("alice");
        const auto b = s.explorer("alice");
        const auto c = s.explorer("carol");
        // Every paglet has endpoints to the services with default operations.
        const auto info = dec<abi::SelfInfo>(s.f->runtime->call(a, "info", {}, 10s).payload);
        std::vector<std::string> names;
        for (const auto& [name, handle] : info.services) names.push_back(name);
        CHECK(names == std::vector<std::string>({"directory", "storage", "user-info"}));

        CHECK(s.ask<std::string>(a, "publish", explorer::Name{"echo-a", false}) == "published:echo-a");
        CHECK(s.ask<std::string>(b, "greet", explorer::Name{"echo-a"}) == "sent:journal:denied");
        REQUIRE(s.f->runtime->wait_idle());
        CHECK(contains(s.journal(a), "hello:hi"));
        CHECK(s.ask<std::string>(c, "greet", explorer::Name{"echo-a"}) == "lookup:not_found");
        CHECK(s.ask<std::string>(c, "publish", explorer::Name{"echo-a", false}) == "publish:bad_state");
        CHECK(s.ask<std::string>(c, "publish", explorer::Name{"files", false}) == "publish:invalid_argument");
        CHECK(s.ask<std::string>(a, "publish", explorer::Name{"echo-a", true}) == "published:echo-a");
        CHECK(s.ask<std::string>(c, "greet", explorer::Name{"echo-a"}) == "sent:journal:denied");
        REQUIRE_OK(s.f->runtime->dispose(a));
        REQUIRE(s.f->runtime->wait_idle());
        CHECK(s.ask<std::string>(b, "greet", explorer::Name{"echo-a"}) == "lookup:not_found");

        // Service endpoints carry the operations the policy allows.
        CHECK(s.ask<std::string>(b, "service_ops", explorer::Text{"server-info"}) ==
              "service:server-info:summary,load,volumes,processes,describe,fixed");
        s.services->set_policy([](const abi::SenderRecord& who, std::string_view service,
                                  const std::vector<std::string>& offered) -> std::vector<std::string> {
            if (service != "server-info") return offered;
            if (who.owner == "carol") return {};
            return {"load", "volumes", "reboot"};  // reboot is not offered and is dropped
        });
        CHECK(s.ask<std::string>(b, "service_ops", explorer::Text{"server-info"}) ==
              "service:server-info:load,volumes,fixed");
        CHECK(s.ask<std::string>(c, "service_ops", explorer::Text{"server-info"}) == "lookup:denied");
    }
    fs::remove_all(root);
}
