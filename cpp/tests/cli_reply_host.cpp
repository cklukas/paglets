// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Authenticated host fixture for the CLI's reply decoder. The driver selects
// one reply at a time through a file, without bypassing TLS/Noise or enrollment.
#include <paglets/mesh/crypto.hpp>
#include <paglets/mesh/ledger.hpp>
#include <paglets/mesh/policy.hpp>
#include <paglets/net/transport.hpp>
#if PAGLETS_HAVE_REFLECTION
#include "app.hpp"
#include <paglets/services/compute_slots.hpp>
#include <paglets/services/mesh_info.hpp>
#include <paglets/services/zombies.hpp>
#include <paglets/wire/reflect.hpp>
#endif

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace {
using namespace paglets::mesh;

Value reply(std::string mode, const PublicKey& key, const Fields& request) {
    const auto id = Value::bin(key);
    const Value location(Map{{"host", id},
                             {"host_name", Value("fixture")},
                             {"moves", Value(2)},
                             {"pin", Value("pin-1")},
                             {"until", Value(std::int64_t{1'800'000'000'000})},
                             {"released", Value(1)}});
    const Value host(Map{{"key", id}, {"name", Value("fixture")}});
    const Value pin(
        Array{Value("pin-1"), Value("demo"), Value(std::int64_t{1'800'000'000'000}), Value("owner"), Value("")});
    if (mode == "not-map") return Value(Array{});
    if (mode == "missing-ok") return Value(Map{});
    if (mode == "bad-ok") return Value(Map{{"ok", Value(1)}});
    if (mode == "refused") return Value(Map{{"ok", Value(false)}, {"e", Value("fixture refusal")}});
    Map out{{"ok", Value(true)}};
    auto put = [&](std::string name, Value value) { out.emplace_back(std::move(name), std::move(value)); };
    auto copy = [&](const Value& value) {
        for (const auto& p : *value.as_map()) out.push_back(p);
    };
    if (mode.starts_with("location")) {
        copy(location);
        if (mode == "location-missing") std::erase_if(out, [](const auto& p) { return p.first == "host"; });
        if (mode == "location-type") put("host", Value("not binary"));
        if (mode == "location-size") put("host", Value(Bytes(31)));
        if (mode == "location-moves") put("moves", Value("two"));
        if (mode == "location-pin") put("pin", Value(7));
        if (mode == "location-until") put("until", Value("tomorrow"));
        if (mode == "location-released") put("released", Value("one"));
        if (mode == "location-future") put("future", Value(Array{Value(), Value(7)}));
    } else if (mode.starts_with("hosts")) {
        Array list{host};
        if (mode == "hosts-row") list.push_back(Value("not a map"));
        if (mode == "hosts-key") list.emplace_back(Map{{"key", Value(Bytes(31))}});
        if (mode == "hosts-name") list.emplace_back(Map{{"key", id}, {"name", Value(1)}});
        if (mode == "hosts-relays") list.emplace_back(Map{{"key", id}, {"relays", Value(Array{Value(Bytes(31))})}});
        if (mode != "hosts-missing") put("hosts", mode == "hosts-type" ? Value(1) : Value(list));
    } else if (mode.starts_with("pins")) {
        Array list{pin};
        if (mode == "pins-row") list.emplace_back(Map{});
        if (mode == "pins-short") list.emplace_back(Array{Value("x")});
        if (mode.starts_with("pins-field-")) {
            Array bad = *pin.as_array();
            const auto i = static_cast<std::size_t>(mode.back() - '0');
            bad[i] = i == 2 ? Value("later") : Value(1);
            list.emplace_back(std::move(bad));
        }
        if (mode != "pins-missing") put("pins", mode == "pins-type" ? Value(1) : Value(list));
    } else if (mode.starts_with("push")) {
        put("added", Value(0));
        put("errors", mode == "push-type" ? Value(1) : mode == "push-row" ? Value(Array{Value(1)}) : Value(Array{}));
    } else if (mode.starts_with("moves")) {
        if (mode != "moves-missing") {
            put("moves_out", mode == "moves-type" ? Value("none") : Value(0));
            put("moves_in", Value(0));
            put("moves_failed", Value(0));
        }
    } else if (mode.starts_with("call")) {
        if (mode != "call-missing")
            put("status", mode == "call-type" ? Value("ok") : mode == "call-range" ? Value(INT64_MAX) : Value(0));
        put("payload", mode == "call-payload"   ? Value(1)
                       : mode == "call-timeout" ? Value(encode(Value(request.integer("timeout_ms").value_or(-1))))
                                                : Value(Bytes{}));
    } else if (mode.starts_with("status")) {
        put("key", mode == "status-key" ? Value(Bytes(31)) : id);
        put("name", Value("fixture"));
        put("mesh", Value("fixture"));
        put("records", Value(1));
        Array rows{Value(Array{Value("demo"), Value("module"), Value("owner"), Value("idle")})};
        if (mode == "status-row") rows.emplace_back(Array{Value("short")});
        if (mode == "status-field") rows.emplace_back(Array{Value(1), Value("m"), Value("o"), Value("idle")});
        put("paglets", mode == "status-type" ? Value(1) : Value(rows));
        if (mode == "status-system") put("system", Value(Array{Value(1)}));
    } else if (mode.starts_with("records")) {
        if (mode != "records-missing")
            put("records", mode == "records-type"  ? Value(1)
                           : mode == "records-row" ? Value(Array{Value(1)})
                                                   : Value(Array{}));
    } else if (mode.starts_with("modules")) {
        Array rows{Value(Map{{"hash", Value(std::string(64, 'a'))}, {"size", Value(7)}, {"users", Value(0)}})};
        if (mode == "modules-row") rows.emplace_back(1);
        if (mode == "modules-names")
            rows.emplace_back(Map{{"hash", Value(std::string(64, 'b'))},
                                  {"size", Value(1)},
                                  {"users", Value(0)},
                                  {"names", Value(Array{Value(1)})}});
        if (mode == "modules-interface")
            rows.emplace_back(Map{{"hash", Value(std::string(64, 'b'))},
                                  {"size", Value(1)},
                                  {"users", Value(0)},
                                  {"interface", Value("{broken")}});
        put("modules", mode == "modules-type" ? Value(1) : Value(rows));
    } else if (mode.starts_with("describe")) {
        put("interface",
            mode == "describe-type"    ? Value(1)
            : mode == "describe-json"  ? Value("{broken")
            : mode == "describe-shape" ? Value("[]")
            : mode == "describe-schema"
                ? Value(R"({"interface":1,"name":"fixture","messages":[{"name":"x","request":"Missing"}],"types":[]})")
                : Value(R"({"interface":1,"name":"fixture","launch":{"type":"none"},"messages":[],"types":[]})"));
    } else if (mode.starts_with("jobs")) {
        put("slots", Value(2));
        put("running", Value(0));
        if (mode == "jobs-cancel")
            put("cancelled", Value(false));
        else if (mode == "jobs-cancel-type")
            put("cancelled", Value(0));
        else {
            Array rows{Value(Map{{"id", Value(1)},
                                 {"paglet", Value("demo")},
                                 {"function", Value("work")},
                                 {"state", Value("queued")},
                                 {"cores", Value(1)},
                                 {"elapsed_ms", Value(0)}})};
            if (mode == "jobs-row") rows.emplace_back(1);
            put("jobs", mode == "jobs-type" ? Value(1) : mode == "jobs-empty" ? Value(Array{}) : Value(rows));
        }
    } else if (mode.starts_with("check")) {
        for (const auto* n : {"records", "replicas", "hosts", "takeovers", "restarts", "accepted"}) put(n, Value(0));
        if (mode != "check-missing")
            put("violations", mode == "check-type"  ? Value(1)
                              : mode == "check-row" ? Value(Array{Value(1)})
                                                    : Value(Array{}));
    } else if (mode.starts_with("dispatch")) {
        if (mode == "dispatch-pending") put("pending", Value(true));
        if (mode == "dispatch-type") put("moved", Value("yes"));
        if (mode == "dispatch-host") {
            put("moved", Value(true));
            put("host", Value(Bytes(31)));
        }
        if (mode == "dispatch-valid") {
            put("moved", Value(true));
            put("host", id);
            put("host_name", Value("fixture"));
        }
        if (mode == "dispatch-false-pending") {
            put("pending", Value(false));
            put("moved", Value(true));
            put("host", id);
        }
        if (mode == "dispatch-bad-pending") put("pending", Value(1));
        if (mode == "dispatch-conflict") {
            put("pending", Value(true));
            put("moved", Value(true));
            put("host", id);
        }
        if (mode == "dispatch-failed" || mode == "dispatch-failed-false-pending") {
            if (mode == "dispatch-failed-false-pending") put("pending", Value(false));
            put("moved", Value(false));
            put("reason", Value("fixture move refused"));
        }
#if PAGLETS_HAVE_REFLECTION
    } else if (mode.starts_with("landscape") || mode.starts_with("groups") || mode.starts_with("traces")) {
        Array rows;
        if (mode.starts_with("landscape"))
            rows.emplace_back(paglets::wire::to_msgpack(paglets::services::mesh_info::Snapshot{}));
        if (mode.starts_with("groups"))
            rows.emplace_back(paglets::wire::to_msgpack(paglets::services::zombies::GroupInfo{}));
        if (mode.starts_with("traces"))
            rows.emplace_back(paglets::wire::to_msgpack(paglets::services::zombies::TraceRecord{}));
        if (mode.ends_with("-row")) rows.emplace_back(1);
        if (mode.ends_with("-body")) rows.emplace_back(Bytes{0xc0});
        put(mode.starts_with("landscape") ? "hosts" : mode.starts_with("groups") ? "groups" : "records", Value(rows));
    } else if (mode.starts_with("slots")) {
        put("status", mode == "slots-type"
                          ? Value(1)
                          : Value(paglets::wire::to_msgpack(paglets::services::compute_slots::Status{})));
#endif
    } else if (mode == "empty") {
        // Older hosts acknowledge dispatch without its later outcome.
    } else {
        return Value(Map{{"ok", Value(false)}, {"e", Value("unknown fixture mode: " + mode)}});
    }
    return Value(std::move(out));
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    const std::filesystem::path dir(argv[1]);
    auto key = load_key(dir / "host.key", std::nullopt);
    auto ledger = Ledger::open(dir / "ledger");
    if (!key || !ledger) return 2;
    std::string passphrase;
    std::ifstream passfile(dir / "pass");
    std::getline(passfile, passphrase);
    auto owner = load_key(dir / "owner.key", passphrase);
    if (!owner) return 2;
    const Principal principal{std::string(32, 'a'), owner->id(), std::string(64, 'b'), "roaming"};
    auto duration_request = ledger->draft("grant-request", data::grant_request(principal, owner->public_key(),
        {Item{"files", {"read"}, "data", "docs/a"}}, std::numeric_limits<std::int64_t>::max(), "large duration"), false);
    if (!duration_request) return 2;
    duration_request->sign(*owner);
    if (!ledger->add(*duration_request)) return 2;
    std::ofstream(dir / "duration-request") << record_id_hex(duration_request->id());
    paglets::net::Transport host(
        *key, ledger->mesh(), {},
        [&](const paglets::net::Identity& peer) -> std::expected<void, std::string> {
            if (peer.role == paglets::net::PeerRole::owner && ledger->state().is_owner(peer.key)) return {};
            return std::unexpected(std::string("not the enrolled owner"));
        },
        [&](const paglets::net::Identity&, Bytes request) {
            auto decoded = decode(request);
            if (!decoded || !decoded->as_map()) return std::vector<Bytes>{};
            std::string mode;
            std::ifstream(dir / "mode") >> mode;
            {
                std::ofstream log(dir / "requests.log", std::ios::app);
                log << Fields{*decoded->as_map()}.str("t").value_or("") << ' ' << mode << '\n';
            }
            if (mode == "location-duration") {
                std::ofstream log(dir / "pin-durations.log", std::ios::app);
                log << Fields{*decoded->as_map()}.integer("duration_ms").value_or(-1) << '\n';
            }
            return std::vector<Bytes>{encode(reply(mode, key->public_key(), Fields{*decoded->as_map()}))};
        });
    if (auto started = host.start(); !started) {
        std::cerr << started.error() << '\n';
        return 2;
    }
#if PAGLETS_HAVE_REFLECTION
    // Exercise the real web action against this authenticated host. The CMake
    // driver replays each generated shell command through the actual CLI.
    {
        std::ofstream(dir / "mode") << "call-timeout";
        auto pass = paglets::client::Passphrases::make((dir / "pass").string());
        if (!pass) return 2;
        auto identity = paglets::client::Identity::open((dir / "owner.key").string(), (dir / "ledger").string(), *pass);
        if (!identity) return 2;
        paglets::webapp::Options options;
        options.connect = host.url();
        options.key_file = (dir / "owner.key").generic_string();
        options.ledger_dir = (dir / "ledger").generic_string();
        options.passphrase_file = (dir / "pass").generic_string();
        paglets::webapp::App app(options, std::move(*identity));
        using paglets::wire::Json;
        Json::Array responses;
        const std::vector<std::string> fields{"",
                                              ",\"timeout_ms\":30000",
                                              ",\"timeout_ms\":37500",
                                              ",\"timeout_ms\":30125",
                                              ",\"timeout_ms\":100",
                                              ",\"timeout_ms\":101",
                                              ",\"timeout_ms\":120000",
                                              ",\"timeout_ms\":1",
                                              ",\"timeout_ms\":120001",
                                              ",\"timeout_ms\":-100",
                                              ",\"timeout_ms\":\"invalid\""};
        for (const auto& field : fields) {
            paglets::web::Request request;
            request.method = "POST";
            request.path = "/api/call";
            request.body = "{\"paglet\":\"demo\",\"name\":\"run\"" + field + "}";
            const auto response = app.api(request);
            auto json = Json::parse(response.body);
            if (response.status != 200 || !json) return 2;
            responses.push_back(std::move(*json));
        }
        std::ofstream(dir / "web-timeouts.json") << Json(std::move(responses)).dump();
        Json::Array moves;
        for (const auto* mode : {"dispatch-valid", "dispatch-false-pending", "dispatch-pending", "dispatch-failed",
                                 "dispatch-failed-false-pending", "empty", "dispatch-bad-pending"}) {
            std::ofstream(dir / "mode") << mode;
            paglets::web::Request request;
            request.method = "POST";
            request.path = "/api/dispatch";
            request.body = R"({"paglet":"demo","destination":"there"})";
            const auto response = app.api(request);
            auto json = Json::parse(response.body);
            if (!json) return 2;
            moves.emplace_back(Json::Object{{"mode", Json(std::string(mode))},
                                            {"http", Json::integer(response.status)},
                                            {"body", std::move(*json)}});
        }
        std::ofstream(dir / "web-dispatches.json") << Json(std::move(moves)).dump();
    }
#endif
    {
        std::ofstream url(dir / "url");
        url << host.url();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(210);
    while (!std::filesystem::exists(dir / "stop")) {
        if (std::chrono::steady_clock::now() >= deadline) return 3;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (std::filesystem::exists(dir / "verify-duration-grant")) {
        auto approved = Ledger::open(dir / "ledger");
        if (!approved) return 2;
        bool found = false;
        for (const auto& [id, grant] : approved->state().grants) {
            if (grant.request == duration_request->id() && grant.expires == std::numeric_limits<std::int64_t>::max()) found = true;
        }
        if (!found) {
            std::cerr << "the approved large-duration grant was lost or its expiration overflowed\n";
            return 2;
        }
    }
    return 0;
}
