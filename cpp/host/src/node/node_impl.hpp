// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internal to the node: its state (Node::Impl) and helpers, shared by
// node.cpp (ledger glue, grants, code mobility) and movement.cpp (moving
// paglets between hosts).

#pragma once

#include <paglets/node/node.hpp>

#include <paglets/services/contract.hpp>
#include <paglets/services/grants.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/reflect.hpp>

#include <algorithm>
#include <chrono>
#include <deque>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <set>

namespace paglets::node {

namespace detail {

// Memory pages by hash, least recently used first out; pages of images this
// host sent or received, so later moves transfer only changed pages
// (planning/cpp-networking.md, section 6).
class PageCache {
public:
    explicit PageCache(std::size_t budget) : budget_(budget) {}
    void set_budget(std::size_t budget) {
        budget_ = budget;
        trim();
    }
    void add(const paglets::Digest& hash, std::span<const std::uint8_t> page) {
        if (auto it = pages_.find(hash); it != pages_.end()) {
            lru_.splice(lru_.begin(), lru_, it->second.second);
            return;
        }
        lru_.push_front(hash);
        pages_.emplace(hash, std::pair{std::vector<std::uint8_t>(page.begin(), page.end()), lru_.begin()});
        bytes_ += page.size();
        trim();
    }
    void add_image(const wasm::Snapshot& image) {
        std::size_t index = 0;
        for (const auto& p : image.pages) {
            if (p.zero) continue;
            add(p.hash, std::span<const std::uint8_t>(image.data).subspan(index * wasm::page_size, wasm::page_size));
            ++index;
        }
    }
    const std::vector<std::uint8_t>* find(const paglets::Digest& hash) {
        auto it = pages_.find(hash);
        if (it == pages_.end()) return nullptr;
        lru_.splice(lru_.begin(), lru_, it->second.second);
        return &it->second.first;
    }
    std::size_t bytes() const { return bytes_; }

private:
    void trim() {
        while (bytes_ > budget_ && !lru_.empty()) {
            auto it = pages_.find(lru_.back());
            bytes_ -= it->second.first.size();
            pages_.erase(it);
            lru_.pop_back();
        }
    }
    std::size_t budget_;
    std::size_t bytes_ = 0;
    std::list<paglets::Digest> lru_;
    std::map<paglets::Digest, std::pair<std::vector<std::uint8_t>, std::list<paglets::Digest>::iterator>> pages_;
};

namespace fs = std::filesystem;
namespace g = services::grants;
using services::Operation;
using services::Result;

constexpr std::int64_t min_duration_ms = 1000;
constexpr std::int64_t max_duration_ms = 30LL * 24 * 3600 * 1000;

// Code mobility frames (planning/cpp-modules.md, section 5).
constexpr std::string_view frame_module_want = "module-want";        // m, r
constexpr std::string_view frame_module_part = "module-part";        // m, r, o, n, d
constexpr std::string_view frame_module_missing = "module-missing";  // m, r, e
constexpr std::string_view frame_launch = "launch";                  // r, p, a
constexpr std::string_view frame_launch_result = "launch-result";    // r, id | e
constexpr std::size_t module_part_bytes = 1024 * 1024;
constexpr std::size_t max_module_bytes = 64u * 1024 * 1024;

inline std::string hex(const mesh::RecordId& id) {
    return mesh::record_id_hex(id);
}

inline std::optional<mesh::RecordId> parse_hex_id(std::string_view text) {
    return mesh::parse_key_id(text);  // record IDs have the same form as key IDs
}

inline std::string new_cap_id() {
    std::array<std::uint8_t, 16> bytes{};
    wasm::fill_random(bytes);
    return to_hex(std::span<const std::uint8_t>(bytes));
}

inline mesh::Item to_mesh(const g::Item& i) {
    return mesh::Item{i.service, i.ops, i.root, i.path};
}

inline g::Item to_contract(const mesh::Item& i) {
    return g::Item{i.service, i.ops, i.root, i.path};
}

// Relative paths of files items: the rules of dir capabilities.
inline bool valid_path(std::string_view path) {
    if (path.empty()) return true;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = std::min(path.find('/', start), path.size());
        const std::string_view seg = path.substr(start, end - start);
        if (seg.empty() || seg == "." || seg == ".." || seg.find_first_of("\\:") != std::string_view::npos) {
            return false;
        }
        start = end + 1;
    }
    return true;
}

}  // namespace detail

using namespace detail;

class GrantsService;

struct Node::Impl {
    Impl(runtime::Runtime& rt, std::shared_ptr<services::SystemServices> svc, mesh::Ledger l, mesh::SigningKey k,
         mesh::GossipTransport& t, fs::path dir)
        : runtime(rt),
          services(std::move(svc)),
          ledger(std::move(l)),
          key(std::move(k)),
          transport(t),
          state_dir(std::move(dir)) {}

    runtime::Runtime& runtime;
    std::shared_ptr<services::SystemServices> services;
    mutable std::mutex mu;
    mesh::Ledger ledger;
    mesh::SigningKey key;
    mesh::GossipTransport& transport;
    std::unique_ptr<mesh::Replica> replica;
    fs::path state_dir;
    bool dirty = false;
    std::set<std::string> materialized;               // grants delivered on this host
    std::set<std::string> answered;                   // denials delivered on this host
    std::map<std::string, mesh::Passport> passports;  // of paglets created here
    runtime::SystemContext* grants_ctx = nullptr;
    // The ledger state as of the last sync, for the runtime's module
    // admission check (called under the runtime's lock, so it cannot take mu).
    std::mutex trust_mu;
    std::shared_ptr<const mesh::LedgerState> trust;

    // Code mobility.
    using Clock = std::chrono::steady_clock;
    using ModuleDone = std::function<void(const std::expected<std::string, std::string>&)>;
    struct Fetch {
        std::deque<mesh::PublicKey> candidates;  // hosts still to ask
        std::optional<mesh::PublicKey> peer;     // asked now
        std::uint64_t request = 0;
        runtime::Bytes data;
        std::int64_t total = -1;
        Clock::time_point deadline{};
        std::vector<std::string> failures;
        std::vector<ModuleDone> waiters;
    };
    std::vector<std::shared_ptr<runtime::ModuleSource>> sources;
    std::chrono::milliseconds fetch_timeout{10000};
    std::map<paglets::Digest, Fetch> fetches;
    std::uint64_t next_request = 1;
    std::map<std::uint64_t, std::pair<mesh::PublicKey, LaunchStatus>> launches;  // started here: target, status

    // Movement (movement.cpp, planning/cpp-networking.md, section 6).
    struct Ticket {
        std::string target;  // host name, key ID, label:<label> or any
        int retries = 3;     // hosts tried at most
        bool activate = true;
    };
    struct Outgoing {  // a paglet leaving this host
        runtime::Departure departure;
        std::optional<mesh::Passport> passport;
        Ticket ticket;
        std::deque<mesh::PublicKey> candidates;
        mesh::PublicKey target{};
        Clock::time_point deadline{};
        std::vector<std::string> failures;
        int attempts = 0;
    };
    std::map<std::uint64_t, Outgoing> outgoing;  // by request (one per offer)
    struct Decision {
        bool committed = false;
        mesh::PublicKey to{};
        Clock::time_point expires{};
    };
    std::map<std::uint64_t, Decision> decisions;  // outcomes of offers, for queries
    struct Incoming {                             // a paglet arriving here
        enum class Phase { module, pages, prepared };
        Phase phase = Phase::module;
        runtime::TravelState state;
        wasm::Snapshot image;
        std::vector<paglets::Digest> data_hashes;  // non-zero pages in order
        std::vector<bool> have;
        std::size_t missing = 0;
        std::optional<mesh::Passport> passport;
        bool clone = false;
        runtime::Bytes clone_args;
        std::vector<runtime::Cap> clone_caps;
        std::string original;
        bool activate = true;
        Clock::time_point deadline{};
    };
    std::map<std::pair<mesh::PublicKey, std::uint64_t>, Incoming> incoming;  // by source and request
    PageCache pages{256u * 1024 * 1024};
    std::chrono::milliseconds move_timeout{30000};
    MoveStats move_stats;

    void install_mobility();
    void on_depart(runtime::Departure d);  // from the runtime, without locks
    void start_move(runtime::Departure d);
    void offer_next(std::uint64_t request);
    void fail_move(Outgoing& o, const std::string& why);
    std::vector<mesh::PublicKey> candidates(const Ticket& ticket, const mesh::Principal& principal,
                                            const std::optional<std::vector<mesh::Item>>& manifest,
                                            std::vector<std::string>& why_not);
    std::optional<runtime::Cap> recreate(const runtime::PagletId& paglet, const runtime::Cap& cap);
    void take_spawns();
    void check_move_deadlines();
    void on_deliver(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_offer(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_want(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_pages(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_ready(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_refused(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_commit(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_abort(const mesh::PublicKey& from, const mesh::Fields& f);
    void on_move_query(const mesh::PublicKey& from, const mesh::Fields& f);
    void arrival_pages_complete(const mesh::PublicKey& from, std::uint64_t request);
    void refuse_arrival(const mesh::PublicKey& from, std::uint64_t request, const std::string& why);

    std::expected<void, std::string> admit(const std::string& module, runtime::TrustClass trust_class) {
        std::shared_ptr<const mesh::LedgerState> st;
        {
            std::lock_guard lock(trust_mu);
            st = trust;
        }
        if (!st) return {};
        auto digest = mesh::parse_key_id(module);
        if (!digest) return std::unexpected("invalid module hash " + module);
        auto v = mesh::check_module(*st, *digest, runtime::to_string(trust_class));
        if (!v.allowed) return std::unexpected(v.reason);
        return {};
    }

    // -- mu held -----------------------------------------------------------

    std::expected<mesh::Record, std::string> issue(std::string type, mesh::Map data) {
        auto r = ledger.draft(std::move(type), std::move(data), false);
        if (!r) return r;
        r->sign(key);
        if (auto added = replica->submit(*r); !added) return std::unexpected(added.error());
        return r;
    }

    static mesh::Principal principal_of(const abi::SenderRecord& s) {
        return mesh::Principal{s.id, s.owner, s.module, s.trust};
    }

    std::expected<runtime::Cap, std::string> materialize(const mesh::RecordId& id, const mesh::Grant& grant) {
        runtime::Cap cap;
        if (grant.item.service == "files") {
            auto dir = services->directory_capability(grant.item.root, grant.item.path, grant.item.ops, hex(id),
                                                      grant.expires);
            if (!dir) return std::unexpected(dir.error());
            cap = std::move(*dir);
        } else {
            if (!runtime.system_paglet(grant.item.service)) {
                return std::unexpected("no service " + grant.item.service + " on this host");
            }
            cap.kind = runtime::Cap::Kind::endpoint;
            cap.target = runtime::system_paglet_id(grant.item.service);
            cap.ops = grant.item.ops;
            cap.transferable = false;
            cap.grant = hex(id);
            cap.expires = grant.expires;
        }
        cap.id = new_cap_id();
        return cap;
    }

    // -- code mobility (mu held) -----------------------------------------------

    void warn(const std::string& text) const {
        if (grants_ctx != nullptr) grants_ctx->log(abi::LogLevel::warning, text);
    }

    void send_frame(const mesh::PublicKey& to, mesh::Map frame) {
        transport.send(to, mesh::encode(mesh::Value(std::move(frame))));
    }

    // Gets `module` and calls `done` with its hex hash, or why it could not.
    void need_module(const paglets::Digest& module, std::optional<mesh::PublicKey> from, ModuleDone done) {
        const std::string h = to_hex(module);
        if (runtime.modules().contains(h)) {
            done(h);
            return;
        }
        if (auto it = fetches.find(module); it != fetches.end()) {
            it->second.waiters.push_back(std::move(done));
            if (from && *from != key.public_key() && it->second.peer != from &&
                std::ranges::find(it->second.candidates, *from) == it->second.candidates.end()) {
                it->second.candidates.push_front(*from);
            }
            return;
        }
        Fetch f;
        for (const auto& source : sources) {
            auto bytes = source->fetch(module);
            if (!bytes) {
                f.failures.push_back(source->name() + ": " + bytes.error());
                continue;
            }
            auto added = runtime.modules().add_verified(module, std::move(*bytes));
            if (added) {
                done(*added);
                return;
            }
            f.failures.push_back(source->name() + ": " + added.error());
        }
        if (from && *from != key.public_key()) f.candidates.push_back(*from);
        for (const auto& peer : replica->peers()) {
            if (peer != key.public_key() && std::ranges::find(f.candidates, peer) == f.candidates.end()) {
                f.candidates.push_back(peer);
            }
        }
        f.waiters.push_back(std::move(done));
        fetches.emplace(module, std::move(f));
        ask_next(module);
    }

    void ask_next(const paglets::Digest& module) {
        Fetch& f = fetches.at(module);
        f.peer.reset();
        f.data.clear();
        f.total = -1;
        if (f.candidates.empty()) {
            std::string why = "module " + to_hex(module).substr(0, 16) + " not found";
            for (const auto& failure : f.failures) why += "; " + failure;
            finish_fetch(module, std::unexpected(why));
            return;
        }
        f.peer = f.candidates.front();
        f.candidates.pop_front();
        f.request = next_request++;
        f.deadline = Clock::now() + fetch_timeout;
        send_frame(*f.peer, mesh::Map{{"t", mesh::Value(frame_module_want)},
                                      {"m", mesh::Value::bin(module)},
                                      {"r", mesh::Value(static_cast<std::int64_t>(f.request))}});
    }

    void peer_failed(const paglets::Digest& module, const std::string& why) {
        Fetch& f = fetches.at(module);
        f.failures.push_back("host " + mesh::key_id(*f.peer).substr(0, 16) + ": " + why);
        ask_next(module);
    }

    void finish_fetch(const paglets::Digest& module, const std::expected<std::string, std::string>& result) {
        auto node = fetches.extract(module);
        for (auto& done : node.mapped().waiters) done(result);
    }

    // The fetch a frame from `from` answers, if any.
    Fetch* answered_fetch(const mesh::PublicKey& from, const mesh::Fields& f, paglets::Digest& module) {
        auto m = f.fixed<32>("m");
        auto r = f.integer("r");
        if (!m || !r) return nullptr;
        auto it = fetches.find(*m);
        if (it == fetches.end() || it->second.peer != from || it->second.request != static_cast<std::uint64_t>(*r)) {
            return nullptr;
        }
        module = *m;
        return &it->second;
    }

    void on_module_want(const mesh::PublicKey& from, const mesh::Fields& f) {
        auto m = f.fixed<32>("m");
        auto r = f.integer("r");
        if (!m || !r) return;
        auto answer = [&](std::string_view why) {
            send_frame(from, mesh::Map{{"t", mesh::Value(frame_module_missing)},
                                       {"m", mesh::Value::bin(*m)},
                                       {"r", mesh::Value(*r)},
                                       {"e", mesh::Value(why)}});
        };
        // Modules go to enrolled hosts only.
        if (!ledger.state().is_host(from)) return answer("not an enrolled host");
        auto bytes = runtime.modules().bytes(to_hex(*m));
        if (!bytes) return answer("module not here");
        const auto total = static_cast<std::int64_t>(bytes->size());
        std::size_t offset = 0;
        do {
            const std::size_t n = std::min(module_part_bytes, bytes->size() - offset);
            send_frame(from, mesh::Map{{"t", mesh::Value(frame_module_part)},
                                       {"m", mesh::Value::bin(*m)},
                                       {"r", mesh::Value(*r)},
                                       {"o", mesh::Value(static_cast<std::int64_t>(offset))},
                                       {"n", mesh::Value(total)},
                                       {"d", mesh::Value(runtime::Bytes(
                                                 bytes->begin() + static_cast<std::ptrdiff_t>(offset),
                                                 bytes->begin() + static_cast<std::ptrdiff_t>(offset + n)))}});
            offset += n;
        } while (offset < bytes->size());
    }

    void on_module_part(const mesh::PublicKey& from, const mesh::Fields& f) {
        paglets::Digest module{};
        Fetch* fetch = answered_fetch(from, f, module);
        if (fetch == nullptr) return;
        auto offset = f.integer("o");
        auto total = f.integer("n");
        auto data = f.bin("d");
        if (!offset || !total || !data || *total <= 0 || static_cast<std::uint64_t>(*total) > max_module_bytes ||
            (fetch->total >= 0 && fetch->total != *total) || *offset != static_cast<std::int64_t>(fetch->data.size()) ||
            fetch->data.size() + data->size() > static_cast<std::uint64_t>(*total)) {
            return peer_failed(module, "malformed module transfer");
        }
        fetch->total = *total;
        fetch->data.insert(fetch->data.end(), data->begin(), data->end());
        fetch->deadline = Clock::now() + fetch_timeout;
        if (static_cast<std::int64_t>(fetch->data.size()) < fetch->total) return;
        auto added = runtime.modules().add_verified(module, std::move(fetch->data));
        if (!added) return peer_failed(module, added.error());
        finish_fetch(module, *added);
    }

    void on_module_missing(const mesh::PublicKey& from, const mesh::Fields& f) {
        paglets::Digest module{};
        if (answered_fetch(from, f, module) == nullptr) return;
        peer_failed(module, f.str("e").value_or("module not there"));
    }

    void check_fetch_deadlines() {
        const auto now = Clock::now();
        std::vector<paglets::Digest> late;
        for (const auto& [module, f] : fetches) {
            if (f.peer && now >= f.deadline) late.push_back(module);
        }
        for (const auto& module : late) peer_failed(module, "no answer in time");
    }

    // Creates a root paglet from its passport (Node::create).
    std::expected<runtime::PagletId, std::string> create_locked(std::string_view module, const mesh::Passport& passport,
                                                                runtime::Bytes args) {
        auto hash = mesh::parse_key_id(module);  // module hashes have the form of key IDs
        if (!hash) return std::unexpected("module " + std::string(module) + " is not a hex SHA-256");
        if (!passport.links().empty()) return std::unexpected(std::string("a root paglet needs a root passport"));
        if (auto ok = mesh::verify_passport(passport, ledger.state(), passport.paglet(), *hash, mesh::unix_ms()); !ok) {
            return std::unexpected("passport: " + ok.error());
        }
        runtime::CreateOptions options;
        options.args = std::move(args);
        options.trust = runtime::TrustClass::roaming;
        options.owner = mesh::key_id(passport.root().owner);
        options.id = passport.paglet();
        auto id = runtime.create(module, std::move(options));
        if (!id) return id;
        passports.emplace(*id, passport);
        save_passport(passport);
        sync_locked();  // grants approved in advance
        return id;
    }

    using LaunchDone = std::function<void(const std::expected<runtime::PagletId, std::string>&)>;

    // A launch arriving from `origin` (or started on this host): checks
    // what can be checked before the module is here, gets the module, then
    // creates the paglet.
    void start_launch(const std::optional<mesh::PublicKey>& origin, runtime::Bytes passport_bytes, runtime::Bytes args,
                      LaunchDone done) {
        const mesh::LedgerState& st = ledger.state();
        if (origin && !st.is_host(*origin))
            return done(std::unexpected(std::string("launch from a host that is not enrolled")));
        auto passport = mesh::Passport::decode(passport_bytes);
        if (!passport) return done(std::unexpected("passport: " + passport.error()));
        if (!passport->links().empty())
            return done(std::unexpected(std::string("a root paglet needs a root passport")));
        const paglets::Digest module = passport->module();
        if (auto ok = mesh::verify_passport(*passport, st, passport->paglet(), module, mesh::unix_ms()); !ok) {
            return done(std::unexpected("passport: " + ok.error()));
        }
        if (auto v = mesh::check_module(st, module, "roaming"); !v.allowed) return done(std::unexpected(v.reason));
        need_module(module, origin,
                    [this, passport = std::move(*passport), args = std::move(args),
                     done = std::move(done)](const std::expected<std::string, std::string>& hash) mutable {
                        if (!hash) return done(std::unexpected(hash.error()));
                        done(create_locked(*hash, passport, std::move(args)));
                    });
    }

    void on_launch(const mesh::PublicKey& from, const mesh::Fields& f) {
        auto r = f.integer("r");
        auto p = f.bin("p");
        auto a = f.bin("a");
        if (!r || !p) return;
        const std::int64_t request = *r;
        start_launch(from, std::move(*p), a ? std::move(*a) : runtime::Bytes{},
                     [this, from, request](const std::expected<runtime::PagletId, std::string>& result) {
                         mesh::Map answer{{"t", mesh::Value(frame_launch_result)}, {"r", mesh::Value(request)}};
                         if (result) {
                             answer.emplace_back("id", mesh::Value(*result));
                         } else {
                             answer.emplace_back("e", mesh::Value(result.error()));
                         }
                         send_frame(from, std::move(answer));
                     });
    }

    void on_launch_result(const mesh::PublicKey& from, const mesh::Fields& f) {
        auto r = f.integer("r");
        if (!r) return;
        auto it = launches.find(static_cast<std::uint64_t>(*r));
        if (it == launches.end() || it->second.first != from ||
            it->second.second.state != LaunchStatus::State::pending) {
            return;
        }
        LaunchStatus& status = it->second.second;
        if (auto id = f.str("id")) {
            status.state = LaunchStatus::State::running;
            status.paglet = std::move(*id);
        } else {
            status.state = LaunchStatus::State::failed;
            status.error = f.str("e").value_or("launch failed");
        }
    }

    // Node frames; false for gossip frames.
    bool handle_node_frame(const mesh::PublicKey& from, std::span<const std::uint8_t> frame) {
        if (frame.size() > mesh::Replica::max_frame) return false;
        auto v = mesh::decode(frame);
        if (!v || v->as_map() == nullptr) return false;
        const mesh::Fields f{*v->as_map()};
        auto type = f.str("t");
        if (!type) return false;
        if (*type == frame_module_want) {
            on_module_want(from, f);
        } else if (*type == frame_module_part) {
            on_module_part(from, f);
        } else if (*type == frame_module_missing) {
            on_module_missing(from, f);
        } else if (*type == frame_launch) {
            on_launch(from, f);
        } else if (*type == frame_launch_result) {
            on_launch_result(from, f);
        } else if (*type == "deliver") {
            on_deliver(from, f);
        } else if (*type == "move-offer") {
            on_move_offer(from, f);
        } else if (*type == "move-want") {
            on_move_want(from, f);
        } else if (*type == "move-pages") {
            on_move_pages(from, f);
        } else if (*type == "move-ready") {
            on_move_ready(from, f);
        } else if (*type == "move-refused") {
            on_move_refused(from, f);
        } else if (*type == "move-commit") {
            on_move_commit(from, f);
        } else if (*type == "move-abort") {
            on_move_abort(from, f);
        } else if (*type == "move-query") {
            on_move_query(from, f);
        } else {
            return false;
        }
        return true;
    }

    void load() {
        if (state_dir.empty()) return;
        auto bytes = wasm::read_file((state_dir / "node.state").string());
        if (!bytes) return;
        auto v = mesh::decode(*bytes);
        if (!v || v->as_map() == nullptr) return;
        const mesh::Fields f{*v->as_map()};
        if (auto m = f.strings("materialized")) materialized.insert(m->begin(), m->end());
        if (auto a = f.strings("answered")) answered.insert(a->begin(), a->end());
        // Moves this host committed: destinations may still ask.
        if (const mesh::Array* committed = f.array("committed")) {
            const std::int64_t now = mesh::unix_ms();
            for (const auto& e : *committed) {
                const mesh::Array* a = e.as_array();
                if (a == nullptr || a->size() != 3 || !(*a)[0].as_int() || !(*a)[2].as_int()) continue;
                const mesh::Bytes* to = (*a)[1].as_bin();
                if (to == nullptr || to->size() != 32 || *(*a)[2].as_int() <= now) continue;
                Decision d;
                d.committed = true;
                std::copy(to->begin(), to->end(), d.to.begin());
                d.expires = Clock::now() + std::chrono::milliseconds(*(*a)[2].as_int() - now);
                decisions[static_cast<std::uint64_t>(*(*a)[0].as_int())] = d;
                next_request = std::max(next_request, static_cast<std::uint64_t>(*(*a)[0].as_int()) + 1);
            }
        }
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(state_dir / "passports", ec)) {
            auto p = wasm::read_file(entry.path().string());
            if (!p) continue;
            if (auto passport = mesh::Passport::decode(*p)) passports.emplace(passport->paglet(), std::move(*passport));
        }
    }

    void save_passport(const mesh::Passport& passport) const {
        if (state_dir.empty()) return;
        std::error_code ec;
        fs::create_directories(state_dir / "passports", ec);
        const fs::path file = state_dir / "passports" / (passport.paglet() + ".passport");
        const fs::path temp = file.string() + ".tmp";
        if (wasm::write_file(temp.string(), passport.encode())) fs::rename(temp, file, ec);
    }

    void save() const {
        if (state_dir.empty()) return;
        auto list = [](const std::set<std::string>& s) {
            mesh::Array a;
            for (const auto& e : s) a.emplace_back(e);
            return mesh::Value(std::move(a));
        };
        mesh::Array committed;
        const auto now = Clock::now();
        const std::int64_t unix_now = mesh::unix_ms();
        for (const auto& [r, d] : decisions) {
            if (!d.committed || d.expires <= now) continue;
            committed.emplace_back(mesh::Array{
                mesh::Value(static_cast<std::int64_t>(r)), mesh::Value::bin(d.to),
                mesh::Value(unix_now +
                            std::chrono::duration_cast<std::chrono::milliseconds>(d.expires - now).count())});
        }
        const auto bytes = mesh::encode(mesh::Value(mesh::Map{{"materialized", list(materialized)},
                                                              {"answered", list(answered)},
                                                              {"committed", mesh::Value(std::move(committed))}}));
        std::error_code ec;
        fs::create_directories(state_dir, ec);
        const fs::path temp = state_dir / "node.state.tmp";
        if (wasm::write_file(temp.string(), bytes)) fs::rename(temp, state_dir / "node.state", ec);
    }

    void sync_if_dirty() {
        if (!dirty) return;
        dirty = false;
        sync_locked();
    }

    void sync_locked() {
        const mesh::LedgerState& st = ledger.state();
        std::set<std::string, std::less<>> ended;
        for (const auto& id : st.ended_grants) ended.insert(hex(id));
        runtime.set_revoked_grants(std::move(ended));
        {
            std::lock_guard lock(trust_mu);
            trust = std::make_shared<const mesh::LedgerState>(st);
        }
        // Paglets whose module lost the mesh's trust end (also paglets
        // recovered before this host saw the change).
        for (const auto& p : runtime.list()) {
            if (p.module.empty()) continue;  // native system paglets
            if (auto ok = admit(p.module, p.trust); !ok) {
                (void)runtime.terminate(p.id, "module no longer trusted: " + ok.error());
            }
        }
        if (grants_ctx == nullptr) return;
        bool changed = false;
        const std::int64_t now = mesh::unix_ms();
        // Approvals: admin grants for paglets on this host.
        for (const auto& [id, grant] : st.grants) {
            const std::string h = hex(id);
            if (!grant.by_admin || materialized.contains(h) || grant.expires <= now) continue;
            if (!runtime.info(grant.principal.paglet) || !mesh::selects(st, grant.hosts, key.public_key())) continue;
            auto cap = materialize(id, grant);
            if (!cap) {
                grants_ctx->log(abi::LogLevel::warning, "grant " + h.substr(0, 16) + ": " + cap.error());
                continue;
            }
            g::Answer a{g::Status::granted,
                        to_contract(grant.item),
                        h,
                        grant.request ? hex(*grant.request) : std::string(),
                        std::string(),
                        grant.expires,
                        "approved"};
            if (grants_ctx->send(grant.principal.paglet, "grants.granted", wire::to_msgpack(a), {std::move(*cap)})) {
                materialized.insert(h);
                changed = true;
            }
        }
        // Denials of requests of paglets on this host.
        for (const mesh::Record* r : ledger.records()) {
            if (r->type() != "request-deny") continue;
            const mesh::Value* request = r->fields().get("request");
            const mesh::Bytes* bin = request == nullptr ? nullptr : request->as_bin();
            if (bin == nullptr || bin->size() != 32) continue;
            mesh::RecordId rid{};
            std::copy(bin->begin(), bin->end(), rid.begin());
            const std::string h = hex(rid);
            if (answered.contains(h)) continue;
            const mesh::Record* req = ledger.find(rid);
            if (req == nullptr || req->type() != "grant-request") continue;
            auto parsed = mesh::parse_grant_request(*req);
            if (!parsed || !runtime.info(parsed->principal.paglet)) continue;
            g::Answer a{g::Status::denied,
                        parsed->items.empty() ? g::Item{} : to_contract(parsed->items.front()),
                        std::string(),
                        h,
                        std::string(),
                        0,
                        r->fields().str("reason").value_or("denied by an admin")};
            if (grants_ctx->send(parsed->principal.paglet, "grants.denied", wire::to_msgpack(a))) {
                answered.insert(h);
                changed = true;
            }
        }
        if (changed) save();
    }
};

}  // namespace paglets::node
