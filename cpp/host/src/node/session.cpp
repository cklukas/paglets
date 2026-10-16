// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// CLI sessions of admins and owners (planning/cpp-networking.md, section 8).

#include "node_impl.hpp"

#include <paglets/protocols/zombie/trace.hpp>
#include <paglets/wire/interface.hpp>
#include <paglets/wire/json_msgpack.hpp>

#include <future>
#include <map>

namespace paglets::node {

namespace {

constexpr auto session_wait = std::chrono::seconds(20);
constexpr std::int64_t max_session_pin_ms = 24LL * 3600 * 1000;
constexpr std::int64_t max_dispatch_wait_ms = 120'000;

runtime::Bytes answer(mesh::Map m) {
    m.emplace_back("ok", mesh::Value(true));
    return mesh::encode(mesh::Value(std::move(m)));
}

runtime::Bytes refusal(std::string_view why) {
    return mesh::encode(mesh::Value(mesh::Map{{"ok", mesh::Value(false)}, {"e", mesh::Value(why)}}));
}

// Interface descriptions of modules by hash (the bytes of a hash never
// change): JSON text, empty for modules without one.
std::string module_interface(runtime::ModuleStore& modules, const std::string& hash) {
    static std::mutex mu;
    static std::map<std::string, std::string> cache;
    {
        std::lock_guard lock(mu);
        if (auto it = cache.find(hash); it != cache.end()) return it->second;
    }
    std::string text;
    if (auto bytes = modules.bytes(hash)) {
        if (auto d = wire::interface::of_module(*bytes); d && *d) text = (*d)->dump();
    } else {
        return {};
    }
    std::lock_guard lock(mu);
    if (cache.size() > 1024) cache.clear();
    cache[hash] = text;
    return text;
}

}  // namespace

// Runs a lookup (location.cpp) and waits for its end; called without locks.
static Node::Impl::LookupResult run_lookup(Node::Impl& impl, Node::Impl::Lookup l) {
    auto promise = std::make_shared<std::promise<Node::Impl::LookupResult>>();
    auto future = promise->get_future();
    l.done = [promise](const Node::Impl::LookupResult& r) { promise->set_value(r); };
    {
        std::lock_guard lock(impl.mu);
        impl.start_lookup(std::move(l));
    }
    if (future.wait_for(session_wait) != std::future_status::ready) {
        return Node::Impl::LookupResult{abi::timeout, "no answer in time"};
    }
    return future.get();
}

runtime::Bytes Node::answer_session(const mesh::PublicKey& peer, std::string_view role, runtime::Bytes request) {
    auto v = mesh::decode(request);
    if (!v || v->as_map() == nullptr) return refusal("malformed request");
    const mesh::Fields f{*v->as_map()};
    auto type = f.str("t");
    if (!type) return refusal("malformed request");
    const bool admin = role == "admin";
    const std::string peer_id = mesh::key_id(peer);

    // Authentication proves the key, not continuing membership. A channel
    // may have been opened before this owner/admin was removed.
    {
        std::lock_guard lock(impl_->mu);
        const auto& state = impl_->ledger.state();
        if (role == "admin") {
            if (!state.is_admin(peer)) return refusal("not a current admin of this mesh");
        } else if (role == "owner") {
            if (!state.is_owner(peer)) return refusal("not a current owner of this mesh");
        } else return refusal("invalid session role");
    }

    // May this session act on the paglet? Its owner and admins may.
    auto may_act = [&](const runtime::PagletId& paglet) -> std::expected<void, std::string> {
        auto info = impl_->runtime.info(paglet);
        if (!info) return std::unexpected("no paglet " + paglet + " here");
        if (!admin && info->owner != peer_id) return std::unexpected(std::string("not the paglet's owner"));
        return {};
    };

    if (*type == "moves") {
        const auto* clear = f.get("clear_cache");
        if (clear && (!clear->as_bool() || (*clear->as_bool() && !admin)))
            return refusal("only an admin may clear the page cache");
        std::lock_guard lock(impl_->mu);
        if (clear && *clear->as_bool()) {
            if (!impl_->outgoing.empty() || !impl_->incoming.empty()) return refusal("a move is in progress");
            impl_->pages.clear();
        }
        const auto& s = impl_->move_stats;
        const auto n = [](std::uint64_t x) { return mesh::Value(static_cast<std::int64_t>(x)); };
        return answer(mesh::Map{{"moves_out", n(s.moves_out)}, {"moves_in", n(s.moves_in)},
            {"moves_failed", n(s.moves_failed)}, {"pages_sent", n(s.pages_sent)},
            {"pages_received", n(s.pages_received)}, {"pages_reused", n(s.pages_reused)},
            {"bytes_sent", n(s.bytes_sent)}, {"image_pages", n(s.image_pages)},
            {"capture_ns", n(s.capture_ns)}, {"ready_ns", n(s.ready_ns)},
            {"prepare_ns", n(s.prepare_ns)}, {"commit_ns", n(s.commit_ns)},
            {"destination_activation_ns", n(s.destination_activation_ns)},
            {"arrival_acks", n(s.arrival_acks)}, {"arrival_failures", n(s.arrival_failures)},
            {"arrival_ack_ns", n(s.arrival_ack_ns)}, {"cached_page_bytes", n(impl_->pages.bytes())},
            {"memory_limit_pages", n(impl_->runtime.config().limits.max_memory_pages)}});
    }

    if (*type == "status") {
        // The paglets here (native system paglets are listed by name in
        // `system`) and facts about the host (WP32).
        mesh::Array paglets;
        for (const auto& p : impl_->runtime.list()) {
            if (p.module.empty()) continue;  // native system paglets
            paglets.emplace_back(mesh::Array{mesh::Value(p.id), mesh::Value(p.module), mesh::Value(p.owner),
                                             mesh::Value(runtime::to_string(p.state)),
                                             mesh::Value(runtime::to_string(p.trust))});
        }
        mesh::Array system;
        for (const auto& name : impl_->runtime.system_paglet_names()) {
            if (auto id = impl_->runtime.system_paglet(name)) system.emplace_back(*id);
        }
        const auto facts = platform::summary();
        auto n = [](std::uint64_t v) { return mesh::Value(static_cast<std::int64_t>(v)); };
        std::lock_guard lock(impl_->mu);
        const mesh::LedgerState& st = impl_->ledger.state();
        auto self = st.hosts.find(impl_->key.public_key());
        return answer(mesh::Map{{"key", mesh::Value::bin(impl_->key.public_key())},
                                {"name", mesh::Value(self == st.hosts.end() ? std::string() : self->second.name)},
                                {"mesh", mesh::Value(st.mesh_name)},
                                {"digest", mesh::Value::bin(impl_->ledger.digest())},
                                {"records", mesh::Value(static_cast<std::int64_t>(st.records))},
                                {"paglets", mesh::Value(std::move(paglets))},
                                {"system", mesh::Value(std::move(system))},
                                {"version", mesh::Value(PAGLETS_VERSION)},
                                {"uptime_s", mesh::Value((mesh::unix_ms() - impl_->started_ms) / 1000)},
                                {"system_uptime_s", n(facts.uptime_s)},
                                {"os", mesh::Value(facts.os)},
                                {"os_version", mesh::Value(facts.os_version)},
                                {"architecture", mesh::Value(facts.architecture)},
                                {"cpu_model", mesh::Value(facts.cpu_model)},
                                {"cpus", n(facts.cpu_count)},
                                {"memory_total", n(facts.memory_total)}});
    }

    if (*type == "hosts") {
        // The host registry (planning/cpp-mesh.md): admins and owners.
        mesh::Array list;
        for (const auto& h : hosts()) {
            mesh::Array labels;
            for (const auto& l : h.labels) labels.emplace_back(l);
            list.emplace_back(
                mesh::Map{{"key", mesh::Value::bin(h.key)},
                          {"name", mesh::Value(h.name)},
                          {"labels", mesh::Value(std::move(labels))},
                          {"url", mesh::Value(h.url)},
                          {"self", mesh::Value(h.self)},
                          {"online", mesh::Value(h.online)},
                          {"send_error", mesh::Value(h.send_error)},
                          {"compatible", mesh::Value(h.compatible)},
                          {"seen", mesh::Value(h.last_seen_ms)},
                          {"proto", mesh::Value(h.protocol)},
                          {"abi", mesh::Value(std::to_string(h.abi_major) + "." + std::to_string(h.abi_minor))},
                          {"via", mesh::Value(h.via)},
                          {"relays", mesh::Value([&] {
                               mesh::Array a;
                               for (const auto& k : h.relays) a.push_back(mesh::Value::bin(k));
                               return a;
                           }())}});
        }
        return answer(mesh::Map{{"hosts", mesh::Value(std::move(list))}});
    }

    if (*type == "landscape" || *type == "slots") {
        // mesh-info and compute-slots (planning/cpp-compute.md): admins and owners.
        if (*type == "landscape") {
            mesh::Array list;
            for (const auto& s : landscape()) list.emplace_back(wire::to_msgpack(s));
            return answer(mesh::Map{{"hosts", mesh::Value(std::move(list))}});
        }
        return answer(mesh::Map{{"status", mesh::Value(wire::to_msgpack(compute_status()))}});
    }

    if (*type == "jobs") {
        // Compute jobs on this host (planning/cpp-compute-jobs.md): owners
        // see and cancel their jobs, admins all.
        const std::string owner = admin ? std::string() : peer_id;
        auto jobs = impl_->runtime.jobs();
        std::erase_if(jobs, [&](const runtime::JobInfo& j) { return !owner.empty() && j.owner != owner; });
        if (f.str("op").value_or("list") == "cancel") {
            const auto id = f.integer("id");
            auto it = std::ranges::find_if(jobs, [&](const runtime::JobInfo& j) {
                return id && j.id == static_cast<std::uint64_t>(*id);
            });
            if (it == jobs.end()) return refusal("no such job of yours on this host");
            return answer(mesh::Map{{"cancelled", mesh::Value(impl_->runtime.cancel_job(it->id))}});
        }
        mesh::Array list;
        for (const auto& j : jobs) {
            list.emplace_back(mesh::Map{{"id", mesh::Value(static_cast<std::int64_t>(j.id))},
                                        {"paglet", mesh::Value(j.paglet)},
                                        {"requester", mesh::Value(j.requester_host)},
                                        {"owner", mesh::Value(j.owner)},
                                        {"module", mesh::Value(j.module)},
                                        {"function", mesh::Value(j.function)},
                                        {"state", mesh::Value(j.state)},
                                        {"cores", mesh::Value(j.cores)},
                                        {"done", mesh::Value(j.done)},
                                        {"total", mesh::Value(j.total)},
                                        {"queued_ms", mesh::Value(j.queued_ms)},
                                        {"elapsed_ms", mesh::Value(j.elapsed_ms)},
                                        {"aot", mesh::Value(j.aot)}});
        }
        return answer(mesh::Map{{"jobs", mesh::Value(std::move(list))},
                                {"slots", mesh::Value(static_cast<std::int64_t>(impl_->runtime.job_slots()))},
                                {"running", mesh::Value(impl_->runtime.job_cores_running())},
                                {"aot", mesh::Value(impl_->runtime.job_aot())}});
    }

    if (*type == "zombies") {
        // Zombie groups (docs/zombie-protocol.md): owners see and act on
        // their groups, admins on all.
        const std::string op = f.str("op").value_or("");
        const std::string group = f.str("g").value_or("");
        const std::string owner = admin ? std::string() : peer_id;
        if (op == "list") {
            mesh::Array list;
            for (const auto& g : zombie_groups(owner)) list.emplace_back(wire::to_msgpack(g));
            return answer(mesh::Map{{"groups", mesh::Value(std::move(list))}});
        }
        if (op == "cancel" || op == "redirect") {
            auto r = op == "cancel" ? zombie_cancel(group, owner, f.str("reason").value_or(""))
                                    : zombie_redirect(group, owner, f.str("sink").value_or(""));
            if (!r) return refusal(r.error());
            return answer(mesh::Map{{"state", mesh::Value(wire::to_msgpack(*r))}});
        }
        if (op == "result") {
            auto r = zombie_result(group);
            if (!r.found) return refusal("no result of group " + group + " on this host");
            bool mine = admin;
            for (const auto& g : zombie_groups(owner)) mine |= g.state.group == group;
            if (!mine) return refusal("not the group's owner");
            return answer(mesh::Map{{"result", mesh::Value(wire::to_msgpack(r))}});
        }
        if (op == "traces" || op == "check") {
            std::optional<services::zombies::GroupState> state;
            for (const auto& g : zombie_groups(owner)) {
                if (g.state.group == group) state = g.state;
            }
            if (!state) return refusal("no group " + group + " of yours on this host");
            auto records = zombie_traces(group);
            if (op == "traces") {
                mesh::Array list;
                for (const auto& r : records) list.emplace_back(wire::to_msgpack(r));
                return answer(mesh::Map{{"records", mesh::Value(std::move(list))},
                                        {"state", mesh::Value(wire::to_msgpack(*state))}});
            }
            const auto rep = protocols::zombie::check_traces(std::move(records), state->sink);
            mesh::Array violations;
            for (const auto& v : rep.violations) violations.emplace_back(v);
            auto n = [](std::size_t v) { return mesh::Value(static_cast<std::int64_t>(v)); };
            return answer(mesh::Map{{"records", n(rep.records)},
                                    {"replicas", n(rep.replicas)},
                                    {"hosts", n(rep.hosts)},
                                    {"takeovers", n(rep.takeovers)},
                                    {"restarts", n(rep.restarts)},
                                    {"accepted", n(rep.accepted)},
                                    {"violations", mesh::Value(std::move(violations))}});
        }
        return refusal("unknown zombies operation");
    }

    if (*type == "push") {
        // Records are signed; the ledger decides what they mean.
        const mesh::Array* records = f.array("records");
        if (records == nullptr) return refusal("malformed request");
        std::int64_t added = 0;
        mesh::Array errors;
        for (const auto& r : *records) {
            const mesh::Bytes* bytes = r.as_bin();
            if (bytes == nullptr) return refusal("malformed request");
            auto record = mesh::Record::decode(*bytes);
            if (!record) {
                errors.emplace_back(record.error());
                continue;
            }
            auto result = submit(*record);
            if (!result) {
                errors.emplace_back(result.error());
            } else if (*result != mesh::AddResult::known) {
                ++added;
            }
        }
        return answer(mesh::Map{{"added", mesh::Value(added)}, {"errors", mesh::Value(std::move(errors))}});
    }

    if (*type == "launch") {
        // An owner starts one of its paglets here from its passport.
        auto passport_bytes = f.bin("passport");
        if (!passport_bytes) return refusal("malformed request");
        auto passport = mesh::Passport::decode(*passport_bytes);
        if (!passport) return refusal("passport: " + passport.error());
        if (passport->root().owner != peer) return refusal("the passport belongs to another owner");
        if (auto module = f.bin("module")) {
            if (auto added = impl_->runtime.modules().add_verified(passport->module(), std::move(*module)); !added) {
                return refusal(added.error());
            }
        }
        const std::string hash = to_hex(passport->module());
        if (!impl_->runtime.modules().contains(hash)) return refusal("module " + hash.substr(0, 16) + " is not here");
        auto id = create(hash, *passport, f.bin("args").value_or(runtime::Bytes{}));
        if (!id) return refusal(id.error());
        return answer(mesh::Map{{"paglet", mesh::Value(*id)}});
    }

    if (*type == "call") {
        auto paglet = f.str("paglet");
        auto name = f.str("name");
        if (!paglet || !name) return refusal("malformed request");
        if (auto ok = may_act(*paglet); !ok) return refusal(ok.error());
        const auto timeout = std::chrono::milliseconds(f.integer("timeout_ms").value_or(30000));
        const runtime::Reply r =
            impl_->runtime.call(*paglet, *name, f.bin("payload").value_or(runtime::Bytes{}), timeout, peer_id);
        return answer(mesh::Map{{"status", mesh::Value(static_cast<std::int64_t>(r.status))},
                                {"payload", mesh::Value(r.payload)}});
    }

    if (*type == "send") {
        // One-way message (WP32); the paglet's errors do not come back.
        auto paglet = f.str("paglet");
        auto name = f.str("name");
        if (!paglet || !name) return refusal("malformed request");
        if (auto ok = may_act(*paglet); !ok) return refusal(ok.error());
        const auto priority = static_cast<std::int32_t>(std::clamp<std::int64_t>(f.integer("priority").value_or(3), 0, 7));
        if (auto ok = impl_->runtime.send(*paglet, *name, f.bin("payload").value_or(runtime::Bytes{}), priority, peer_id); !ok) {
            return refusal(std::string(abi::error_name(ok.error())));
        }
        return answer({});
    }

    if (*type == "describe") {
        // The interface description of a paglet's module, of a module, or of
        // a native system paglet's service (WP32, WP33): JSON text, empty
        // when there is none.
        if (auto module = f.str("module")) {
            if (!impl_->runtime.modules().contains(*module)) return refusal("module " + module->substr(0, 16) + " is not here");
            return answer(mesh::Map{{"interface", mesh::Value(module_interface(impl_->runtime.modules(), *module))}});
        }
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        auto info = impl_->runtime.info(*paglet);
        if (!info) return refusal("no paglet " + *paglet + " here");
        if (!info->module.empty()) {
            return answer(mesh::Map{{"interface", mesh::Value(module_interface(impl_->runtime.modules(), info->module))}});
        }
        // Native system paglets serve contracts, which describe themselves.
        const runtime::Reply r = impl_->runtime.call(*paglet, "describe", {}, std::chrono::milliseconds(5000));
        std::string text;
        if (r.status == abi::ok) {
            std::string schema;
            if (auto j = wire::msgpack_to_json(r.payload); j && j->is_string()) schema = j->as_string();
            if (auto parsed = wire::Json::parse(schema)) {
                std::string name = *paglet;
                if (name.starts_with("system.")) name = name.substr(7);
                text = wire::interface::from_schema(*parsed, name).dump();
            }
        }
        return answer(mesh::Map{{"interface", mesh::Value(text)}});
    }

    if (*type == "terminate") {
        // Admins end a paglet as failed (WP32), for example one that hangs.
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        if (!admin) return refusal("only admins terminate paglets");
        if (!impl_->runtime.info(*paglet)) return refusal("no paglet " + *paglet + " here");
        if (auto ok = impl_->runtime.terminate(*paglet, f.str("reason").value_or("terminated by an admin")); !ok) {
            return refusal(std::string(abi::error_name(ok.error())));
        }
        return answer({});
    }

    if (*type == "dispatch") {
        auto paglet = f.str("paglet");
        auto destination = f.str("destination");
        if (!paglet || !destination) return refusal("malformed request");
        if (auto ok = may_act(*paglet); !ok) return refusal(ok.error());
        // With `wait_ms`, the answer tells where the paglet went, or why it
        // stays; without, only that the move started.
        const auto wait = std::chrono::milliseconds(
            std::clamp<std::int64_t>(f.integer("wait_ms").value_or(0), 0, max_dispatch_wait_ms));
        if (wait.count() == 0) {
            if (auto ok = dispatch(*paglet, *destination); !ok) return refusal(ok.error());
            return answer({});
        }
        {
            std::lock_guard lock(impl_->move_waits_mu);
            auto& w = impl_->move_waits[*paglet];
            if (w.sessions++ == 0) w.outcome.reset();
        }
        auto outcome = [&]() -> std::expected<std::optional<Impl::MoveOutcome>, std::string> {
            if (auto ok = dispatch(*paglet, *destination); !ok) return std::unexpected(ok.error());
            std::unique_lock lock(impl_->move_waits_mu);
            auto& w = impl_->move_waits[*paglet];
            impl_->move_waits_cv.wait_for(lock, wait, [&] { return w.outcome.has_value(); });
            return w.outcome;
        }();
        {
            std::lock_guard lock(impl_->move_waits_mu);
            if (--impl_->move_waits[*paglet].sessions == 0) impl_->move_waits.erase(*paglet);
        }
        if (!outcome) return refusal(outcome.error());
        if (!*outcome) return answer(mesh::Map{{"pending", mesh::Value(true)}});
        mesh::Map m{{"moved", mesh::Value((*outcome)->host.has_value())}, {"reason", mesh::Value((*outcome)->reason)}};
        if (auto host = (*outcome)->host ? mesh::parse_key_id(*(*outcome)->host) : std::nullopt) {
            std::lock_guard lock(impl_->mu);
            const auto& hosts = impl_->ledger.state().hosts;
            auto h = hosts.find(*host);
            m.emplace_back("host", mesh::Value::bin(*host));
            m.emplace_back("host_name", mesh::Value(h == hosts.end() ? std::string() : h->second.name));
        }
        return answer(std::move(m));
    }

    // Location and pins (planning/cpp-location.md): admins, and owners for
    // their own paglets.
    auto locate = [&](const runtime::PagletId& paglet) {
        Impl::Lookup l;
        l.paglet = paglet;
        return run_lookup(*impl_, std::move(l));
    };
    auto location = [&](const Impl::LookupResult& r) {
        std::lock_guard lock(impl_->mu);
        const auto& hosts = impl_->ledger.state().hosts;
        auto h = hosts.find(r.where.host);
        return mesh::Map{{"host", mesh::Value::bin(r.where.host)},
                         {"host_name", mesh::Value(h == hosts.end() ? std::string() : h->second.name)},
                         {"moves", mesh::Value(static_cast<std::int64_t>(r.where.moves))},
                         {"moved", mesh::Value(r.where.moved)}};
    };

    if (*type == "locate" || *type == "pin") {
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        auto found = locate(*paglet);
        if (found.status != abi::ok) return refusal(found.error.empty() ? "not found" : found.error);
        if (!admin && found.where.owner != peer_id) return refusal("not the paglet's owner");
        if (*type == "locate") return answer(location(found));
        Impl::Lookup l;
        l.kind = Impl::LookupKind::pin;
        l.paglet = *paglet;
        l.pin = new_cap_id();
        l.until = mesh::unix_ms() + std::clamp<std::int64_t>(f.integer("duration_ms").value_or(600'000),
                                                             min_duration_ms, max_session_pin_ms);
        l.holder = std::string(role) + ":" + peer_id;
        l.reason = f.str("reason").value_or("");
        auto pinned = run_lookup(*impl_, std::move(l));
        if (pinned.status != abi::ok) return refusal(pinned.error.empty() ? "not pinned" : pinned.error);
        mesh::Map m = location(pinned);
        m.emplace_back("pin", mesh::Value(pinned.pin));
        m.emplace_back("until", mesh::Value(pinned.until));
        return answer(std::move(m));
    }

    if (*type == "pins") {
        mesh::Array list;
        for (const auto& p : pins()) {
            auto info = impl_->runtime.info(p.paglet);
            if (!admin && (!info || info->owner != peer_id)) continue;
            list.emplace_back(mesh::Array{mesh::Value(p.pin), mesh::Value(p.paglet), mesh::Value(p.until),
                                          mesh::Value(p.holder), mesh::Value(p.reason)});
        }
        return answer(mesh::Map{{"pins", mesh::Value(std::move(list))}});
    }

    if (*type == "unpin") {
        // Admins end pins wherever the paglet is.
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        if (!admin) return refusal("only admins end pins");
        Impl::Lookup l;
        l.kind = Impl::LookupKind::force;
        l.paglet = *paglet;
        l.pin = f.str("pin").value_or("");
        auto ended = run_lookup(*impl_, std::move(l));
        if (ended.status != abi::ok) return refusal(ended.error.empty() ? "not found" : ended.error);
        mesh::Map m = location(ended);
        m.emplace_back("released", mesh::Value(ended.released));
        return answer(std::move(m));
    }

    // Tooling (WP19): the ledger's records (an admin's copy catches up with
    // them), the modules here, a module to keep here, a paglet to end.
    if (*type == "records") {
        mesh::Array list;
        std::lock_guard lock(impl_->mu);
        for (const mesh::Record* r : impl_->ledger.records()) list.emplace_back(r->encode());
        return answer(mesh::Map{{"records", mesh::Value(std::move(list))}});
    }

    if (*type == "modules") {
        mesh::Array list;
        std::vector<std::pair<std::string, std::string>> names;  // hash, "name version"
        {
            std::lock_guard lock(impl_->mu);
            for (const auto& s : impl_->ledger.state().module_signatures) {
                names.emplace_back(to_hex(s.module), s.name + (s.version.empty() ? "" : " " + s.version));
            }
        }
        for (const auto& e : impl_->runtime.modules().list()) {
            mesh::Array signed_as;
            for (const auto& [hash, name] : names) {
                if (hash == e.hash) signed_as.emplace_back(name);
            }
            list.emplace_back(mesh::Map{{"hash", mesh::Value(e.hash)},
                                        {"size", mesh::Value(static_cast<std::int64_t>(e.size))},
                                        {"users", mesh::Value(static_cast<std::int64_t>(e.users))},
                                        {"pinned", mesh::Value(e.pinned)},
                                        {"names", mesh::Value(std::move(signed_as))},
                                        {"interface", mesh::Value(module_interface(impl_->runtime.modules(), e.hash))}});
        }
        return answer(mesh::Map{{"modules", mesh::Value(std::move(list))}});
    }

    if (*type == "push-module") {
        auto bytes = f.bin("module");
        if (!bytes) return refusal("malformed request");
        auto hash = impl_->runtime.modules().add(std::move(*bytes));
        if (!hash) return refusal(hash.error());
        return answer(mesh::Map{{"hash", mesh::Value(*hash)}});
    }

    if (*type == "dispose") {
        auto paglet = f.str("paglet");
        if (!paglet) return refusal("malformed request");
        if (auto ok = may_act(*paglet); !ok) return refusal(ok.error());
        if (auto ok = impl_->runtime.dispose(*paglet); !ok) return refusal(std::string(abi::error_name(ok.error())));
        return answer({});
    }

    return refusal("unknown request " + *type);
}

}  // namespace paglets::node
