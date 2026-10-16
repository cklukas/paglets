// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Moving paglets between hosts (planning/cpp-networking.md, section 6):
// transfer tickets, signed move offers with page manifests, page transfer
// with deduplication and zstd, two-phase commit, capability re-creation from
// grants, passports for clones and children, messages across hosts.

#include "node_impl.hpp"

#include <paglets/runtime/mobility.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <zstd.h>

#include <charconv>
#include <random>

namespace paglets::node {

namespace {

constexpr std::string_view offer_domain = "paglets move offer v1";
constexpr std::size_t pages_per_frame_bytes = 1024 * 1024;  // compressed bytes per move-pages frame
constexpr int zstd_level = 3;
constexpr auto decision_ttl = std::chrono::hours(1);
constexpr std::size_t max_move_diagnostic_bytes = 8 * 1024;

std::string move_diagnostic(std::string_view summary, const std::vector<std::string>& reasons) {
    // Keep complete fragments rather than cutting a UTF-8 host name or
    // reason in the middle; reserve space for the omitted-reason count.
    constexpr std::size_t budget = max_move_diagnostic_bytes - 64;
    std::string out = summary.size() <= budget ? std::string(summary)
                                              : "the transfer failed (oversized summary omitted)";
    std::size_t omitted = 0;
    for (const auto& reason : reasons) {
        if (out.size() + 2 <= budget && reason.size() <= budget - out.size() - 2) out += "; " + reason;
        else ++omitted;
    }
    if (omitted != 0) out += "; " + std::to_string(omitted) + " more host reason(s) omitted";
    return out;
}

std::uint64_t elapsed_ns(std::chrono::steady_clock::time_point start) {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start).count());
}


mesh::Value image_manifest(const wasm::Snapshot& image) {
    mesh::Array globals;
    for (const auto& g : image.globals) {
        globals.emplace_back(mesh::Array{mesh::Value(g.name), mesh::Value(static_cast<std::int64_t>(g.type)),
                                         mesh::Value(static_cast<std::int64_t>(g.bits))});
    }
    mesh::Array pages;
    for (const auto& p : image.pages) pages.push_back(p.zero ? mesh::Value() : mesh::Value::bin(p.hash));
    return mesh::Value(mesh::Map{{"page_count", mesh::Value(static_cast<std::int64_t>(image.page_count))},
                                 {"globals", mesh::Value(std::move(globals))},
                                 {"pages", mesh::Value(std::move(pages))}});
}

// The image without its data, from a manifest.
std::optional<wasm::Snapshot> parse_image_manifest(const mesh::Value& v, const paglets::Digest& module) {
    const mesh::Map* m = v.as_map();
    if (m == nullptr) return std::nullopt;
    const mesh::Fields f{*m};
    auto count = f.integer("page_count");
    const mesh::Array* globals = f.array("globals");
    const mesh::Array* pages = f.array("pages");
    if (!count || *count < 0 || *count > 65536 || globals == nullptr || pages == nullptr ||
        pages->size() != static_cast<std::size_t>(*count)) {
        return std::nullopt;
    }
    wasm::Snapshot s;
    s.module_hash = module;
    s.page_count = static_cast<std::uint32_t>(*count);
    for (const auto& g : *globals) {
        const mesh::Array* a = g.as_array();
        if (a == nullptr || a->size() != 3 || (*a)[0].as_str() == nullptr || !(*a)[1].as_int() || !(*a)[2].as_int()) {
            return std::nullopt;
        }
        s.globals.push_back(wasm::GlobalValue{*(*a)[0].as_str(), static_cast<std::uint8_t>(*(*a)[1].as_int()),
                                              static_cast<std::uint64_t>(*(*a)[2].as_int())});
    }
    for (const auto& p : *pages) {
        wasm::PageEntry e;
        if (p.is_nil()) {
            e.zero = true;
        } else {
            const mesh::Bytes* b = p.as_bin();
            if (b == nullptr || b->size() != 32) return std::nullopt;
            e.zero = false;
            std::copy(b->begin(), b->end(), e.hash.begin());
        }
        s.pages.push_back(e);
    }
    return s;
}

std::vector<paglets::Digest> data_hashes(const wasm::Snapshot& s) {
    std::vector<paglets::Digest> out;
    for (const auto& p : s.pages) {
        if (!p.zero) out.push_back(p.hash);
    }
    return out;
}

mesh::Bytes offer_message(std::span<const std::uint8_t> body) {
    mesh::Bytes m(offer_domain.begin(), offer_domain.end());
    m.push_back(0);
    const auto digest = paglets::sha256(body);
    m.insert(m.end(), digest.begin(), digest.end());
    return m;
}

// Does `inner` equal `outer` or name a subdirectory (dir resources use root/path)?
bool within(std::string_view outer, std::string_view inner) {
    if (outer.empty()) return false;
    if (inner == outer) return true;
    if (!inner.starts_with(outer)) return false;
    return inner[outer.size()] == '/';
}

}  // namespace

// -- hooks -------------------------------------------------------------------------

void Node::Impl::install_mobility() {
    runtime::MobilityHooks hooks;
    hooks.host_id = mesh::key_id(key.public_key());
    // Called with the runtime's lock held: only the transport is used.
    hooks.deliver = [this](runtime::RemoteMessage m) {
        auto to = mesh::parse_key_id(m.host);
        if (!to || *to == key.public_key()) return;
        transport.send(*to, mesh::encode(mesh::Value(mesh::Map{{"t", mesh::Value("deliver")},
                                                               {"m", mesh::Value(runtime::encode_remote(m))}})));
    };
    hooks.depart = [this](runtime::Departure d) { on_depart(std::move(d)); };
    hooks.arrived = [this](const std::string& from, std::uint64_t request, bool succeeded, std::uint64_t activation_ns) {
        if (auto source = mesh::parse_key_id(from)) {
            transport.send(*source, mesh::encode(mesh::Value(mesh::Map{
                {"t", mesh::Value("move-arrived")}, {"r", mesh::Value(static_cast<std::int64_t>(request))},
                {"a", mesh::Value(succeeded)}, {"activation_ns", mesh::Value(static_cast<std::int64_t>(activation_ns))}})));
        }
    };
    // Called with the runtime's lock held: queued for location.cpp.
    hooks.unresolved = [this](runtime::RemoteMessage m) {
        std::lock_guard lock(unresolved_mu);
        unresolved.push_back(std::move(m));
    };
    // Called with the runtime's lock held: only sessions waiting for the
    // move hear of it.
    hooks.moved = [this](const runtime::PagletId& paglet, const std::optional<std::string>& host,
                         const std::string& reason) {
        std::lock_guard lock(move_waits_mu);
        auto it = move_waits.find(paglet);
        if (it == move_waits.end() || it->second.outcome) return;
        it->second.outcome = MoveOutcome{host, reason};
        move_waits_cv.notify_all();
    };
    runtime.set_mobility(std::move(hooks));
}

void Node::Impl::on_depart(runtime::Departure d) {
    std::lock_guard lock(mu);
    start_move(std::move(d));
}

void Node::Impl::on_deliver(const mesh::PublicKey& from, const mesh::Fields& f) {
    if (!ledger.state().is_host(from)) return;  // messages come from enrolled hosts only
    auto bytes = f.bin("m");
    if (!bytes) return;
    auto m = runtime::decode_remote(*bytes);
    if (!m) return;
    runtime.deliver_remote(std::move(*m));
}

// Passports for children and clones made on this host: links signed by the
// host, extending their creator's passport.
void Node::Impl::take_spawns() {
    const std::int64_t now = mesh::unix_ms();
    for (const auto& s : runtime.take_spawns()) {
        if (!held.contains(s.child) && runtime.info(s.child)) note_created(s.child);
        auto parent = passports.find(s.parent);
        auto module = mesh::parse_key_id(s.module);
        if (parent == passports.end() || !module || passports.contains(s.child)) continue;
        auto link = parent->second.extend(key, s.kind, s.child, *module, now);
        if (!link) {
            warn("passport link for " + s.child.substr(0, 8) + ": " + link.error());
            continue;
        }
        save_passport(*link);
        passports.emplace(s.child, std::move(*link));
    }
}

// -- the source ------------------------------------------------------------------------

std::vector<mesh::PublicKey> Node::Impl::candidates(const Ticket& ticket, const mesh::Principal& principal,
                                                    const std::optional<std::vector<mesh::Item>>& manifest,
                                                    const std::vector<std::string>& marks,
                                                    std::vector<std::string>& why_not) {
    const mesh::LedgerState& st = ledger.state();
    std::vector<mesh::PublicKey> matches;
    const std::string& t = ticket.target;
    std::optional<mesh::PublicKey> addressed;
    if (t != "any" && !t.starts_with("offer:") && !t.starts_with("label:")) {
        const auto host = host_detail::resolve_host_address(st, t, true);
        if (!host) {
            if (host.error() == host_detail::HostAddressError::ambiguous)
                why_not.push_back("ambiguous host address " + t + "; use a full key ID");
            return {};
        }
        addressed = *host;
    }
    // `offer:<service>[.<op>]`: hosts offering it (planning/cpp-offers.md),
    // the least loaded first.
    std::vector<std::string> offering;
    if (t.starts_with("offer:")) {
        services::mesh_info::OffersRequest q;
        const std::string what = t.substr(6);
        const auto dot = what.find('.');
        q.service = what.substr(0, dot);
        if (dot != std::string::npos) q.op = what.substr(dot + 1);
        q.include_self = false;
        q.limit = 256;
        for (const auto& m : find_offers(q)) {
            if (std::ranges::find(offering, m.host) == offering.end()) offering.push_back(m.host);
        }
    }
    for (const auto& [k, h] : st.hosts) {
        if (k == key.public_key()) continue;
        bool match = false;
        if (t == "any") {
            match = true;
        } else if (t.starts_with("offer:")) {
            match = std::ranges::find(offering, mesh::key_id(k)) != offering.end();
        } else if (t.starts_with("label:")) {
            match = std::ranges::find(h.labels, t.substr(6)) != h.labels.end();
        } else {
            match = addressed == k;
        }
        if (!match) continue;
        // Hosts that could not run the paglet: another protocol or ABI, or
        // an older ABI minor version (the paglet may use newer imports).
        if (auto r = registry.find(k);
            !compatible(k) || (r != registry.end() && r->second.abi_minor < abi::minor_version)) {
            why_not.push_back("host " + h.name + ": runs an incompatible paglet ABI");
            continue;
        }
        matches.push_back(k);
    }
    std::vector<mesh::PublicKey> out;
    const std::int64_t now = mesh::unix_ms();
    for (const auto& k : matches) {
        bool ok = true;
        if (manifest && !manifest->empty()) {
            const auto evaluations = mesh::preflight(st, principal, *manifest, k);
            for (std::size_t i = 0; i < evaluations.size() && ok; ++i) {
                if (evaluations[i].decision != mesh::Decision::deny) continue;
                // A grant the admins gave for that host satisfies the item too.
                const bool granted = std::ranges::any_of(st.grants, [&](const auto& e) {
                    const mesh::Grant& g = e.second;
                    return g.principal == principal && g.item == (*manifest)[i] && g.expires > now &&
                           mesh::selects(st, g.hosts, k);
                });
                if (!granted) {
                    ok = false;
                    why_not.push_back("host " + st.hosts.at(k).name + ": the manifest's " + (*manifest)[i].service +
                                      " access is denied there");
                }
            }
        }
        // Data residency: the roots the paglet read whose content may go
        // only to some hosts.
        for (const auto& mark : marks) {
            if (!ok || !mark.starts_with("root:")) continue;
            auto rule = st.residency.find(mark.substr(5));
            if (rule == st.residency.end() || mesh::selects(st, rule->second.hosts, k)) continue;
            ok = false;
            why_not.push_back("host " + st.hosts.at(k).name + ": the content of root " + rule->first +
                              " may not go there");
        }
        if (ok) out.push_back(k);
    }
    // Several hosts match `any` and labels: spread paglets over them;
    // offers keep their order.
    if (t == "any" || t.starts_with("label:")) {
        static thread_local std::mt19937_64 random{std::random_device{}()};
        std::ranges::shuffle(out, random);
    } else if (t.starts_with("offer:")) {
        auto rank = [&](const mesh::PublicKey& k) { return std::ranges::find(offering, mesh::key_id(k)) - offering.begin(); };
        std::ranges::stable_sort(out, [&](const auto& a, const auto& b) { return rank(a) < rank(b); });
    }
    // Hosts that are up first (location.cpp keeps the view).
    update_view();
    std::ranges::stable_partition(out, [&](const mesh::PublicKey& k) { return std::ranges::binary_search(view, k); });
    const auto limit = static_cast<std::size_t>(ticket.retries);
    if (out.size() > limit) {
        for (std::size_t i = limit; i < out.size(); ++i) {
            why_not.push_back("host " + st.hosts.at(out[i]).name + ": not tried (retry limit " +
                              std::to_string(ticket.retries) + ")");
        }
        out.resize(limit);
    }
    return out;
}

void Node::Impl::fail_move(Outgoing& o, const std::string& why) {
    ++move_stats.moves_failed;
    runtime.finish_departure(o.departure.move, std::nullopt, why);
}

void Node::Impl::start_move(runtime::Departure d) {
    take_spawns();
    Outgoing o;
    o.started = Clock::now();
    move_stats.capture_ns += d.capture_ns;
    move_stats.image_pages += d.image.page_count;
    auto fail = [&](const std::string& why) {
        o.departure = std::move(d);
        fail_move(o, why);
    };
    auto ticket = runtime::parse_transfer_ticket(d.destination);
    if (!ticket) return fail(ticket.error());
    // The passport travels: the paglet's own, or for a clone a link signed
    // by this host.
    auto module = mesh::parse_key_id(d.state.module);
    if (!module) return fail("invalid module hash");
    if (d.clone) {
        auto parent = passports.find(d.original);
        if (parent == passports.end()) return fail("no passport for the original");
        auto link = parent->second.extend(key, "clone", d.state.id, *module, mesh::unix_ms());
        if (!link) return fail("passport: " + link.error());
        o.passport = std::move(*link);
    } else {
        auto own = passports.find(d.state.id);
        if (own == passports.end()) return fail("no passport: only paglets of enrolled owners move");
        o.passport = own->second;
    }
    std::optional<std::vector<mesh::Item>> manifest;
    if (!o.passport->root().manifest.is_nil()) manifest = mesh::parse_manifest(o.passport->root().manifest);
    const mesh::Principal principal{d.state.id, d.state.owner, d.state.module, "roaming"};
    std::vector<std::string> why_not;
    // --8<-- [start:residency]
    // Data residency (planning/cpp-residency.md): a paglet that read a root
    // whose content stays on this host does not leave.
    for (const auto& mark : d.state.marks) {
        if (!mark.starts_with("root:")) continue;
        auto rule = ledger.state().residency.find(mark.substr(5));
        if (rule != ledger.state().residency.end() && rule->second.host_only)
            return fail("data residency: the content of root " + rule->first + " stays on this host");
    }
    // --8<-- [end:residency]
    auto hosts = candidates(*ticket, principal, manifest, d.state.marks, why_not);
    if (hosts.empty()) {
        const std::string why = why_not.empty() ? "no enrolled host matches " + ticket->target
                                              : "every host matching " + ticket->target + " was excluded";
        return fail(move_diagnostic(why, why_not));
    }
    o.failures = std::move(why_not);
    o.candidates.assign(hosts.begin(), hosts.end());
    o.ticket = *ticket;
    pages.add_image(d.image);  // for the way back
    o.departure = std::move(d);
    const std::uint64_t request = next_request++;
    outgoing.emplace(request, std::move(o));
    offer_next(request);
}

void Node::Impl::offer_next(std::uint64_t request) {
    auto it = outgoing.find(request);
    if (it == outgoing.end()) return;
    Outgoing& o = it->second;
    if (o.candidates.empty()) {
        fail_move(o, move_diagnostic("no host took the paglet", o.failures));
        outgoing.erase(it);
        return;
    }
    // A new request ID per offer: answers to earlier offers do not match.
    auto node = outgoing.extract(it);
    const std::uint64_t r = next_request++;
    node.key() = r;
    auto& out = outgoing.insert(std::move(node)).position->second;
    out.target = out.candidates.front();
    out.candidates.pop_front();
    ++out.attempts;
    const runtime::Departure& d = out.departure;
    out.moves = d.clone ? 1 : held[d.state.id].moves + 1;  // the move counter after this move
    mesh::Map body{{"v", mesh::Value(std::int64_t{1})},
                   {"mesh", mesh::Value::bin(ledger.mesh())},
                   {"from", mesh::Value::bin(key.public_key())},
                   {"to", mesh::Value::bin(out.target)},
                   {"request", mesh::Value(static_cast<std::int64_t>(r))},
                   {"kind", mesh::Value(d.clone ? "clone" : "dispatch")},
                   {"paglet", mesh::Value(d.state.id)},
                   {"passport", mesh::Value(out.passport->encode())},
                   {"state", mesh::Value(runtime::encode_state(d.state))},
                   {"image", image_manifest(d.image)},
                   {"activate", mesh::Value(out.ticket.activate)},
                   {"moves", mesh::Value(static_cast<std::int64_t>(out.moves))},
                   {"time", mesh::Value(mesh::unix_ms())}};
    if (d.clone) {
        body.emplace_back("clone_args", mesh::Value(d.clone_args));
        body.emplace_back("clone_caps", mesh::Value(runtime::encode_caps(d.clone_caps)));
        body.emplace_back("original", mesh::Value(d.original));
    }
    const mesh::Bytes bytes = mesh::encode(mesh::Value(std::move(body)));
    const mesh::Signature sig = key.sign(offer_message(bytes));
    auto frame = mesh::encode(mesh::Value(mesh::Map{{"t", mesh::Value("move-offer")},
                                     {"r", mesh::Value(static_cast<std::int64_t>(r))},
                                     {"b", mesh::Value(bytes)},
                                     {"s", mesh::Value::bin(sig)}}));
    if (frame.size() > transport.frame_limit()) {
        fail_move(out, "move offer exceeds the transport frame limit");
        outgoing.erase(r);
        return;
    }
    out.deadline = Clock::now() + move_timeout;
    transport.send(out.target, std::move(frame));
}

void Node::Impl::on_move_want(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    const mesh::Array* wanted = f.array("p");
    if (!r || wanted == nullptr) return;
    auto it = outgoing.find(static_cast<std::uint64_t>(*r));
    if (it == outgoing.end() || it->second.target != from) return;
    Outgoing& o = it->second;
    const wasm::Snapshot& image = o.departure.image;
    const std::size_t count = image.data_pages();
    mesh::Array parts;
    std::size_t part_bytes = 0;
    auto flush = [&] {
        if (parts.empty()) return;
        send_frame(
            from,
            mesh::Map{{"t", mesh::Value("move-pages")}, {"r", mesh::Value(*r)}, {"p", mesh::Value(std::move(parts))}});
        parts.clear();
        part_bytes = 0;
    };
    std::vector<std::uint8_t> buffer(ZSTD_compressBound(wasm::page_size));
    for (const auto& w : *wanted) {
        auto index = w.as_int();
        if (!index || *index < 0 || static_cast<std::size_t>(*index) >= count) continue;
        const std::uint8_t* page = image.data.data() + static_cast<std::size_t>(*index) * wasm::page_size;
        const std::size_t n = ZSTD_compress(buffer.data(), buffer.size(), page, wasm::page_size, zstd_level);
        if (ZSTD_isError(n)) continue;
        // A single compressed 64 KB page is below the frame budget. Flush
        // before adding the next page, rather than exceeding it by one page.
        if (!parts.empty() && part_bytes + n > pages_per_frame_bytes) flush();
        parts.emplace_back(
            mesh::Array{mesh::Value(*index),
                        mesh::Value(mesh::Bytes(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(n)))});
        part_bytes += n;
        ++move_stats.pages_sent;
        move_stats.bytes_sent += n;
    }
    flush();
    o.deadline = Clock::now() + move_timeout;
}

void Node::Impl::on_move_ready(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    auto it = outgoing.find(static_cast<std::uint64_t>(*r));
    if (it == outgoing.end() || it->second.target != from || it->second.recording) return;
    move_stats.ready_ns += elapsed_ns(it->second.started);
    // The move commits once a majority of the responsible hosts recorded
    // where the paglet goes (location.cpp).
    record_move(it->first);
}

void Node::Impl::on_move_arrived(const mesh::PublicKey& from, const mesh::Fields& f) {
    const auto request = f.integer("r");
    const auto* succeeded = f.get("a");
    if (!request || !succeeded || !succeeded->as_bool()) return;
    auto it = decisions.find(static_cast<std::uint64_t>(*request));
    if (it == decisions.end() || !it->second.committed || it->second.to != from ||
        it->second.arrival_reported || it->second.capture_started == Clock::time_point{}) return;
    it->second.arrival_reported = true;
    ++move_stats.arrival_acks;
    if (const auto ns = f.integer("activation_ns"); ns && *ns >= 0)
        move_stats.destination_activation_ns += static_cast<std::uint64_t>(*ns);
    if (!*succeeded->as_bool()) ++move_stats.arrival_failures;
    move_stats.arrival_ack_ns += elapsed_ns(it->second.capture_started);
}

void Node::Impl::commit_move(std::uint64_t request) {
    auto it = outgoing.find(request);
    if (it == outgoing.end()) return;
    Outgoing o = std::move(it->second);
    outgoing.erase(it);
    const mesh::PublicKey from = o.target;
    const auto r = static_cast<std::int64_t>(request);
    // The decision is recorded (durably) before the destination hears it.
    decisions[request] = Decision{true, from, Clock::now() + decision_ttl, o.moves};
    decisions[request].capture_started = o.departure.capture_started == Clock::time_point{}
        ? o.started : o.departure.capture_started;
    save();
    runtime.finish_departure(o.departure.move, mesh::key_id(from));
    loc_cache[o.departure.state.id] = {LocRecord{from, o.moves, mesh::unix_ms(), o.departure.state.owner, 0},
                                       Clock::now()};
    if (!o.departure.clone) {
        held.erase(o.departure.state.id);
        // Its passport lives on where it went.
        passports.erase(o.departure.state.id);
        if (!state_dir.empty()) {
            std::error_code ec;
            fs::remove(state_dir / "passports" / (o.departure.state.id + ".passport"), ec);
        }
    }
    ++move_stats.moves_out;
    send_frame(from, mesh::Map{{"t", mesh::Value("move-commit")},
                               {"r", mesh::Value(r)},
                               {"c", mesh::Value(static_cast<std::int64_t>(o.moves))}});
}

void Node::Impl::on_move_refused(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    auto it = outgoing.find(static_cast<std::uint64_t>(*r));
    if (it == outgoing.end() || it->second.target != from) return;
    const auto host = ledger.state().hosts.find(from);
    it->second.failures.push_back(
        "host " + (host == ledger.state().hosts.end() ? mesh::key_id(from).substr(0, 16) : host->second.name) + ": " +
        f.str("e").value_or("refused"));
    decisions[static_cast<std::uint64_t>(*r)] = Decision{false, from, Clock::now() + decision_ttl};
    offer_next(static_cast<std::uint64_t>(*r));
}

void Node::Impl::on_move_query(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    const auto request = static_cast<std::uint64_t>(*r);
    if (auto o = outgoing.find(request); o != outgoing.end() && o->second.target == from) return;  // undecided
    auto d = decisions.find(request);
    const bool committed = d != decisions.end() && d->second.committed && d->second.to == from;
    mesh::Map answer{{"t", mesh::Value(committed ? "move-commit" : "move-abort")}, {"r", mesh::Value(*r)}};
    if (committed) answer.emplace_back("c", mesh::Value(static_cast<std::int64_t>(d->second.moves)));
    send_frame(from, std::move(answer));
}

// -- the destination ------------------------------------------------------------------

void Node::Impl::refuse_arrival(const mesh::PublicKey& from, std::uint64_t request, const std::string& why) {
    auto it = incoming.find({from, request});
    if (it != incoming.end()) {
        if (it->second.phase == Incoming::Phase::prepared) runtime.abort_arrival(it->second.state.id);
        incoming.erase(it);
    }
    send_frame(from, mesh::Map{{"t", mesh::Value("move-refused")},
                               {"r", mesh::Value(static_cast<std::int64_t>(request))},
                               {"e", mesh::Value(why)}});
}

void Node::Impl::on_move_offer(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    auto body_bytes = f.bin("b");
    auto sig = f.fixed<64>("s");
    if (!r || *r <= 0 || !body_bytes || !sig) return;
    const auto request = static_cast<std::uint64_t>(*r);
    auto refuse = [&](const std::string& why) { refuse_arrival(from, request, why); };
    const mesh::LedgerState& st = ledger.state();
    if (!st.is_host(from)) return refuse("offers come from enrolled hosts only");
    if (incoming.contains({from, request})) return;  // a repeated offer
    // The envelope is signed by the source host.
    if (!mesh::verify(from, offer_message(*body_bytes), *sig)) return refuse("offer signature invalid");
    auto v = mesh::decode(*body_bytes);
    if (!v || v->as_map() == nullptr) return refuse("malformed offer");
    const mesh::Fields b{*v->as_map()};
    auto version = b.integer("v");
    auto mesh_id = b.fixed<32>("mesh");
    auto body_from = b.fixed<32>("from");
    auto to = b.fixed<32>("to");
    auto body_request = b.integer("request");
    auto kind = b.str("kind");
    auto paglet = b.str("paglet");
    auto passport_bytes = b.bin("passport");
    auto state_bytes = b.bin("state");
    const mesh::Value* manifest = b.get("image");
    const bool* activate_flag = b.get("activate") != nullptr ? b.get("activate")->as_bool() : nullptr;
    const bool activate = activate_flag == nullptr || *activate_flag;
    if (!version || *version != 1 || !mesh_id || !body_from || !to || !body_request || !kind || !paglet ||
        !passport_bytes || !state_bytes || manifest == nullptr) {
        return refuse("malformed offer");
    }
    // --8<-- [start:accept]
    if (*mesh_id != ledger.mesh() || *body_from != from || *to != key.public_key() ||
        static_cast<std::uint64_t>(*body_request) != request || (*kind != "dispatch" && *kind != "clone")) {
        return refuse("offer not for this host");
    }
    auto state = runtime::decode_state(*state_bytes);
    if (!state) return refuse(state.error());
    auto module = mesh::parse_key_id(state->module);
    if (!module || state->id != *paglet) return refuse("offer and state disagree");
    auto passport = mesh::Passport::decode(*passport_bytes);
    if (!passport) return refuse("passport: " + passport.error());
    if (auto ok = mesh::verify_passport(*passport, st, *paglet, *module, mesh::unix_ms()); !ok) {
        return refuse("passport: " + ok.error());
    }
    if (auto verdict = mesh::check_module(st, *module, "roaming"); !verdict.allowed) return refuse(verdict.reason);
    if (runtime.info(*paglet)) return refuse("paglet " + paglet->substr(0, 8) + " is here already");
    auto image = parse_image_manifest(*manifest, *module);
    if (!image) return refuse("malformed page manifest");
    // --8<-- [end:accept]

    Incoming in;
    in.state = std::move(*state);
    in.data_hashes = data_hashes(*image);
    in.image = std::move(*image);
    in.image.data.assign(in.data_hashes.size() * wasm::page_size, 0);
    in.have.assign(in.data_hashes.size(), false);
    in.passport = std::move(*passport);
    in.clone = *kind == "clone";
    in.activate = activate;
    in.moves = static_cast<std::uint64_t>(std::max<std::int64_t>(0, b.integer("moves").value_or(0)));
    if (in.clone) {
        in.clone_args = b.bin("clone_args").value_or(runtime::Bytes{});
        auto caps = runtime::decode_caps(b.bin("clone_caps").value_or(runtime::Bytes{0x90}));
        if (!caps) return refuse(caps.error());
        in.clone_caps = std::move(*caps);
        in.original = b.str("original").value_or("");
    }
    in.deadline = Clock::now() + move_timeout;
    incoming.emplace(std::pair{from, request}, std::move(in));
    need_module(*module, from, [this, from, request](const std::expected<std::string, std::string>& hash) {
        auto it = incoming.find({from, request});
        if (it == incoming.end()) return;
        if (!hash) return refuse_arrival(from, request, hash.error());
        Incoming& inc = it->second;
        // Pages this host has seen are not transferred again.
        mesh::Array wanted;
        for (std::size_t i = 0; i < inc.data_hashes.size(); ++i) {
            if (const auto* page = pages.find(inc.data_hashes[i])) {
                std::copy(page->begin(), page->end(),
                          inc.image.data.begin() + static_cast<std::ptrdiff_t>(i * wasm::page_size));
                inc.have[i] = true;
                ++move_stats.pages_reused;
            } else {
                wanted.emplace_back(static_cast<std::int64_t>(i));
            }
        }
        inc.missing = wanted.size();
        if (inc.missing == 0) return arrival_pages_complete(from, request);
        inc.phase = Incoming::Phase::pages;
        inc.deadline = Clock::now() + move_timeout;
        send_frame(from, mesh::Map{{"t", mesh::Value("move-want")},
                                   {"r", mesh::Value(static_cast<std::int64_t>(request))},
                                   {"p", mesh::Value(std::move(wanted))}});
    });
}

void Node::Impl::on_move_pages(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    const mesh::Array* parts = f.array("p");
    if (!r || parts == nullptr) return;
    const auto request = static_cast<std::uint64_t>(*r);
    auto it = incoming.find({from, request});
    if (it == incoming.end() || it->second.phase != Incoming::Phase::pages) return;
    Incoming& in = it->second;
    for (const auto& part : *parts) {
        const mesh::Array* a = part.as_array();
        const std::int64_t* index = a != nullptr && a->size() == 2 ? (*a)[0].as_int() : nullptr;
        const mesh::Bytes* data = a != nullptr && a->size() == 2 ? (*a)[1].as_bin() : nullptr;
        if (!index || data == nullptr || *index < 0 || static_cast<std::size_t>(*index) >= in.have.size()) {
            return refuse_arrival(from, request, "malformed pages");
        }
        const auto i = static_cast<std::size_t>(*index);
        if (in.have[i]) continue;
        if (ZSTD_getFrameContentSize(data->data(), data->size()) != wasm::page_size) {
            return refuse_arrival(from, request, "malformed page");
        }
        std::uint8_t* page = in.image.data.data() + i * wasm::page_size;
        const std::size_t n = ZSTD_decompress(page, wasm::page_size, data->data(), data->size());
        if (ZSTD_isError(n) || n != wasm::page_size) return refuse_arrival(from, request, "malformed page");
        // Every page must be the one the signed manifest names.
        if (paglets::sha256(std::span<const std::uint8_t>(page, wasm::page_size)) != in.data_hashes[i]) {
            return refuse_arrival(from, request, "damaged page");
        }
        in.have[i] = true;
        --in.missing;
        ++move_stats.pages_received;
    }
    in.deadline = Clock::now() + move_timeout;
    if (in.missing == 0) arrival_pages_complete(from, request);
}

void Node::Impl::arrival_pages_complete(const mesh::PublicKey& from, std::uint64_t request) {
    auto it = incoming.find({from, request});
    if (it == incoming.end()) return;
    Incoming& in = it->second;
    pages.add_image(in.image);
    runtime::Arrival a;
    a.state = in.state;
    a.image = std::move(in.image);
    a.from = mesh::key_id(from);
    a.clone = in.clone;
    a.clone_args = std::move(in.clone_args);
    a.clone_caps = std::move(in.clone_caps);
    a.original = in.original;
    a.request = request;
    const mesh::Principal principal{in.state.id, in.state.owner, in.state.module, in.state.trust};
    const auto preparing = Clock::now();
    auto lost = runtime.prepare_arrival(std::move(a), [this, principal](const runtime::Cap& c) { return recreate(principal, c); });
    move_stats.prepare_ns += elapsed_ns(preparing);
    if (!lost) return refuse_arrival(from, request, lost.error());
    in.phase = Incoming::Phase::prepared;
    in.deadline = Clock::now() + move_timeout;
    send_frame(from,
               mesh::Map{{"t", mesh::Value("move-ready")}, {"r", mesh::Value(static_cast<std::int64_t>(request))}});
}

void Node::Impl::on_move_commit(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    auto it = incoming.find({from, static_cast<std::uint64_t>(*r)});
    if (it == incoming.end() || it->second.phase != Incoming::Phase::prepared) return;
    Incoming in = std::move(it->second);
    incoming.erase(it);
    const auto committing = Clock::now();
    auto committed = runtime.commit_arrival(in.state.id, in.activate);
    move_stats.commit_ns += elapsed_ns(committing);
    if (!committed) {
        warn("committing the arrival of " + in.state.id.substr(0, 8) + ": " + committed.error());
        return;
    }
    save_passport(*in.passport);
    passports.insert_or_assign(in.state.id, std::move(*in.passport));
    held[in.state.id] =
        Held{static_cast<std::uint64_t>(f.integer("c").value_or(static_cast<std::int64_t>(in.moves))), mesh::unix_ms()};
    save();
    ++move_stats.moves_in;
    sync_locked();  // grants approved for it meanwhile
}

void Node::Impl::on_move_abort(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    auto it = incoming.find({from, static_cast<std::uint64_t>(*r)});
    if (it == incoming.end()) return;
    if (it->second.phase == Incoming::Phase::prepared) runtime.abort_arrival(it->second.state.id);
    incoming.erase(it);
}

// A capability the runtime cannot re-create: from the grant it came from,
// if the grant is the paglet's own, valid and covers this host. Narrowings
// the paglet made (fewer rights, a subdirectory, an earlier expiry) stay.
std::optional<runtime::Cap> Node::Impl::recreate(const mesh::Principal& principal, const runtime::Cap& kept) {
    // Pins refer to the pinned paglet, not to their holder's host: they stay
    // valid (every host has a locator).
    if (kept.kind == runtime::Cap::Kind::resource && kept.resource_type == "pin" &&
        kept.target == runtime::system_paglet_id("locator")) {
        if (!runtime.system_paglet("locator")) return std::nullopt;
        return kept;
    }
    // Artifacts that name the host holding their content work everywhere
    // (planning/cpp-content-exchange.md). Called under the runtime's lock:
    // no runtime calls here.
    if (runtime::mesh_artifact(kept)) return kept;
    if (!kept.grant) return std::nullopt;
    auto id = parse_hex_id(*kept.grant);
    if (!id) return std::nullopt;
    const mesh::LedgerState& st = ledger.state();
    auto g = st.grants.find(*id);
    if (g == st.grants.end() || g->second.principal != principal || g->second.expires <= mesh::unix_ms() ||
        !mesh::selects(st, g->second.hosts, key.public_key())) {
        return std::nullopt;
    }
    auto full = materialize(*id, g->second);
    if (!full || full->kind != kept.kind) return std::nullopt;
    runtime::Cap c = std::move(*full);
    std::vector<std::string> ops;
    for (const auto& op : kept.ops) {
        if (std::ranges::find(c.ops, op) != c.ops.end() || std::ranges::find(c.ops, std::string("*")) != c.ops.end()) {
            ops.push_back(op);
        }
    }
    c.ops = std::move(ops);
    if (c.kind == runtime::Cap::Kind::resource) {
        if (c.resource_type != kept.resource_type || !within(c.resource, kept.resource)) return std::nullopt;
        c.resource = kept.resource;
    }
    if (kept.expires && (!c.expires || *kept.expires < *c.expires)) c.expires = kept.expires;
    c.uses_left = kept.uses_left;
    c.transferable = c.transferable && kept.transferable;
    c.badge = kept.badge;
    // Rebinding the local provider must not create a new revocation root.
    c.id = kept.id;
    c.lineage = kept.lineage;
    materialized.insert(hex(*id));  // delivered: sync does not send it again
    return c;
}

void Node::Impl::check_move_deadlines() {
    const auto now = Clock::now();
    std::vector<std::uint64_t> late;
    for (const auto& [r, o] : outgoing) {
        if (now >= o.deadline) late.push_back(r);
    }
    for (auto r : late) {
        auto it = outgoing.find(r);
        if (it->second.recording) {
            abort_recording(r, "the location records could not be updated (" + std::to_string(it->second.acks.size()) +
                                   " of " + std::to_string(it->second.responsible.size()) + " hosts answered)");
            continue;
        }
        it->second.failures.push_back("host " + mesh::key_id(it->second.target).substr(0, 16) + ": no answer in time");
        decisions[r] = Decision{false, it->second.target, now + decision_ttl};
        send_frame(it->second.target,
                   mesh::Map{{"t", mesh::Value("move-abort")}, {"r", mesh::Value(static_cast<std::int64_t>(r))}});
        offer_next(r);
    }
    for (auto it = incoming.begin(); it != incoming.end();) {
        Incoming& in = it->second;
        if (now < in.deadline) {
            ++it;
            continue;
        }
        if (in.phase == Incoming::Phase::prepared) {
            // Only the source decides; ask it until it answers.
            in.deadline = now + move_timeout;
            send_frame(it->first.first, mesh::Map{{"t", mesh::Value("move-query")},
                                                  {"r", mesh::Value(static_cast<std::int64_t>(it->first.second))}});
            ++it;
        } else {
            it = incoming.erase(it);  // the source has given up on it as well
        }
    }
    std::erase_if(decisions, [&](const auto& e) { return now >= e.second.expires; });
}

// -- Node API -----------------------------------------------------------------------

std::expected<void, std::string> Node::dispatch(const runtime::PagletId& paglet, std::string destination) {
    if (auto t = runtime::parse_transfer_ticket(destination); !t) return std::unexpected(t.error());
    auto r = impl_->runtime.dispatch(paglet, std::move(destination));
    if (!r) return std::unexpected(std::string(abi::error_name(r.error())));
    return {};
}

void Node::set_move_timeout(std::chrono::milliseconds timeout) {
    std::lock_guard lock(impl_->mu);
    impl_->move_timeout = timeout;
}

void Node::set_page_cache_bytes(std::size_t bytes) {
    std::lock_guard lock(impl_->mu);
    impl_->pages.set_budget(bytes);
}

Node::MoveStats Node::move_stats() const {
    std::lock_guard lock(impl_->mu);
    return impl_->move_stats;
}

}  // namespace paglets::node
