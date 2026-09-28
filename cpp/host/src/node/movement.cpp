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

std::expected<Node::Impl::Ticket, std::string> parse_ticket(std::string_view text) {
    Node::Impl::Ticket t;
    const auto q = text.find('?');
    t.target = std::string(text.substr(0, q));
    if (t.target.empty()) return std::unexpected(std::string("empty destination"));
    if (q == std::string_view::npos) return t;
    std::string_view rest = text.substr(q + 1);
    while (!rest.empty()) {
        const auto amp = rest.find('&');
        const std::string_view param = rest.substr(0, amp);
        rest = amp == std::string_view::npos ? std::string_view() : rest.substr(amp + 1);
        const auto eq = param.find('=');
        if (eq == std::string_view::npos) return std::unexpected("malformed ticket parameter " + std::string(param));
        const std::string_view key = param.substr(0, eq);
        const std::string_view value = param.substr(eq + 1);
        if (key == "retries") {
            int n = 0;
            auto [p, ec] = std::from_chars(value.data(), value.data() + value.size(), n);
            if (ec != std::errc() || p != value.data() + value.size() || n < 1 || n > 16) {
                return std::unexpected(std::string("retries is 1 to 16"));
            }
            t.retries = n;
        } else if (key == "arrival") {
            if (value != "active" && value != "inactive")
                return std::unexpected(std::string("arrival is active or inactive"));
            t.activate = value == "active";
        } else {
            return std::unexpected("unknown ticket parameter " + std::string(key));
        }
    }
    return t;
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

// Does the path `inner` lie within `outer` (resources of dir capabilities,
// `root:path`)?
bool within(std::string_view outer, std::string_view inner) {
    if (inner == outer) return true;
    if (!inner.starts_with(outer)) return false;
    const char next = inner[outer.size()];
    return outer.back() == ':' || next == '/';
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
    // Called with the runtime's lock held: queued for location.cpp.
    hooks.unresolved = [this](runtime::RemoteMessage m) {
        std::lock_guard lock(unresolved_mu);
        unresolved.push_back(std::move(m));
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
                                                    std::vector<std::string>& why_not) {
    const mesh::LedgerState& st = ledger.state();
    std::vector<mesh::PublicKey> matches;
    const std::string& t = ticket.target;
    for (const auto& [k, h] : st.hosts) {
        if (k == key.public_key()) continue;
        bool match = false;
        if (t == "any") {
            match = true;
        } else if (t.starts_with("label:")) {
            match = std::ranges::find(h.labels, t.substr(6)) != h.labels.end();
        } else {
            const std::string id = mesh::key_id(k);
            match = h.name == t || id == t || (t.size() >= 8 && id.starts_with(t));
        }
        if (match) matches.push_back(k);
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
                    return g.principal.paglet == principal.paglet && g.item == (*manifest)[i] && g.expires > now &&
                           mesh::selects(st, g.hosts, k);
                });
                if (!granted) {
                    ok = false;
                    why_not.push_back("host " + st.hosts.at(k).name + ": the manifest's " + (*manifest)[i].service +
                                      " access is denied there");
                }
            }
        }
        if (ok) out.push_back(k);
    }
    // Several hosts match `any` and labels: spread paglets over them.
    if (t == "any" || t.starts_with("label:")) {
        static thread_local std::mt19937_64 random{std::random_device{}()};
        std::ranges::shuffle(out, random);
    }
    // Hosts that are up first (location.cpp keeps the view).
    update_view();
    std::ranges::stable_partition(out, [&](const mesh::PublicKey& k) { return std::ranges::binary_search(view, k); });
    if (out.size() > static_cast<std::size_t>(ticket.retries)) out.resize(static_cast<std::size_t>(ticket.retries));
    return out;
}

void Node::Impl::fail_move(Outgoing& o, const std::string& why) {
    ++move_stats.moves_failed;
    runtime.finish_departure(o.departure.move, std::nullopt, why);
}

void Node::Impl::start_move(runtime::Departure d) {
    take_spawns();
    Outgoing o;
    auto fail = [&](const std::string& why) {
        o.departure = std::move(d);
        fail_move(o, why);
    };
    auto ticket = parse_ticket(d.destination);
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
    auto hosts = candidates(*ticket, principal, manifest, why_not);
    if (hosts.empty()) {
        std::string why = "no host matches " + ticket->target;
        for (const auto& w : why_not) why += "; " + w;
        return fail(why);
    }
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
        std::string why = "no host took the paglet";
        for (const auto& f : o.failures) why += "; " + f;
        fail_move(o, why);
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
    out.deadline = Clock::now() + move_timeout;
    send_frame(out.target, mesh::Map{{"t", mesh::Value("move-offer")},
                                     {"r", mesh::Value(static_cast<std::int64_t>(r))},
                                     {"b", mesh::Value(bytes)},
                                     {"s", mesh::Value::bin(sig)}});
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
        parts.emplace_back(
            mesh::Array{mesh::Value(*index),
                        mesh::Value(mesh::Bytes(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(n)))});
        part_bytes += n;
        ++move_stats.pages_sent;
        move_stats.bytes_sent += n;
        if (part_bytes >= pages_per_frame_bytes) flush();
    }
    flush();
    o.deadline = Clock::now() + move_timeout;
}

void Node::Impl::on_move_ready(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    auto it = outgoing.find(static_cast<std::uint64_t>(*r));
    if (it == outgoing.end() || it->second.target != from || it->second.recording) return;
    // The move commits once a majority of the responsible hosts recorded
    // where the paglet goes (location.cpp).
    record_move(it->first);
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
    const runtime::PagletId id = in.state.id;
    auto lost = runtime.prepare_arrival(std::move(a), [this, id](const runtime::Cap& c) { return recreate(id, c); });
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
    if (auto ok = runtime.commit_arrival(in.state.id, in.activate); !ok) {
        warn("committing the arrival of " + in.state.id.substr(0, 8) + ": " + ok.error());
        return;
    }
    save_passport(*in.passport);
    passports.insert_or_assign(in.state.id, std::move(*in.passport));
    held[in.state.id] =
        Held{static_cast<std::uint64_t>(f.integer("c").value_or(static_cast<std::int64_t>(in.moves))), mesh::unix_ms()};
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
std::optional<runtime::Cap> Node::Impl::recreate(const runtime::PagletId& paglet, const runtime::Cap& kept) {
    // Pins refer to the pinned paglet, not to their holder's host: they stay
    // valid (every host has a locator).
    if (kept.kind == runtime::Cap::Kind::resource && kept.resource_type == "pin" &&
        kept.target == runtime::system_paglet_id("locator")) {
        if (!runtime.system_paglet("locator")) return std::nullopt;
        return kept;
    }
    if (!kept.grant) return std::nullopt;
    auto id = parse_hex_id(*kept.grant);
    if (!id) return std::nullopt;
    const mesh::LedgerState& st = ledger.state();
    auto g = st.grants.find(*id);
    if (g == st.grants.end() || g->second.principal.paglet != paglet || g->second.expires <= mesh::unix_ms() ||
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
    if (c.kind == runtime::Cap::Kind::resource && within(c.resource, kept.resource)) c.resource = kept.resource;
    if (kept.expires && (!c.expires || *kept.expires < *c.expires)) c.expires = kept.expires;
    c.uses_left = kept.uses_left;
    c.transferable = c.transferable && kept.transferable;
    c.badge = kept.badge;
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
    if (auto t = parse_ticket(destination); !t) return std::unexpected(t.error());
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
