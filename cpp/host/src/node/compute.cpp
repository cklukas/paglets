// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Mesh information and compute services (planning/cpp-compute.md): the
// `mesh-info` system paglet (every host samples itself; snapshots go to
// every live host) and `compute-slots` (admission of compute work on this
// host, with queues, grants and redirects to hosts with free slots; no
// central scheduler).

#include "node_impl.hpp"

#include "../platform/platform.hpp"

#include <paglets/services/compute_slots.hpp>
#include <paglets/services/mesh_info.hpp>

namespace paglets::node {

namespace {

namespace mi = services::mesh_info;
namespace cs = services::compute_slots;

constexpr std::size_t max_snapshot_bytes = 64 * 1024;
constexpr int max_redirects_per_tick = 4;

std::int64_t now_ms() {
    return mesh::unix_ms();
}

// Lower is better.
double score(const mi::Snapshot& s) {
    const double memory =
        s.memory_total > 0 ? 1.0 - static_cast<double>(s.memory_available) / static_cast<double>(s.memory_total) : 0;
    const double slots = s.slots > 0 ? 1.0 - static_cast<double>(s.slots_free) / static_cast<double>(s.slots) : 1;
    return s.load_per_cpu + s.cpu_percent / 100 + memory + slots + 0.1 * static_cast<double>(s.queued);
}

cs::HostSlots slots_of(const mi::Snapshot& s) {
    return cs::HostSlots{s.host, s.host_name, s.slots, s.slots_free, s.queued, s.observed_ms};
}

}  // namespace

// -- sampling and gossip ------------------------------------------------------------------

void Node::Impl::sample_self() {
    ComputeState& c = compute;
    mi::Snapshot s;
    s.host = mesh::key_id(key.public_key());
    const mesh::LedgerState& st = ledger.state();
    if (auto h = st.hosts.find(key.public_key()); h != st.hosts.end()) {
        s.host_name = h->second.name;
        s.labels = h->second.labels;
    }
    s.observed_ms = now_ms();
    const auto summary = platform::summary();
    s.os = summary.os;
    s.architecture = summary.architecture;
    s.cpus = std::max<std::int64_t>(1, summary.cpu_count);
    const auto cpu = platform::cpu_sample();
    if (c.cpu_before) s.cpu_percent = platform::cpu_percent(c.cpu_before->all, cpu.all);
    c.cpu_before = cpu;
    if (auto load = platform::load_average(); load && !load->empty()) {
        s.load_per_cpu = (*load)[0] / static_cast<double>(s.cpus);
    } else {
        s.load_per_cpu = s.cpu_percent / 100;
    }
    const auto memory = platform::memory();
    s.memory_total = static_cast<std::int64_t>(memory.total);
    s.memory_available = static_cast<std::int64_t>(memory.available);
    std::error_code ec;
    const fs::path work = runtime.config().state_dir.empty() ? fs::temp_directory_path(ec) : runtime.config().state_dir;
    if (auto space = fs::space(work, ec); !ec) s.work_free = static_cast<std::int64_t>(space.available);
    s.paglets = std::ranges::count_if(runtime.list(), [](const auto& p) { return !p.module.empty(); });
    if (c.slots == 0) c.slots = s.cpus;
    s.slots = c.slots;
    s.slots_free = c.slots - c.used();
    s.queued = static_cast<std::int64_t>(c.queue.size());
    for (const auto& [service, offer] : c.offers) s.offers.push_back(offer);
    c.own = std::move(s);
}

void Node::Impl::compute_tick() {
    ComputeState& c = compute;
    const auto now = Clock::now();
    if (now >= c.next_sample) {
        c.next_sample = now + c.timing.sample;
        sample_self();
    }
    end_stale_leases();
    grant_queued();
    redirect_queued();
    // Our snapshot goes to every live host (each host answers for the
    // whole mesh from what it hears).
    if (now >= c.next_gossip) {
        c.next_gossip = now + c.timing.gossip;
        c.own.slots_free = c.slots - c.used();
        c.own.queued = static_cast<std::int64_t>(c.queue.size());
        const runtime::Bytes snapshot = wire::to_msgpack(c.own);
        for (const auto& k : view) {
            if (k == key.public_key()) continue;
            send_frame(k, mesh::Map{{"t", mesh::Value("mi-sync")}, {"s", mesh::Value(snapshot)}});
        }
    }
    const std::int64_t oldest = now_ms() - 3 * c.timing.ttl.count();
    std::erase_if(c.snapshots, [&](const auto& e) { return e.second.observed_ms < oldest; });
}

void Node::Impl::on_mesh_info(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto bytes = f.bin("s");
    if (!bytes || bytes->size() > max_snapshot_bytes || !ledger.state().is_host(from)) return;
    mi::Snapshot s;
    if (!wire::from_msgpack(*bytes, s)) return;
    // A host speaks for itself only (the channel proved who sent it).
    if (s.host != mesh::key_id(from) || s.observed_ms > now_ms() + 300'000) return;
    auto& slot = compute.snapshots[from];
    if (slot.observed_ms <= s.observed_ms) slot = std::move(s);
}

std::vector<services::mesh_info::Snapshot> Node::Impl::fresh_snapshots(std::int64_t max_age_ms, bool include_self) {
    std::vector<mi::Snapshot> out;
    if (include_self) {
        compute.own.slots_free = compute.slots - compute.used();
        compute.own.queued = static_cast<std::int64_t>(compute.queue.size());
        out.push_back(compute.own);
    }
    const std::int64_t oldest = now_ms() - max_age_ms;
    for (const auto& [k, s] : compute.snapshots) {
        if (s.observed_ms >= oldest && std::ranges::binary_search(view, k) && compatible(k)) out.push_back(s);
    }
    return out;
}

// -- service offers ---------------------------------------------------------------------------

namespace {

const mi::Attribute* attribute(const mi::Offer& o, std::string_view name) {
    auto it = std::ranges::find_if(o.attributes, [&](const mi::Attribute& a) { return a.name == name; });
    return it == o.attributes.end() ? nullptr : &*it;
}

bool listed(std::string_view list, std::string_view item) {
    while (!list.empty()) {
        const auto comma = list.find(',');
        std::string_view entry = list.substr(0, comma);
        while (!entry.empty() && entry.front() == ' ') entry.remove_prefix(1);
        while (!entry.empty() && entry.back() == ' ') entry.remove_suffix(1);
        if (entry == item) return true;
        if (comma == std::string_view::npos) break;
        list.remove_prefix(comma + 1);
    }
    return false;
}

bool satisfies(const mi::Offer& o, const mi::Requirement& r) {
    const mi::Attribute* a = attribute(o, r.name);
    if (a == nullptr) return false;
    if (r.is == "=") return a->text == r.text;
    if (r.is == "has") return listed(a->text, r.text);
    if (r.is == ">=") return a->numeric && a->number >= r.number;
    if (r.is == "<=") return a->numeric && a->number <= r.number;
    return false;
}

}  // namespace

std::vector<services::mesh_info::OfferMatch> Node::Impl::find_offers(const mi::OffersRequest& q) {
    std::vector<std::pair<double, mi::OfferMatch>> found;  // order key, match
    const bool largest = q.prefer.starts_with('-');
    const std::string prefer = largest ? q.prefer.substr(1) : q.prefer;
    for (const auto& s : fresh_snapshots(std::max<std::int64_t>(q.max_age_ms, 1000), q.include_self)) {
        for (const auto& o : s.offers) {
            if (o.service != q.service) continue;
            if (!q.op.empty() && std::ranges::find(o.ops, q.op) == o.ops.end()) continue;
            if (!std::ranges::all_of(q.require, [&](const mi::Requirement& r) { return satisfies(o, r); })) continue;
            double order = score(s);
            if (!prefer.empty()) {
                const mi::Attribute* a = attribute(o, prefer);
                if (a == nullptr || !a->numeric) continue;
                order = largest ? -a->number : a->number;
            }
            found.emplace_back(order, mi::OfferMatch{s.host, s.host_name, s.labels, s.load_per_cpu, o});
        }
    }
    std::ranges::stable_sort(found, [](const auto& a, const auto& b) { return a.first < b.first; });
    std::vector<mi::OfferMatch> out;
    const auto limit = static_cast<std::size_t>(std::clamp<std::int64_t>(q.limit, 1, 256));
    for (auto& [order, m] : found) {
        if (out.size() >= limit) break;
        out.push_back(std::move(m));
    }
    return out;
}

// -- compute-slots ----------------------------------------------------------------------------

std::int64_t Node::Impl::ComputeState::used() const {
    std::int64_t n = 0;
    for (const auto& [id, l] : leases) n += l.cores;
    return n;
}

// Leases of paglets that ended or left, and waiters that are gone.
void Node::Impl::end_stale_leases() {
    ComputeState& c = compute;
    std::erase_if(c.leases, [&](const auto& e) { return !runtime.info(e.second.paglet).has_value(); });
    std::erase_if(c.queue, [&](const auto& w) { return !runtime.info(w.paglet).has_value(); });
}

std::string Node::Impl::grant(const runtime::PagletId& paglet, const std::string& job, std::int64_t cores) {
    const std::string id = "lease-" + new_cap_id().substr(0, 16);
    compute.leases[id] = ComputeState::Lease{id, paglet, job, cores, now_ms()};
    return id;
}

// Oldest-fit: the first waiter that fits gets the slots.
void Node::Impl::grant_queued() {
    ComputeState& c = compute;
    if (c.ctx == nullptr) return;
    for (auto it = c.queue.begin(); it != c.queue.end();) {
        if (it->cores > c.slots - c.used()) {
            ++it;
            continue;
        }
        const std::string lease = grant(it->paglet, it->job, it->cores);
        cs::Decision d;
        d.verdict = cs::Verdict::run_now;
        d.lease = lease;
        d.cores = it->cores;
        d.host = mesh::key_id(key.public_key());
        d.host_name = compute.own.host_name;
        // Removed from the queue only when the message was delivered.
        if (c.ctx->send(it->paglet, "compute.granted", wire::to_msgpack(d))) {
            it = c.queue.erase(it);
        } else {
            c.leases.erase(lease);
            it = c.queue.erase(it);
        }
    }
}

// A peer with free slots for a waiter (projected: its free slots minus the
// work already sent its way in this round).
std::optional<services::mesh_info::Snapshot> Node::Impl::peer_with_slots(std::int64_t cores,
                                                                         std::map<std::string, std::int64_t>& shadow,
                                                                         const std::string& not_to) {
    std::optional<mi::Snapshot> best;
    for (const auto& s : fresh_snapshots(compute.timing.ttl.count(), false)) {
        if (s.host == not_to) continue;
        if (s.slots_free - shadow[s.host] < cores || s.queued > 0) continue;
        if (!best || s.slots_free - shadow[s.host] > best->slots_free - shadow[best->host] ||
            (s.slots_free - shadow[s.host] == best->slots_free - shadow[best->host] && score(s) < score(*best))) {
            best = s;
        }
    }
    if (best) shadow[best->host] += cores;
    return best;
}

// Waiters go to hosts with free slots; one stays here (it is next).
void Node::Impl::redirect_queued() {
    ComputeState& c = compute;
    if (c.ctx == nullptr || c.queue.size() < 2) return;
    const auto now = Clock::now();
    std::map<std::string, std::int64_t> shadow;
    int budget = std::min<int>(max_redirects_per_tick, static_cast<int>((c.queue.size() + 1) / 2));
    for (auto it = std::next(c.queue.begin()); it != c.queue.end() && budget > 0;) {
        if (now - it->queued_at < c.timing.redirect_after) {
            ++it;
            continue;
        }
        auto target = peer_with_slots(it->cores, shadow, it->came_from);
        if (!target) break;
        cs::Decision d;
        d.verdict = cs::Verdict::redirect;
        d.cores = it->cores;
        d.host = target->host;
        d.host_name = target->host_name;
        d.reason = "host " + target->host_name + " has free slots";
        if (c.ctx->send(it->paglet, "compute.redirect", wire::to_msgpack(d))) {
            ++c.redirects;
            --budget;
        }
        it = c.queue.erase(it);
    }
}

class MeshInfoService final : public services::ContractPaglet<mi::Contract, MeshInfoService> {
public:
    explicit MeshInfoService(Node::Impl& node) : node_(node) {}
    std::vector<std::string> default_ops() const override { return operations(); }

    services::Result<mi::Snapshot> snapshot(const mi::SnapshotRequest&, services::Operation&) {
        std::lock_guard lock(node_.mu);
        return node_.fresh_snapshots(0, true).front();
    }

    services::Result<mi::Landscape> landscape(const mi::LandscapeRequest& q, services::Operation&) {
        std::lock_guard lock(node_.mu);
        return mi::Landscape{node_.fresh_snapshots(std::max<std::int64_t>(q.max_age_ms, 1000), true)};
    }

    services::Result<mi::Selection> select(const mi::SelectRequest& q, services::Operation&) {
        std::lock_guard lock(node_.mu);
        auto all = node_.fresh_snapshots(std::max<std::int64_t>(q.max_age_ms, 1000), q.include_self);
        std::ranges::sort(all, [](const auto& a, const auto& b) { return score(a) < score(b); });
        mi::Selection out;
        const auto limit = static_cast<std::size_t>(std::clamp<std::int64_t>(q.limit, 1, 256));
        for (const auto& s : all) {
            if (out.hosts.size() >= limit) break;
            if (s.load_per_cpu > q.max_load_per_cpu || s.memory_available < q.min_memory_available ||
                s.slots_free < q.min_slots_free) {
                continue;
            }
            const bool labelled = std::ranges::all_of(
                q.labels, [&](const std::string& l) { return std::ranges::find(s.labels, l) != s.labels.end(); });
            if (labelled) out.hosts.push_back(s);
        }
        if (out.hosts.empty()) {
            // Nothing fits: the least loaded hosts, so work still goes somewhere.
            out.fallback = true;
            for (const auto& s : all) {
                if (out.hosts.size() >= limit) break;
                out.hosts.push_back(s);
            }
        }
        return out;
    }

    services::Result<mi::Offers> find_offers(const mi::OffersRequest& q, services::Operation&) {
        std::lock_guard lock(node_.mu);
        return mi::Offers{node_.find_offers(q)};
    }

private:
    Node::Impl& node_;
};

class ComputeSlotsService final : public services::ContractPaglet<cs::Contract, ComputeSlotsService> {
public:
    explicit ComputeSlotsService(Node::Impl& node) : node_(node) {}
    std::vector<std::string> default_ops() const override { return operations(); }

    void start(runtime::SystemContext& ctx) override {
        std::lock_guard lock(node_.mu);
        node_.compute.ctx = &ctx;
    }

    services::Result<cs::Decision> request_slot(const cs::SlotRequest& q, services::Operation& op) {
        const auto& caller = op.sender();
        if (!caller || caller->id == "host" || caller->id.starts_with("system.")) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        auto& c = node_.compute;
        if (c.slots == 0) node_.sample_self();
        const std::int64_t cores = std::max<std::int64_t>(1, q.cores);
        cs::Decision d;
        d.cores = cores;
        d.host = mesh::key_id(node_.key.public_key());
        d.host_name = c.own.host_name;
        // It holds a lease already (asked twice): run.
        for (const auto& [id, l] : c.leases) {
            if (l.paglet == caller->id) {
                d.verdict = cs::Verdict::run_now;
                d.lease = id;
                return d;
            }
        }
        std::map<std::string, std::int64_t> shadow;
        if (cores > c.slots) {
            // Never here; perhaps elsewhere.
            if (auto peer = node_.peer_with_slots(cores, shadow, {})) {
                d.verdict = cs::Verdict::redirect;
                d.host = peer->host;
                d.host_name = peer->host_name;
                d.reason = "this host has " + std::to_string(c.slots) + " slots";
                return d;
            }
            d.verdict = cs::Verdict::rejected;
            d.reason = "this host has " + std::to_string(c.slots) + " slots";
            return d;
        }
        std::erase_if(c.queue, [&](const auto& w) { return w.paglet == caller->id; });
        if (c.queue.empty() && cores <= c.slots - c.used()) {
            d.verdict = cs::Verdict::run_now;
            d.lease = node_.grant(caller->id, q.job, cores);
            return d;
        }
        // Local work first, but a host with free slots now beats a queue.
        if (auto peer = node_.peer_with_slots(cores, shadow, {})) {
            d.verdict = cs::Verdict::redirect;
            d.host = peer->host;
            d.host_name = peer->host_name;
            d.reason = "host " + peer->host_name + " has free slots";
            ++c.redirects;
            return d;
        }
        Node::Impl::ComputeState::Waiter w;
        w.paglet = caller->id;
        w.job = q.job;
        w.cores = cores;
        w.since = now_ms();
        w.queued_at = Node::Impl::Clock::now();
        c.queue.push_back(std::move(w));
        d.verdict = cs::Verdict::queued;
        d.position = static_cast<std::int64_t>(c.queue.size()) - 1;
        return d;
    }

    services::Result<cs::ReleaseReply> release_slot(const cs::ReleaseRequest& q, services::Operation& op) {
        const auto& caller = op.sender();
        if (!caller) return std::unexpected(abi::denied);
        std::lock_guard lock(node_.mu);
        auto& c = node_.compute;
        auto it = c.leases.find(q.lease);
        if (it == c.leases.end()) return cs::ReleaseReply{false};
        if (it->second.paglet != caller->id && caller->id != "host") return std::unexpected(abi::denied);
        c.leases.erase(it);
        node_.grant_queued();
        return cs::ReleaseReply{true};
    }

    services::Result<cs::Status> status(const cs::StatusRequest&, services::Operation&) {
        std::lock_guard lock(node_.mu);
        return node_.compute_status();
    }

    services::Result<cs::Candidates> candidates(const cs::CandidatesRequest& q, services::Operation&) {
        std::lock_guard lock(node_.mu);
        auto all = node_.fresh_snapshots(node_.compute.timing.ttl.count(), q.include_self);
        std::erase_if(all, [&](const auto& s) { return s.slots < q.cores; });
        std::ranges::sort(all, [](const auto& a, const auto& b) {
            if (a.slots_free - a.queued != b.slots_free - b.queued) {
                return a.slots_free - a.queued > b.slots_free - b.queued;
            }
            return score(a) < score(b);
        });
        cs::Candidates out;
        for (const auto& s : all) {
            if (out.hosts.size() >= static_cast<std::size_t>(std::clamp<std::int64_t>(q.limit, 1, 256))) break;
            out.hosts.push_back(slots_of(s));
        }
        return out;
    }

private:
    Node::Impl& node_;
};

services::compute_slots::Status Node::Impl::compute_status() {
    const ComputeState& c = compute;
    cs::Status s;
    s.self = slots_of(compute.own);
    s.self.free = c.slots - c.used();
    s.self.slots = c.slots;
    s.self.queued = static_cast<std::int64_t>(c.queue.size());
    for (const auto& [id, l] : c.leases) s.leases.push_back(cs::Lease{id, l.paglet, l.job, l.cores, l.granted});
    for (const auto& w : c.queue) s.queue.push_back(cs::Waiting{w.paglet, w.job, w.cores, w.since});
    for (const auto& snap : fresh_snapshots(c.timing.ttl.count(), false)) s.peers.push_back(slots_of(snap));
    return s;
}

std::vector<std::shared_ptr<runtime::SystemPaglet>> make_compute_services(Node::Impl& impl) {
    return {std::make_shared<MeshInfoService>(impl), std::make_shared<ComputeSlotsService>(impl)};
}

// -- Node API ----------------------------------------------------------------------------------

void Node::set_compute_slots(std::int64_t slots) {
    std::lock_guard lock(impl_->mu);
    impl_->compute.slots = std::max<std::int64_t>(1, slots);
    impl_->sample_self();
}

void Node::set_compute_timing(ComputeTiming timing) {
    std::lock_guard lock(impl_->mu);
    impl_->compute.timing = timing;
    impl_->compute.next_gossip = {};
    impl_->compute.next_sample = {};
}

services::compute_slots::Status Node::compute_status() const {
    std::lock_guard lock(impl_->mu);
    return impl_->compute_status();
}

std::vector<services::mesh_info::Snapshot> Node::landscape() const {
    std::lock_guard lock(impl_->mu);
    return impl_->fresh_snapshots(impl_->compute.timing.ttl.count(), true);
}

// A changed offer is sampled and sent at once.
void Node::set_offer(services::mesh_info::Offer offer) {
    std::lock_guard lock(impl_->mu);
    if (offer.service.empty()) return;
    impl_->compute.offers[offer.service] = std::move(offer);
    impl_->sample_self();
    impl_->compute.next_gossip = {};
}

void Node::withdraw_offer(const std::string& service) {
    std::lock_guard lock(impl_->mu);
    if (impl_->compute.offers.erase(service) == 0) return;
    impl_->sample_self();
    impl_->compute.next_gossip = {};
}

std::vector<services::mesh_info::OfferMatch> Node::find_offers(const services::mesh_info::OffersRequest& q) const {
    std::lock_guard lock(impl_->mu);
    return impl_->find_offers(q);
}

}  // namespace paglets::node
