// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Compute jobs in the mesh (planning/cpp-compute-jobs.md): the mesh policy
// admits jobs (service `compute-jobs`, operation `submit`), and jobs with
// hosts ("any", or host names and key IDs) run on another host of the mesh.
//
// Frames between hosts:
//   job-submit   p paglet, c correlation, s sender, m module, j spec (with
//                the input), a artifact with the input (caps), k marks
//   job-cancel   p, c
//   job-status   p, c, s status (every 2 s while the job is queued or running)
//   job-refused  p, c, e error
// The result goes back as the reply of the job (a `deliver` frame), to the
// paglet wherever it is now. A job whose host leaves the view fails with
// `gone`; jobs of a host that left the view are cancelled (both after the
// time mesh-info snapshots stay fresh, so a short break changes nothing).

#include "node_impl.hpp"

namespace paglets::node {

namespace {

constexpr auto status_every = std::chrono::seconds(2);

runtime::Bytes encode_sender(const abi::SenderRecord& s) {
    msgpack::Writer w;
    abi::paglets_encode(w, s);
    return w.take();
}

std::optional<abi::SenderRecord> decode_sender(std::span<const std::uint8_t> bytes) {
    msgpack::Reader r(bytes);
    abi::SenderRecord s;
    if (!abi::paglets_decode(r, s) || !r.ok()) return std::nullopt;
    return s;
}

mesh::Map job_frame(std::string_view type, const runtime::PagletId& paglet, std::uint64_t correlation) {
    return mesh::Map{{"t", mesh::Value(type)},
                     {"p", mesh::Value(paglet)},
                     {"c", mesh::Value(static_cast<std::int64_t>(correlation))}};
}

}  // namespace

void Node::Impl::install_jobs() {
    runtime::JobHooks hooks;
    // Called with the runtime's lock held: only the trust snapshot is used.
    hooks.admit = [this](const abi::SenderRecord& caller, const abi::JobSpec& spec) {
        return admit_job(caller, spec, key.public_key());
    };
    hooks.submit_remote = [this](runtime::RemoteJob job) { submit_remote_job(std::move(job)); };
    hooks.cancel_remote = [this](const runtime::PagletId& paglet, std::uint64_t correlation) {
        cancel_remote_job(paglet, correlation);
    };
    runtime.set_job_hooks(std::move(hooks));
}

// The policy decides on `host`: a rule for service `compute-jobs`, operation
// `submit`, that denies refuses the job; an allowing rule may cap its wall
// time (max_duration); without a rule, jobs are allowed. A module other
// than the paglet's own must have the mesh's trust.
std::expected<std::int64_t, std::int32_t> Node::Impl::admit_job(const abi::SenderRecord& caller,
                                                                 const abi::JobSpec& spec,
                                                                 const mesh::PublicKey& host) {
    std::shared_ptr<const mesh::LedgerState> st;
    {
        std::lock_guard lock(trust_mu);
        st = trust;
    }
    if (!st) return 0;
    if (spec.module) {
        auto digest = mesh::parse_key_id(*spec.module);
        if (!digest) return std::unexpected(abi::invalid_argument);
        if (!mesh::check_module(*st, *digest, "roaming").allowed) return std::unexpected(abi::denied);
    }
    const mesh::Principal principal = principal_of(caller);
    const mesh::Item item{"compute-jobs", {"submit"}, {}, {}};
    const mesh::Evaluation e = mesh::evaluate(*st, principal, host, item);
    if (!e.rule) return 0;
    if (e.decision == mesh::Decision::allow) return e.max_duration_ms.value_or(0);
    if (e.decision == mesh::Decision::ask) {
        // A grant for it, if the paglet asked and was approved.
        const std::int64_t now = mesh::unix_ms();
        for (const auto& [id, g] : st->grants) {
            if (g.principal == principal && g.expires > now && mesh::selects(*st, g.hosts, host) &&
                g.item.service == item.service &&
                std::ranges::find(g.item.ops, std::string("submit")) != g.item.ops.end()) {
                return 0;
            }
        }
    }
    return std::unexpected(abi::denied);
}

// mu held: the host for a job. `hosts` is {"any"} (every live host) or host
// names and key IDs; the one with the most free slots wins (this host on a
// tie, then the least loaded).
std::optional<mesh::PublicKey> Node::Impl::pick_job_host(const std::vector<std::string>& hosts, std::int64_t cores) {
    const mesh::PublicKey self = key.public_key();
    const mesh::LedgerState& st = ledger.state();
    std::vector<mesh::PublicKey> keys;
    const bool any = hosts.size() == 1 && hosts.front() == "any";
    if (any) {
        keys.push_back(self);
        for (const auto& k : view) {
            if (k != self && compatible(k)) keys.push_back(k);
        }
    } else {
        for (const auto& h : hosts) {
            const auto k = host_detail::resolve_host_address(st, h, false);
            if (!k || std::ranges::find(keys, *k) != keys.end()) continue;
            if (*k == self || (std::ranges::binary_search(view, *k) && compatible(*k))) keys.push_back(*k);
        }
    }
    // Cores sent to each host since its last snapshot count as taken.
    std::map<mesh::PublicKey, std::int64_t> sent_since;
    for (const auto& [id, s] : jobs.sent) {
        auto snap = compute.snapshots.find(s.host);
        if (snap != compute.snapshots.end() && s.sent_ms > snap->second.observed_ms) sent_since[s.host] += s.cores;
    }
    refresh_job_load();
    std::optional<mesh::PublicKey> best;
    std::int64_t best_free = 0;
    double best_score = 0;
    for (const auto& k : keys) {
        std::int64_t slots = 0;
        std::int64_t free = 0;
        double load = 0;
        if (k == self) {
            if (compute.slots == 0) sample_self();
            slots = compute.slots;
            free = compute.slots - compute.used() - compute.jobs_queued - static_cast<std::int64_t>(compute.queue.size());
            load = compute.own.load_per_cpu;
        } else if (auto snap = compute.snapshots.find(k); snap != compute.snapshots.end()) {
            slots = snap->second.slots;
            free = snap->second.slots_free - snap->second.queued - sent_since[k];
            load = snap->second.load_per_cpu;
        } else if (any) {
            continue;  // nothing known about it yet
        } else {
            slots = cores;  // named, but no snapshot yet: it may take it
        }
        if (slots < cores) continue;
        if (!best || free > best_free || (free == best_free && k != self && *best != self && load < best_score)) {
            best = k;
            best_free = free;
            best_score = load;
        }
    }
    return best;
}

// From the runtime's deferred thread (no locks held).
void Node::Impl::submit_remote_job(runtime::RemoteJob job) {
    std::lock_guard lock(mu);
    const auto id = std::make_pair(job.paglet, job.correlation);
    auto digest = mesh::parse_key_id(job.module);
    auto target = digest ? pick_job_host(job.spec.hosts, job.spec.cores) : std::nullopt;
    if (!target) {
        runtime.fail_job(job.paglet, job.correlation, digest ? abi::not_found : abi::invalid_argument);
        return;
    }
    const mesh::PublicKey self = key.public_key();
    JobsState::Sent sent;
    sent.host = *target;
    sent.sent = Clock::now();
    sent.sent_ms = mesh::unix_ms();
    sent.cores = job.spec.cores;
    jobs.sent[id] = sent;
    abi::JobStatus status;
    status.state = "queued";
    status.host = mesh::key_id(*target);
    runtime.note_job(job.paglet, job.correlation, status);
    if (*target == self) {
        runtime::IncomingJob in{job.paglet,       {},
                                job.correlation,  job.sender.owner,
                                job.module,       job.spec.function,
                                std::move(job.spec.input), job.spec.cores,
                                job.spec.max_ms,  job.spec.memory_mb,
                                std::move(job.marks), std::move(job.input_artifact)};
        run_job_here(std::nullopt, std::move(in), *digest);
        return;
    }
    mesh::Map frame = job_frame("job-submit", job.paglet, job.correlation);
    frame.emplace_back("s", mesh::Value(encode_sender(job.sender)));
    frame.emplace_back("m", mesh::Value::bin(*digest));
    mesh::Array marks;
    for (auto& m : job.marks) marks.emplace_back(std::move(m));
    frame.emplace_back("k", mesh::Value(std::move(marks)));
    if (job.input_artifact) {
        frame.emplace_back("a", mesh::Value(runtime::encode_caps({*job.input_artifact})));
    }
    frame.emplace_back("j", mesh::Value(abi::encode(job.spec)));
    send_frame(*target, std::move(frame));
}

// mu held: gets the module, then runs the job on this host. `from`: the
// requester's host (nullopt: a paglet of this host).
void Node::Impl::run_job_here(const std::optional<mesh::PublicKey>& from, runtime::IncomingJob job,
                              const paglets::Digest& module) {
    auto refuse = [this, from](const runtime::PagletId& paglet, std::uint64_t correlation, std::int32_t error) {
        if (!from) {
            runtime.fail_job(paglet, correlation, error);
            return;
        }
        mesh::Map frame = job_frame("job-refused", paglet, correlation);
        frame.emplace_back("e", mesh::Value(static_cast<std::int64_t>(error)));
        send_frame(*from, std::move(frame));
    };
    {
        std::lock_guard lock(trust_mu);
        if (trust && !mesh::check_module(*trust, module, "roaming").allowed) {
            return refuse(job.paglet, job.correlation, abi::denied);
        }
    }
    need_module(module, from,
                [this, refuse, job = std::move(job)](const std::expected<std::string, std::string>& hash) mutable {
                    const auto paglet = job.paglet;
                    const auto correlation = job.correlation;
                    if (!hash) {
                        warn("job of " + paglet.substr(0, 8) + ": " + hash.error());
                        return refuse(paglet, correlation, abi::not_found);
                    }
                    auto id = runtime.submit_job(std::move(job));
                    if (!id) refuse(paglet, correlation, id.error());
                });
}

// From the runtime's deferred thread (no locks held).
void Node::Impl::cancel_remote_job(const runtime::PagletId& paglet, std::uint64_t correlation) {
    std::lock_guard lock(mu);
    auto it = jobs.sent.find({paglet, correlation});
    if (it == jobs.sent.end()) return;
    if (it->second.host == key.public_key()) {
        runtime.cancel_job_of(paglet, correlation, {});
        return;
    }
    send_frame(it->second.host, job_frame("job-cancel", paglet, correlation));
}

bool Node::Impl::on_jobs_frame(std::string_view type, const mesh::PublicKey& from, const mesh::Fields& f) {
    if (!type.starts_with("job-")) return false;
    auto paglet = f.str("p");
    auto c = f.integer("c");
    if (!paglet || !c || !ledger.state().is_host(from)) return true;
    const auto correlation = static_cast<std::uint64_t>(*c);
    if (type == "job-submit") {
        auto s = f.bin("s");
        auto m = f.fixed<32>("m");
        auto j = f.bin("j");
        if (!s || !m || !j) return true;
        auto sender = decode_sender(*s);
        abi::JobSpec spec;
        if (!sender || !abi::decode(*j, spec) || spec.cores < 1) return true;
        auto refuse = [&](std::int32_t error) {
            mesh::Map frame = job_frame("job-refused", *paglet, correlation);
            frame.emplace_back("e", mesh::Value(static_cast<std::int64_t>(error)));
            send_frame(from, std::move(frame));
        };
        // This host's policy decides too.
        auto cap_ms = admit_job(*sender, spec, key.public_key());
        if (!cap_ms) return refuse(cap_ms.error()), true;
        if (*cap_ms > 0 && (spec.max_ms == 0 || spec.max_ms > *cap_ms)) spec.max_ms = *cap_ms;
        std::optional<runtime::Cap> artifact;
        if (auto a = f.bin("a")) {
            auto caps = runtime::decode_caps(*a);
            if (!caps || caps->size() != 1) return refuse(abi::malformed), true;
            artifact = std::move(caps->front());
        }
        std::vector<std::string> marks = f.strings("k").value_or(std::vector<std::string>{});
        runtime::IncomingJob in{*paglet,         mesh::key_id(from), correlation,   sender->owner,
                                to_hex(*m),      spec.function,      std::move(spec.input), spec.cores,
                                spec.max_ms,     spec.memory_mb,     std::move(marks), std::move(artifact)};
        jobs.requesters_lost.erase(from);
        run_job_here(from, std::move(in), *m);
    } else if (type == "job-cancel") {
        runtime.cancel_job_of(*paglet, correlation, mesh::key_id(from));
    } else if (type == "job-status" || type == "job-refused") {
        auto it = jobs.sent.find({*paglet, correlation});
        if (it == jobs.sent.end() || it->second.host != from) return true;
        if (type == "job-refused") {
            jobs.sent.erase(it);
            runtime.fail_job(*paglet, correlation, static_cast<std::int32_t>(f.integer("e").value_or(abi::failed)));
            return true;
        }
        abi::JobStatus status;
        if (auto s = f.bin("s"); s && abi::decode(*s, status)) {
            status.host = mesh::key_id(from);
            runtime.note_job(*paglet, correlation, std::move(status));
        }
    }
    return true;
}

// mu held, every tick.
void Node::Impl::jobs_tick() {
    const auto now = Clock::now();
    const auto lost_after = compute.timing.ttl;
    const mesh::PublicKey self = key.public_key();
    // Jobs of paglets here on other hosts: ended (the result came), or lost
    // with their host.
    const auto waiting = runtime.remote_jobs();
    for (auto it = jobs.sent.begin(); it != jobs.sent.end();) {
        auto& [id, s] = *it;
        if (std::ranges::find(waiting, id) == waiting.end()) {
            it = jobs.sent.erase(it);
            continue;
        }
        if (s.host != self && !std::ranges::binary_search(view, s.host)) {
            if (s.lost == Clock::time_point{}) s.lost = now;
            if (now - s.lost > lost_after) {
                runtime.fail_job(id.first, id.second, abi::gone);
                it = jobs.sent.erase(it);
                continue;
            }
        } else {
            s.lost = {};
        }
        ++it;
    }
    if (now < jobs.next_status) return;
    jobs.next_status = now + status_every;
    // Where the jobs that run here are, for their paglets.
    const std::string host = mesh::key_id(self);
    for (const auto& j : runtime.jobs()) {
        abi::JobStatus status{j.state, host, j.done, j.total, j.elapsed_ms, j.aot};
        if (j.requester_host.empty()) {
            if (jobs.sent.contains({j.paglet, j.correlation})) runtime.note_job(j.paglet, j.correlation, status);
            continue;
        }
        auto requester = mesh::parse_key_id(j.requester_host);
        if (!requester) continue;
        if (!std::ranges::binary_search(view, *requester)) {
            // Its host went away: nobody waits for the result.
            auto [lost, first] = jobs.requesters_lost.try_emplace(*requester, now);
            if (!first && now - lost->second > lost_after) runtime.cancel_job(j.id);
            continue;
        }
        jobs.requesters_lost.erase(*requester);
        mesh::Map frame = job_frame("job-status", j.paglet, j.correlation);
        frame.emplace_back("s", mesh::Value(abi::encode(status)));
        send_frame(*requester, std::move(frame));
    }
}

}  // namespace paglets::node
