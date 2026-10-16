// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The runtime's side of movement (WP12, planning/cpp-networking.md,
// sections 4 and 5): remote endpoints, departures and arrivals, messages
// that wait for and follow a moving paglet, clones for other hosts and
// failed moves. Two runtimes are joined directly here; the mesh protocol is
// tested with nodes.

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <paglets/runtime/mobility.hpp>
#include <paglets/wasm/snapshot.hpp>
#include "../host/src/runtime_store.hpp"

#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <filesystem>
#include <random>
#include <limits>

using namespace paglets::test;
namespace pw = paglets::wasm;

namespace {

// Hosts "a" and "b": remote messages and departures go through a pump
// thread, outside the runtimes' locks, as a mesh would carry them.
struct Pair {
    std::unique_ptr<Fixture> a;
    std::unique_ptr<Fixture> b;
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::function<void()>> work;
    bool stopping = false;
    std::thread pump;
    // Departures wait here while `hold` is set.
    bool hold = false;
    std::condition_variable hold_cv;
    std::atomic<int> moves{0}, departures{0}, snapshot_pending{0}, snapshot_timers{0};
    std::function<std::optional<rt::Cap>(const rt::Cap&)> recreate;
    std::function<void(rt::Arrival&)> before_arrival;

    bool hold_reports = false;
    std::vector<rt::RemoteMessage> sent, held_reports;

    Pair(rt::Config a_config = {}, rt::Config b_config = {})
        : a(std::make_unique<Fixture>(std::move(a_config))), b(std::make_unique<Fixture>(std::move(b_config))) {
        install(*a, "a");
        install(*b, "b");
        pump = std::thread([this] { run(); });
    }
    ~Pair() {
        {
            std::lock_guard lock(mu);
            stopping = true;
            hold = false;
        }
        cv.notify_all();
        hold_cv.notify_all();
        pump.join();
        a->runtime->shutdown();
        b->runtime->shutdown();
    }

    Fixture* host(const std::string& name) {
        if (name == "a") return a.get();
        if (name == "b") return b.get();
        return nullptr;
    }

    void post(std::function<void()> job) {
        {
            std::lock_guard lock(mu);
            work.push_back(std::move(job));
        }
        cv.notify_one();
    }

    void run() {
        std::unique_lock lock(mu);
        while (true) {
            cv.wait(lock, [&] { return stopping || !work.empty(); });
            if (stopping) return;
            auto job = std::move(work.front());
            work.pop_front();
            lock.unlock();
            job();
            lock.lock();
        }
    }

    void release() {
        {
            std::lock_guard lock(mu);
            hold = false;
        }
        hold_cv.notify_all();
    }

    void install(Fixture& f, const std::string& name) {
        rt::MobilityHooks hooks;
        hooks.host_id = name;
        hooks.deliver = [this](rt::RemoteMessage m) {
            post([this, m = std::move(m)]() mutable {
                deliver(std::move(m));
            });
        };
        hooks.depart = [this, name](rt::Departure d) {
            post([this, name, d = std::move(d)]() mutable { move(name, std::move(d)); });
        };
        f.runtime->set_mobility(std::move(hooks));
    }

    // Exercise the wire representation as well as direct runtime routing.
    void deliver(rt::RemoteMessage m) {
        {
            std::lock_guard lock(mu);
            sent.push_back(m);
            if (hold_reports && m.kind == rt::RemoteMessage::Kind::undelivered) {
                held_reports.push_back(std::move(m));
                return;
            }
        }
        auto wire = rt::decode_remote(rt::encode_remote(m));
        if (!wire) {
            std::cerr << "remote test transport: " << wire.error() << "\n";
            return;
        }
        if (Fixture* to = host(m.host)) to->runtime->deliver_remote(std::move(*wire));
    }
    std::vector<rt::RemoteMessage> frames() {
        std::lock_guard lock(mu);
        return sent;
    }
    std::vector<rt::RemoteMessage> take_reports() {
        std::lock_guard lock(mu);
        hold_reports = false;
        return std::exchange(held_reports, {});
    }

    void move(const std::string& from, rt::Departure d) {
        snapshot_pending = static_cast<int>(d.state.pending.size());
        snapshot_timers = static_cast<int>(std::ranges::count_if(d.state.caps, [](const auto& c) { return c.second.kind == rt::Cap::Kind::timer; }));
        ++departures;
        {
            std::unique_lock lock(mu);
            hold_cv.wait(lock, [&] { return !hold || stopping; });
        }
        Fixture* src = host(from);
        Fixture* dst = host(d.destination);
        if (dst == nullptr || dst == src) {
            src->runtime->finish_departure(d.move, std::nullopt, "unknown destination");
            return;
        }
        // What a mesh would carry: the encoded state and image.
        auto state = rt::decode_state(rt::encode_state(d.state));
        auto image = pw::deserialize(pw::serialize(d.image));
        REQUIRE_OK(state);
        REQUIRE_OK(image);
        if (auto bytes = src->runtime->modules().bytes(state->module)) (void)dst->runtime->add_module(*bytes);
        rt::Arrival arrival{std::move(*state),       std::move(*image),       from,      d.clone,
                            std::move(d.clone_args), std::move(d.clone_caps), d.original};
        const std::string id = arrival.state.id;
        if (before_arrival) before_arrival(arrival);
        auto lost = dst->runtime->prepare_arrival(std::move(arrival), recreate);
        if (!lost) {
            src->runtime->finish_departure(d.move, std::nullopt, lost.error());
            return;
        }
        REQUIRE_OK(dst->runtime->commit_arrival(id));
        src->runtime->finish_departure(d.move, d.destination);
        ++moves;
    }
};

bool eventually(const std::function<bool()>& done, std::chrono::milliseconds timeout = 10s) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < until) {
        if (done()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return done();
}

std::int64_t count(Fixture& f, const rt::PagletId& id) {
    auto r = f.call(id, "count");
    if (r.status != 0) throw std::runtime_error("count: " + std::string(abi::error_name(r.status)));
    return dec<std::int64_t>(r.payload);
}

// A remote endpoint added by the host (as the mesh's directory will).
std::int32_t endpoint_to(Fixture& holder, const rt::PagletId& holder_id, const rt::PagletId& target,
                         const std::string& host) {
    rt::Cap c;
    c.kind = rt::Cap::Kind::endpoint;
    c.target = target;
    c.ops = {"*"};
    c.id = "test-" + target;
    c.host = host;
    auto h = holder.runtime->add_capability(holder_id, c);
    if (!h) throw std::runtime_error("add_capability failed");
    return *h;
}

}  // namespace

PAGLETS_TEST("mobility: sender and self identity use the mobility host rather than its display label") {
    Fixture f(rt::Config{.host_name = "friendly label"});
    rt::RemoteMessage sent;
    rt::MobilityHooks hooks;
    hooks.host_id = "origin-key";
    hooks.deliver = [&](rt::RemoteMessage message) { sent = std::move(message); };
    f.runtime->set_mobility(std::move(hooks));
    const auto id = f.create("conformance.wasm");
    const auto info = dec<abi::SelfInfo>(f.call(id, "info").payload);
    CHECK_EQ(info.host, std::string("origin-key"));
    const auto handle = endpoint_to(f, id, std::string(32, 'f'), "other-key");
    CHECK_EQ(f.code(id, "send", Cmd{.handle = handle, .name = "echo"}), 0);
    REQUIRE(sent.sender.has_value());
    CHECK_EQ(sent.sender->host, info.host);
    CHECK_EQ(sent.sender->id, id);
    CHECK_EQ(sent.sender->owner, info.owner);
    CHECK_EQ(sent.sender->module, info.module);
}

PAGLETS_TEST("abi C37: a paglet dispatches to another host and continues there") {
    Pair p;
    auto id = p.a->create("conformance.wasm", text("x"));
    CHECK_EQ(count(*p.a, id), 1);
    CHECK_EQ(count(*p.a, id), 2);
    CHECK_EQ(
        p.a->code(id, "lifecycle", Cmd{.name = "b", .code = static_cast<std::int32_t>(abi::LifecycleOp::dispatch)}), 0);
    REQUIRE(eventually([&] { return p.b->runtime->info(id).has_value() && !p.a->runtime->info(id); }));
    CHECK(p.a->runtime->location(id) == std::string("b"));
    CHECK(!p.a->runtime->ending(id).has_value());  // it did not end, it moved
    CHECK_EQ(count(*p.b, id), 3);                  // the memory came along
    auto j = p.b->journal(id);
    CHECK(contains(j, "event:created:x:0"));
    CHECK(contains(j, "event:dispatching:b"));
    CHECK(contains(j, "event:arrived:a:0"));
    // The host can send it on, too.
    REQUIRE(p.b->runtime->dispatch(id, "a").has_value());
    REQUIRE(eventually([&] { return p.a->runtime->info(id).has_value() && !p.b->runtime->info(id); }));
    CHECK_EQ(count(*p.a, id), 4);
    CHECK(!p.a->runtime->location(id));  // back here: no tombstone
    CHECK_EQ(p.moves.load(), 2);
}

PAGLETS_TEST("abi C40: remote endpoints, requests and replies; messages follow a moved paglet") {
    Pair p;
    auto sender = p.a->create("conformance.wasm");
    auto receiver = p.b->create("conformance.wasm");
    const std::int32_t h = endpoint_to(*p.a, sender, receiver, "b");
    CHECK(p.a->code(sender, "request", Cmd{.handle = h, .name = "echo", .payload = text("ping")}) > 0);
    CHECK(journal_eventually(*p.a, sender, "reply:ok:ping"));
    CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = h, .name = "echo"}), 0);
    CHECK(journal_eventually(*p.b, receiver, "msg:echo"));
    // Endpoints travel with messages; other capabilities do not.
    CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = h, .name = "echo", .caps = {abi::self_handle}}), 0);
    const std::int32_t timer = static_cast<std::int32_t>(p.a->code(sender, "timer", Cmd{.name = "tick", .ms = 60'000}));
    CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = h, .name = "echo", .caps = {timer}}),
             static_cast<std::int64_t>(abi::unsupported));

    // The receiver moves to a; the endpoint still says b, whose tombstone
    // forwards requests and the replies find their way back.
    REQUIRE(p.b->runtime->dispatch(receiver, "a").has_value());
    REQUIRE(eventually([&] { return p.a->runtime->info(receiver).has_value(); }));
    CHECK(p.a->code(sender, "request", Cmd{.handle = h, .name = "echo", .payload = text("again")}) > 0);
    CHECK(journal_eventually(*p.a, sender, "reply:ok:again"));
    // Requests to paglets nobody knows are answered.
    const std::int32_t lost = endpoint_to(*p.a, sender, std::string(32, 'e'), "b");
    CHECK(p.a->code(sender, "request", Cmd{.handle = lost, .name = "echo"}) > 0);
    CHECK(journal_eventually(*p.a, sender, "reply:not_found:"));
}

PAGLETS_TEST("remote undelivered: asynchronous refusals reach the sending paglet") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto handle = endpoint_to(*p.a, sender, "missing-receiver", "b");
    CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = handle, .name = "missing", .async = true}), 0);
    CHECK(journal_eventually(*p.a, sender, "undelivered:missing:not_found"));
    CHECK(p.b->logged("remote one-way message refused: missing (not_found)"));
}

PAGLETS_TEST("remote undelivered: quota and system refusals preserve the original handle and consume transfers") {
    struct RemoteOnly final : rt::SystemPaglet {
        std::string_view name() const override { return "remote-only"; }
        void handle(rt::SystemContext&, rt::ServiceCall&) override {}
    };
    for (const bool native : {false, true}) {
        rt::Config config;
        if (!native) config.mailbox_limit = 0;
        Pair p({}, config);
        const auto sender = p.a->create("conformance.wasm");
        std::string target;
        if (native) {
            auto system = p.b->runtime->add_system_paglet(std::make_shared<RemoteOnly>());
            REQUIRE_OK(system);
            target = *system;
        } else {
            target = p.b->create("conformance.wasm");
            p.b->runtime->wait_idle();
        }
        const auto handle = endpoint_to(*p.a, sender, target, "b");
        const auto transfer = endpoint_to(*p.a, sender, sender, "a");
        CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = handle, .name = "refused", .caps = {transfer}, .async = true}), 0);
        const auto status = native ? abi::denied : abi::quota;
        CHECK(journal_eventually(*p.a, sender, "undelivered:refused:" + std::string(abi::error_name(status))));
        CHECK_EQ(p.a->cmd(sender, "inspect", Cmd{.handle = transfer}).status, abi::bad_handle);
        auto frames = p.frames();
        const auto report = std::ranges::find_if(frames, [](const auto& m) { return m.kind == rt::RemoteMessage::Kind::undelivered; });
        REQUIRE(report != frames.end());
        const auto body = dec<abi::Undelivered>(report->payload);
        CHECK_EQ(body.handle, handle);
        CHECK_EQ(body.status, status);
        CHECK(report->caps.empty());
        CHECK(!report->sender);
    }
}

PAGLETS_TEST("remote undelivered: synchronous remote sends remain best effort") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto handle = endpoint_to(*p.a, sender, "missing", "b");
    CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = handle, .name = "sync"}), 0);
    REQUIRE(eventually([&] { return p.b->logged("remote one-way message refused: sync (not_found)"); }));
    const auto frames = p.frames();
    REQUIRE(frames.size() == 1);
    CHECK(!frames.front().failure);
    CHECK(!contains(p.a->journal(sender), "undelivered:sync:not_found"));
}

PAGLETS_TEST("remote undelivered: in-flight and queued reports follow a moving sender") {
    for (const bool queued : {false, true}) {
        Pair p;
        const auto sender = p.a->create("conformance.wasm");
        const auto handle = endpoint_to(*p.a, sender, "missing", "b");
        {
            std::lock_guard lock(p.mu);
            p.hold_reports = true;
            p.hold = queued;
        }
        CHECK_EQ(p.a->code(sender, "send", Cmd{.handle = handle, .name = "moving", .async = true}), 0);
        REQUIRE(eventually([&] { std::lock_guard lock(p.mu); return p.held_reports.size() == 1; }));
        REQUIRE_OK(p.a->runtime->dispatch(sender, "b"));
        if (queued) {
            REQUIRE(eventually([&] { const auto info = p.a->runtime->info(sender); return info && info->state == rt::PagletState::inactive; }));
            auto reports = p.take_reports();
            REQUIRE(reports.size() == 1);
            // A queued local validation report may contain an invalid original
            // name/handle. Forwarding must preserve that diagnostic as data.
            reports.front().payload = enc(abi::Undelivered{"paglets.spoof", -1, abi::invalid_argument});
            REQUIRE(p.a->runtime->deliver_remote(std::move(reports.front())) == abi::ok);
            p.release();
        } else {
            REQUIRE(eventually([&] { return p.moves.load() == 1; }));
            for (auto& report : p.take_reports()) p.post([&p, report = std::move(report)]() mutable { p.deliver(std::move(report)); });
        }
        REQUIRE(eventually([&] { return p.moves.load() == 1; }));
        CHECK(journal_eventually(*p.b, sender, queued ? "undelivered:paglets.spoof:invalid_argument" : "undelivered:moving:not_found"));
    }
}

PAGLETS_TEST("remote undelivered: bounded reports reject malformed or guest-shaped notifications") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    rt::RemoteMessage failed;
    failed.target = "missing";
    failed.host = "b";
    failed.name = "bounded";
    failed.hops = 4;
    failed.failure = rt::RemoteMessage::FailureRoute{sender, "a", 72};
    auto wire = rt::decode_remote(rt::encode_remote(failed));
    REQUIRE_OK(wire);
    CHECK_EQ(p.b->runtime->deliver_remote(std::move(*wire)), abi::not_found);
    CHECK(journal_eventually(*p.a, sender, "undelivered:bounded:not_found"));
    auto report = rt::undelivered_report(failed, abi::quota);
    REQUIRE(report.has_value());
    for (int mutation = 0; mutation < 8; ++mutation) {
        auto bad = *report;
        switch (mutation) {
            case 0: bad.kind = rt::RemoteMessage::Kind::message; break;
            case 1: {
                paglets::msgpack::Writer missing;
                missing.write_map_header(2);
                missing.write_str("name"); missing.write_str("bounded");
                missing.write_str("status"); missing.write_int(abi::quota);
                bad.payload = missing.take();
                break;
            }
            case 2: bad.payload = enc(abi::Undelivered{"bounded", 72, abi::ok}); break;
            case 3: bad.name = "paglets.recovered"; break;
            case 4: bad.caps.emplace_back(); break;
            case 5: bad.sender = abi::SenderRecord{}; break;
            case 6: bad.failure = failed.failure; break;
            case 7: bad.payload = text("not msgpack"); break;
        }
        CHECK_EQ(p.a->runtime->deliver_remote(std::move(bad)), abi::invalid_argument);
    }
    report->target = "gone-sender";
    report->hops = 4;
    CHECK_EQ(p.a->runtime->deliver_remote(*report), abi::not_found);
    report->hops = -1;
    CHECK_EQ(p.a->runtime->deliver_remote(*report), abi::invalid_argument);
    CHECK(!rt::decode_remote(rt::encode_remote(*report)));
    CHECK(!contains(p.a->journal(sender), "undelivered:bounded:quota"));
}

PAGLETS_TEST("remote undelivered: a receiving paglet carries the failure route through its move") {
    Fixture source, receiver;
    rt::Config config;
    config.mailbox_limit = 0;
    Fixture destination(config);
    std::mutex mu;
    std::vector<rt::RemoteMessage> frames;
    std::optional<rt::Departure> departure;
    auto install = [&](Fixture& f, const std::string& name, bool moving = false) {
        rt::MobilityHooks hooks;
        hooks.host_id = name;
        hooks.deliver = [&](rt::RemoteMessage m) { std::lock_guard lock(mu); frames.push_back(std::move(m)); };
        if (moving) hooks.depart = [&](rt::Departure d) { std::lock_guard lock(mu); departure = std::move(d); };
        f.runtime->set_mobility(std::move(hooks));
    };
    auto take = [&] { std::lock_guard lock(mu); return std::exchange(frames, {}); };
    auto deliver = [&](Fixture& f, rt::RemoteMessage m) {
        auto wire = rt::decode_remote(rt::encode_remote(m));
        REQUIRE_OK(wire);
        return f.runtime->deliver_remote(std::move(*wire));
    };
    install(source, "source");
    install(receiver, "receiver", true);
    install(destination, "destination");
    const auto sender = source.create("conformance.wasm");
    const auto target = receiver.create("conformance.wasm");
    const auto handle = endpoint_to(source, sender, target, "receiver");
    REQUIRE_OK(receiver.runtime->dispatch(target, "destination"));
    REQUIRE(eventually([&] { std::lock_guard lock(mu); return departure.has_value(); }));
    CHECK_EQ(source.code(sender, "send", Cmd{.handle = handle, .name = "held-refusal", .async = true}), 0);
    auto outgoing = take();
    REQUIRE(outgoing.size() == 1);
    REQUIRE(deliver(receiver, std::move(outgoing.front())) == abi::ok);
    REQUIRE(receiver.runtime->info(target)->mailbox == 1);
    rt::Departure d;
    { std::lock_guard lock(mu); d = std::move(*departure); }
    const auto module = receiver.runtime->modules().bytes(d.state.module);
    REQUIRE(module.has_value());
    REQUIRE_OK(destination.runtime->add_module(*module));
    rt::Arrival arrival{std::move(d.state), std::move(d.image), "receiver", d.clone,
                        std::move(d.clone_args), std::move(d.clone_caps), d.original};
    REQUIRE_OK(destination.runtime->prepare_arrival(std::move(arrival), {}));
    REQUIRE_OK(destination.runtime->commit_arrival(target));
    receiver.runtime->finish_departure(d.move, "destination");
    auto forwarded = take();
    REQUIRE(forwarded.size() == 1);
    REQUIRE(forwarded.front().failure.has_value());
    CHECK_EQ(forwarded.front().failure->handle, handle);
    CHECK_EQ(deliver(destination, std::move(forwarded.front())), abi::quota);
    auto reports = take();
    REQUIRE(reports.size() == 1);
    REQUIRE(deliver(source, std::move(reports.front())) == abi::ok);
    CHECK(journal_eventually(source, sender, "undelivered:held-refusal:quota"));
}

PAGLETS_TEST("remote undelivered: legacy messages omit refusal routes and malformed routes are refused") {
    paglets::msgpack::Writer w;
    w.write_map_header(3);
    w.write_str("kind"); w.write_int(0);
    w.write_str("target"); w.write_str("legacy-target");
    w.write_str("name"); w.write_str("legacy-message");
    auto legacy = rt::decode_remote(w.bytes());
    REQUIRE_OK(legacy);
    CHECK(!legacy->failure);
    CHECK(!rt::undelivered_report(*legacy, abi::not_found));
    rt::RemoteMessage m;
    m.failure = rt::RemoteMessage::FailureRoute{"sender", "host", 0};
    CHECK(!rt::decode_remote(rt::encode_remote(m)));
    m.failure->handle = 1;
    m.kind = rt::RemoteMessage::Kind::request;
    CHECK(!rt::decode_remote(rt::encode_remote(m)));
}

PAGLETS_TEST("remote undelivered: self-directed failures and timeouts follow the moving paglet") {
    for (int mode = 0; mode < 3; ++mode) {
        Pair p;
        if (std::getenv("PAGLETS_TEST_TRACE")) std::cerr << "[self-mode] " << mode << "\n";
        const auto sender = p.a->create("conformance.wasm");
        rt::Cap resource;
        resource.kind = rt::Cap::Kind::resource;
        resource.target = "system.local-resource";
        resource.resource_type = "dir";
        resource.resource = "root";
        resource.transferable = true;
        const auto local_cap = p.a->runtime->add_capability(sender, std::move(resource));
        REQUIRE_OK(local_cap);
        if (mode == 2) { std::lock_guard lock(p.mu); p.hold = true; }
        CHECK_EQ(p.a->code(sender, "self_queue_then_move", Cmd{.name = "b", .caps = {*local_cap}, .code = mode ? 1 : 0, .ms = mode == 2 ? 1000 : 0}), 0);
        if (mode == 2) {
            REQUIRE(eventually([&] { return p.departures.load() == 1; }));
            REQUIRE(p.snapshot_pending.load() == 1);
            std::this_thread::sleep_for(1100ms);
            p.release();
        }
        REQUIRE(eventually([&] { return p.moves.load() == 1; }));
        const auto event = mode == 0 ? "undelivered:held-self:unsupported" : mode == 1 ? "self-failure:unsupported" : "self-failure:timeout";
        const bool received = journal_eventually(*p.b, sender, event);
        CHECK(received);
        const auto journal = p.b->journal(sender);
        if (!received) {
            std::cerr << "[self-state] mode=" << mode << " pending=" << p.snapshot_pending.load() << "\n";
            for (const auto& frame : p.frames()) std::cerr << "[self-frame] kind=" << static_cast<int>(frame.kind) << " name=" << frame.name << " target=" << frame.target << " host=" << frame.host << " correlation=" << frame.correlation << " status=" << frame.status << "\n";
            for (const auto& entry : journal) std::cerr << "[self-journal] " << entry << "\n";
        }
        CHECK_EQ(std::ranges::count(journal, std::string(event)), 1);
    }
}

PAGLETS_TEST("requester deadlines: time spent moving is deducted and a late reply stays late") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    REQUIRE(p.a->code(sender, "request", Cmd{.handle = endpoint, .name = "defer", .ms = 2000}) > 0);
    REQUIRE(journal_eventually(*p.b, receiver, "msg:defer"));
    {
        std::lock_guard lock(p.mu);
        p.hold = true;
    }
    REQUIRE_OK(p.a->runtime->dispatch(sender, "b"));
    REQUIRE(eventually([&] { return p.departures.load() == 1; }));
    REQUIRE(p.snapshot_pending.load() == 1);
    std::this_thread::sleep_for(1000ms);  // less than the original request timeout
    p.release();
    REQUIRE(eventually([&] { return p.moves.load() == 1; }));
    // The destination has about one second left, not another full two.
    CHECK(journal_eventually(*p.b, sender, "reply:timeout:", 1500ms));
    CHECK_EQ(p.b->code(receiver, "answer", Cmd{.payload = text("too-late")}), 0);
    std::this_thread::sleep_for(50ms);
    const auto journal = p.b->journal(sender);
    CHECK_EQ(std::ranges::count(journal, std::string("reply:timeout:")), 1);
    CHECK(!contains(journal, "reply:ok:too-late"));
}

PAGLETS_TEST("requester deadlines: repeated moves retain the original timeout") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    REQUIRE(p.a->code(sender, "request", Cmd{.handle = endpoint, .name = "defer", .ms = 3000}) > 0);
    REQUIRE(journal_eventually(*p.b, receiver, "msg:defer"));
    for (int hop = 0; hop < 2; ++hop) {
        {
            std::lock_guard lock(p.mu);
            p.hold = true;
        }
        auto& source = hop == 0 ? *p.a : *p.b;
        REQUIRE_OK(source.runtime->dispatch(sender, hop == 0 ? "b" : "a"));
        REQUIRE(eventually([&] { return p.departures.load() == hop + 1; }));
        REQUIRE(p.snapshot_pending.load() == 1);
        std::this_thread::sleep_for(800ms);
        p.release();
        REQUIRE(eventually([&] { return p.moves.load() == hop + 1; }));
    }
    CHECK(journal_eventually(*p.a, sender, "reply:timeout:", 1800ms));
    const auto journal = p.a->journal(sender);
    CHECK_EQ(std::ranges::count(journal, std::string("reply:timeout:")), 1);
}

PAGLETS_TEST("requester deadlines: expiry in transit produces one timeout after arrival") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    REQUIRE(p.a->code(sender, "request", Cmd{.handle = endpoint, .name = "defer", .ms = 300}) > 0);
    {
        std::lock_guard lock(p.mu);
        p.hold = true;
    }
    REQUIRE_OK(p.a->runtime->dispatch(sender, "b"));
    REQUIRE(eventually([&] { return p.departures.load() == 1; }));
    REQUIRE(p.snapshot_pending.load() == 1);
    std::this_thread::sleep_for(500ms);
    p.release();
    REQUIRE(eventually([&] { return p.moves.load() == 1; }));
    REQUIRE(journal_eventually(*p.b, sender, "reply:timeout:", 300ms));
    std::this_thread::sleep_for(100ms);
    const auto journal = p.b->journal(sender);
    CHECK_EQ(std::ranges::count(journal, std::string("reply:timeout:")), 1);
}

PAGLETS_TEST("requester deadlines: rollback leaves the source timeout running") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    REQUIRE(p.a->code(sender, "request", Cmd{.handle = endpoint, .name = "defer", .ms = 1000}) > 0);
    {
        std::lock_guard lock(p.mu);
        p.hold = true;
    }
    REQUIRE_OK(p.a->runtime->dispatch(sender, "missing"));
    REQUIRE(eventually([&] { return p.departures.load() == 1; }));
    std::this_thread::sleep_for(400ms);
    p.release();
    REQUIRE(journal_eventually(*p.a, sender, "move_failed:missing:unknown destination:", 300ms));
    CHECK(journal_eventually(*p.a, sender, "reply:timeout:", 900ms));
    const auto journal = p.a->journal(sender);
    CHECK_EQ(std::ranges::count(journal, std::string("reply:timeout:")), 1);
    CHECK_EQ(p.moves.load(), 0);
}

PAGLETS_TEST("requester deadlines: version3 timing metadata cannot be hidden from old readers") {
    rt::TravelState state;
    state.id = std::string(32, 'a');
    state.module = std::string(64, 'b');
    state.pending = {{1, 250}};
    state.next_correlation = 2;
    state.pending_captured_ms = 123456789;
    auto encoded = rt::encode_state(state);
    auto back = rt::decode_state(encoded);
    REQUIRE_OK(back);
    CHECK_EQ(back->pending_captured_ms, state.pending_captured_ms);
    CHECK(back->pending == state.pending);
    std::int32_t version = 0;
    paglets::msgpack::Reader reader(encoded);
    REQUIRE(abi::detail::read_map(reader, [&](std::string_view key) {
        return key == "v" ? paglets::msgpack::read_value(reader, version) : reader.skip();
    }));
    CHECK_EQ(version, 3);  // old readers accept only1/2 and refuse this state
    state.pending_captured_ms = 0;
    REQUIRE_OK(rt::decode_state(rt::encode_state(state)));  // legacy remaining-time state
    state.pending_captured_ms = std::numeric_limits<std::int64_t>::max();
    REQUIRE_OK(rt::decode_state(rt::encode_state(state)));  // parsed without timestamp arithmetic
    state.pending.clear();
    back = rt::decode_state(rt::encode_state(state));
    REQUIRE_OK(back);
    CHECK_EQ(back->pending_captured_ms, 0);  // clones cannot inherit timing metadata
    auto wire = [&](int v, bool captured, std::int64_t time, bool duplicate = false) {
        paglets::msgpack::Writer writer;
        writer.write_map_header(4 + (captured ? 1 : 0) + (duplicate ? 1 : 0));
        writer.write_str("v"); writer.write_int(v);
        writer.write_str("id"); writer.write_str(state.id);
        writer.write_str("module"); writer.write_str(state.module);
        writer.write_str("ready"); writer.write_array_header(0);
        if (captured) { writer.write_str("pending_captured_ms"); writer.write_int(time); }
        if (duplicate) { writer.write_str("pending_captured_ms"); writer.write_int(time); }
        return rt::decode_state(writer.bytes());
    };
    CHECK(wire(1, false, 0).has_value());
    CHECK(wire(2, false, 0).has_value());
    CHECK(!wire(1, true, 123).has_value());
    CHECK(!wire(2, true, 123).has_value());
    CHECK(!wire(3, false, 0).has_value());
    CHECK(!wire(3, true, 0).has_value());
    CHECK(!wire(3, true, -1).has_value());
    CHECK(!wire(3, true, 123, true).has_value());
    CHECK(wire(3, true, 123).has_value());
}

PAGLETS_TEST("runtime safety: transferring the used endpoint preserves routing badge and remaining uses") {
    for (const bool remote : {false, true}) for (int mode = 0; mode < 3; ++mode) {
        Pair p;
        auto& from = *p.a;
        auto& to = remote ? *p.b : *p.a;
        const auto sender = from.create("conformance.wasm");
        const auto receiver = to.create("conformance.wasm");
        const auto before = dec<std::vector<std::int32_t>>(to.call(receiver, "caps").payload);
        rt::Cap endpoint;
        endpoint.kind = rt::Cap::Kind::endpoint;
        endpoint.target = receiver;
        endpoint.host = remote ? "b" : "";
        endpoint.ops = {"*"};
        endpoint.badge = "used-endpoint";
        endpoint.uses_left = 2;
        endpoint.transferable = true;
        auto handle = from.runtime->add_capability(sender, std::move(endpoint));
        REQUIRE_OK(handle);
        const auto result = from.code(sender, mode == 2 ? "request" : "send",
            Cmd{.handle = *handle, .name = "take_badged", .caps = {*handle}, .async = mode == 1});
        CHECK(mode == 2 ? result > 0 : result == 0);
        REQUIRE(journal_eventually(to, receiver, "badged:used-endpoint"));
        if (mode == 2) CHECK(journal_eventually(from, sender, "reply:ok:used-endpoint"));
        CHECK_EQ(from.cmd(sender, "inspect", Cmd{.handle = *handle}).status, abi::bad_handle);
        const auto after = dec<std::vector<std::int32_t>>(to.call(receiver, "caps").payload);
        std::vector<std::int32_t> received;
        for (auto h : after) if (std::ranges::find(before, h) == before.end()) received.push_back(h);
        REQUIRE(received.size() == 1);
        const auto info = dec<abi::CapInfo>(to.cmd(receiver, "inspect", Cmd{.handle = received.front()}).payload);
        CHECK(info.uses_left == 1);
        CHECK(info.badge == "used-endpoint");
        CHECK_EQ(to.code(receiver, "send", Cmd{.handle = received.front(), .name = "echo"}), 0);
        CHECK_EQ(to.code(receiver, "send", Cmd{.handle = received.front(), .name = "echo"}), abi::quota);
    }
}

PAGLETS_TEST("runtime safety: a reply cannot consume and transfer its own capability") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "reply_self_transfer", Cmd{}), abi::invalid_argument);
    auto failed = f.call(id, "reply_self_transfer", enc(Cmd{.async = true}));
    CHECK_EQ(failed.status, abi::gone);
    CHECK(journal_eventually(f, id, "undelivered:reply:invalid_argument"));
}

PAGLETS_TEST("runtime safety: timers retain their kind and fire once through successful and failed moves") {
    for (int mode = 0; mode < 3; ++mode) {
        Pair p;
        const auto id = p.a->create("conformance.wasm");
        std::string name;
        if (mode == 1) {
            name = "timer-case-queued";
            CHECK_EQ(p.a->code(id, "pause_timer_then_move", Cmd{.name = "b", .ms = 500}), 0);
            REQUIRE(eventually([&] { return p.moves.load() == 1; }));
            REQUIRE(p.snapshot_timers.load() == 0);
        } else {
            name = "timer-case-transit";
            { std::lock_guard lock(p.mu); p.hold = true; }
            CHECK(p.a->code(id, "timer", Cmd{.name = name, .ms = 300}) > 0);
            REQUIRE_OK(p.a->runtime->dispatch(id, mode == 2 ? "missing-host" : "b"));
            REQUIRE(eventually([&] { return p.departures.load() == 1; }));
            REQUIRE(p.snapshot_timers.load() == 1);
            std::this_thread::sleep_for(400ms);
            p.release();
            if (mode == 0) REQUIRE(eventually([&] { return p.moves.load() == 1; }));
        }
        auto& dest = mode == 2 ? *p.a : *p.b;
        REQUIRE(journal_eventually(dest, id, "timer:" + name));
        std::this_thread::sleep_for(50ms);
        const auto journal = dest.journal(id);
        CHECK_EQ(std::ranges::count(journal, "timer:" + name), 1);
        CHECK(!contains(journal, "wrong-timer-kind:" + name));
    }
}

PAGLETS_TEST("runtime safety: a reply queued before capture survives the move with its payload and capability") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    CHECK_EQ(p.a->code(sender, "pause_request_then_move", Cmd{.handle = endpoint, .name = "b", .payload = text("before-capture"), .ms = 500}), 0);
    REQUIRE(eventually([&] { return p.moves.load() == 1; }));
    REQUIRE(p.snapshot_pending.load() == 0);  // the host accepted the reply before capture
    CHECK(journal_eventually(*p.b, sender, "moved-reply:ok:before-capture:1"));
    CHECK(journal_eventually(*p.b, sender, "moved-cap:paglet:" + receiver));
    const auto journal = p.b->journal(sender);
    CHECK_EQ(std::ranges::count(journal, std::string("moved-reply:ok:before-capture:1")), 1);
}

PAGLETS_TEST("runtime safety: queued timeout and completed job continuations survive a move") {
    for (const bool job : {false, true}) {
        Pair p;
        const auto sender = p.a->create("conformance.wasm");
        const auto receiver = p.b->create("conformance.wasm");
        const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
        p.before_arrival = [](rt::Arrival& arrival) {
            REQUIRE(arrival.state.ready.size() == 1);
            CHECK(arrival.state.ready.front().delivered.kind == abi::MessageKind::reply);
        };
        CHECK_EQ(p.a->code(sender, job ? "pause_job_then_move" : "pause_request_then_move",
                          Cmd{.handle = endpoint, .name = "b", .payload = text("job-result"), .code = 1, .ms = 1000}), 0);
        REQUIRE(eventually([&] { return p.moves.load() == 1; }));
        CHECK_EQ(p.snapshot_pending.load(), 0);
        const auto expected = job ? "moved-job:job-result" : "moved-reply:timeout::0";
        REQUIRE(journal_eventually(*p.b, sender, expected));
        const auto journal = p.b->journal(sender);
        CHECK_EQ(std::ranges::count(journal, std::string(expected)), 1);
    }
}

PAGLETS_TEST("runtime safety: failed transfer validation does not consume endpoint uses") {
    Fixture f;
    const auto sender = f.create("conformance.wasm");
    const auto receiver = f.create("conformance.wasm");
    rt::Cap endpoint;
    endpoint.kind = rt::Cap::Kind::endpoint;
    endpoint.target = receiver;
    endpoint.ops = {"*"};
    endpoint.uses_left = 2;
    endpoint.transferable = true;
    auto h = f.runtime->add_capability(sender, std::move(endpoint));
    REQUIRE_OK(h);
    CHECK_EQ(f.code(sender, "send", Cmd{.handle = *h, .name = "echo", .caps = {9999}}), abi::bad_handle);
    const auto info = dec<abi::CapInfo>(f.cmd(sender, "inspect", Cmd{.handle = *h}).payload);
    CHECK(info.uses_left == 2);
    CHECK_EQ(f.code(sender, "send", Cmd{.handle = *h, .name = "echo"}), 0);
    CHECK_EQ(f.code(sender, "send", Cmd{.handle = *h, .name = "echo"}), 0);
    CHECK_EQ(f.code(sender, "send", Cmd{.handle = *h, .name = "echo"}), abi::quota);
}

PAGLETS_TEST("runtime safety: a move budget failure keeps the queued reply at the source") {
    rt::Config config;
    config.move_ready_bytes = 0;
    Pair p(config, {});
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    CHECK_EQ(p.a->code(sender, "pause_request_then_move", Cmd{.handle = endpoint, .name = "b", .payload = text("kept"), .ms = 500}), 0);
    CHECK(journal_eventually(*p.a, sender, "moved-reply:ok:kept:1"));
    CHECK(journal_eventually(*p.a, sender, "move_failed:b:ready notifications exceed the move byte budget:"));
    CHECK_EQ(p.moves.load(), 0);
    CHECK(p.a->runtime->info(sender).has_value());
}

PAGLETS_TEST("runtime safety: a queued moved reply remains outstanding in a restart image") {
    struct Temp {
        std::filesystem::path path = std::filesystem::temp_directory_path() / ("paglets-ready-restart-" + std::to_string(std::random_device{}()));
        ~Temp() { std::error_code ec; std::filesystem::remove_all(path, ec); }
    } temp;
    rt::Config config;
    config.state_dir = temp.path;
    config.checkpoint_interval = std::chrono::hours(24);
    Pair p({}, config);
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    CHECK_EQ(p.a->code(sender, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::restart), .ms = 1, .async = true}), 0);
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    CHECK_EQ(p.a->code(sender, "pause_request_then_move", Cmd{.handle = endpoint, .name = "b", .payload = text("checkpointed"), .ms = 500}), 0);
    REQUIRE(eventually([&] { return p.moves.load() == 1; }));
    REQUIRE(journal_eventually(*p.b, sender, "moved-reply:ok:checkpointed:1"));
    p.b->runtime->shutdown(false);
    rt::Store store(temp.path);
    const auto rows = store.load_paglets([](const std::string&) {});
    const auto row = std::ranges::find_if(rows, [&](const auto& entry) { return entry.id == sender; });
    REQUIRE(row != rows.end());
    REQUIRE(row->pending_requests.size() == 1);
    Fixture restarted(config);
    REQUIRE(journal_eventually(restarted, sender, "moved-reply:timeout::0"));
    const auto journal = restarted.journal(sender);
    CHECK(index_of(journal, "event:recovered:ok") < index_of(journal, "moved-reply:timeout::0"));
    CHECK_EQ(std::ranges::count(journal, std::string("moved-reply:timeout::0")), 1);
}

PAGLETS_TEST("runtime safety: ready state v2 rejects malformed notification authority before arrival") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.b->create("conformance.wasm");
    const auto endpoint = endpoint_to(*p.a, sender, receiver, "b");
    p.before_arrival = [](rt::Arrival& arrival) {
        REQUIRE(arrival.state.ready.size() == 1);
        arrival.state.ready.front().delivered.sender = abi::SenderRecord{"forged", "owner", "system", "", ""};
    };
    CHECK_EQ(p.a->code(sender, "pause_request_then_move", Cmd{.handle = endpoint, .name = "b", .payload = text("original"), .ms = 500}), 0);
    CHECK(journal_eventually(*p.a, sender, "moved-reply:ok:original:1"));
    CHECK(p.a->logged("invalid arrival ready notification authority"));
    CHECK_EQ(p.moves.load(), 0);
    CHECK(!p.b->runtime->info(sender));
}

PAGLETS_TEST("abi C37: messages wait for a moving paglet and follow it") {
    Pair p;
    auto id = p.a->create("conformance.wasm");
    CHECK_EQ(count(*p.a, id), 1);
    {
        std::lock_guard lock(p.mu);
        p.hold = true;
    }
    REQUIRE(p.a->runtime->dispatch(id, "b").has_value());
    REQUIRE(eventually([&] {
        auto j = p.a->runtime->info(id);
        return j && j->state == rt::PagletState::inactive && j->lane < 0;
    }));
    // While it moves, messages wait here, unhandled.
    REQUIRE(p.a->runtime->send(id, "note1").has_value());
    auto pending = p.a->runtime->request(id, "count");
    std::this_thread::sleep_for(50ms);
    CHECK_EQ(p.a->runtime->info(id)->mailbox, 2u);
    p.release();
    // After the move they are handled where it went; the request is answered.
    auto r = pending.get();
    CHECK_EQ(r.status, 0);
    CHECK_EQ(dec<std::int64_t>(r.payload), 2);
    CHECK(journal_eventually(*p.b, id, "msg:note1"));
}

PAGLETS_TEST("abi C38: a failed move leaves the paglet where it was") {
    Pair p;
    auto id = p.a->create("conformance.wasm");
    CHECK_EQ(count(*p.a, id), 1);
    CHECK_EQ(p.a->code(id, "lifecycle",
                       Cmd{.name = "nowhere", .code = static_cast<std::int32_t>(abi::LifecycleOp::dispatch)}),
             0);
    CHECK(journal_eventually(*p.a, id, "move_failed:nowhere:unknown destination:"));
    CHECK_EQ(count(*p.a, id), 2);  // state kept, still here
    // A destination that refuses the arrival (its admission check).
    p.b->runtime->set_module_admission([](const std::string&, rt::TrustClass) -> std::expected<void, std::string> {
        return std::unexpected(std::string("not on this host"));
    });
    REQUIRE(p.a->runtime->dispatch(id, "b").has_value());
    CHECK(journal_eventually(*p.a, id, "move_failed:b:not on this host:"));
    CHECK(!p.b->runtime->info(id).has_value());
    CHECK_EQ(count(*p.a, id), 3);
    // Paglets without mobility hooks cannot dispatch; system paglets and
    // resident paglets never move.
    Fixture alone;
    auto still = alone.create("conformance.wasm");
    CHECK_EQ(
        alone.code(still, "lifecycle", Cmd{.name = "b", .code = static_cast<std::int32_t>(abi::LifecycleOp::dispatch)}),
        static_cast<std::int64_t>(abi::unsupported));
    auto resident =
        p.a->runtime->create(p.a->module("conformance.wasm"), rt::CreateOptions{{}, rt::TrustClass::resident});
    REQUIRE_OK(resident);
    CHECK_EQ(p.a->runtime->dispatch(*resident, "b").error(), static_cast<std::int32_t>(abi::denied));
}

PAGLETS_TEST("abi C39: clones for another host") {
    Pair p;
    auto original = p.a->create("conformance.wasm");
    CHECK_EQ(count(*p.a, original), 1);
    const auto h =
        static_cast<std::int32_t>(p.a->code(original, "lifecycle",
                                            Cmd{.handle = 0,
                                                .name = "b",
                                                .payload = text("twin"),
                                                .code = static_cast<std::int32_t>(abi::LifecycleOp::clone)}));
    REQUIRE(h > 1);
    const auto clone = p.a->target_of(original, h);
    REQUIRE(eventually([&] { return p.b->runtime->info(clone).has_value(); }));
    CHECK(contains(p.b->journal(clone), "event:cloned:twin"));
    CHECK_EQ(count(*p.b, clone), 2);     // the clone starts with the original's memory
    CHECK_EQ(count(*p.a, original), 2);  // the original stays
    // The original reaches its clone through the endpoint it got.
    CHECK(p.a->code(original, "request", Cmd{.handle = h, .name = "echo", .payload = text("hi")}) > 0);
    CHECK(journal_eventually(*p.a, original, "reply:ok:hi"));
    // A clone that cannot be made is reported to the original.
    const auto h2 = static_cast<std::int32_t>(p.a->code(
        original, "lifecycle", Cmd{.name = "nowhere", .code = static_cast<std::int32_t>(abi::LifecycleOp::clone)}));
    REQUIRE(h2 > 1);
    const auto missing = p.a->target_of(original, h2);
    CHECK(journal_eventually(*p.a, original, "move_failed:nowhere:unknown destination:" + missing));
}

PAGLETS_TEST("mobility: travelling state round trip") {
    rt::TravelState s;
    s.id = std::string(32, 'a');
    s.module = std::string(64, 'b');
    s.trust = "roaming";
    s.owner = "o";
    s.next_handle = 9;
    s.next_correlation = 5;
    s.next_timer = 3;
    s.checkpoint_ms = 250;
    s.services = {{"files", 2}};
    rt::Cap c;
    c.kind = rt::Cap::Kind::endpoint;
    c.target = std::string(32, 'c');
    c.ops = {"echo"};
    c.host = "host-b";
    s.caps = {{2, c}};
    s.pending = {{4, 1500}};
    s.marks = {"root:clinic"};
    auto back = rt::decode_state(rt::encode_state(s));
    REQUIRE_OK(back);
    CHECK_EQ(back->id, s.id);
    CHECK_EQ(back->next_handle, 9);
    CHECK_EQ(back->checkpoint_ms, 250);
    CHECK(back->services == s.services);
    REQUIRE(back->caps.size() == 1u);
    CHECK_EQ(back->caps[0].second.host, std::string("host-b"));
    CHECK(back->pending == s.pending);
    CHECK(back->marks == s.marks);
    rt::ReadyMessage ready;
    ready.delivered.kind = abi::MessageKind::reply;
    ready.delivered.name = "reply";
    ready.delivered.correlation = 3;
    ready.delivered.payload = text("queued");
    ready.caps = {c};
    ready.residency_marks = s.marks;
    s.ready = {ready};
    back = rt::decode_state(rt::encode_state(s));
    REQUIRE_OK(back);
    REQUIRE(back->ready.size() == 1u);
    CHECK(back->ready.front().delivered.payload == ready.delivered.payload);
    CHECK(back->ready.front().residency_marks == ready.residency_marks);
    REQUIRE(back->ready.front().caps.size() == 1u);
    CHECK_EQ(back->ready.front().caps.front().host, c.host);
    // A v1 document may omit ready, but may not disguise a nonempty v2
    // notification list. V2 must explicitly carry that list.
    auto state = [&](int version, bool include_ready) {
        paglets::msgpack::Writer writer;
        writer.write_map_header(include_ready ? 4 : 3);
        writer.write_str("v"); writer.write_int(version);
        writer.write_str("id"); writer.write_str(s.id);
        writer.write_str("module"); writer.write_str(s.module);
        if (include_ready) {
            writer.write_str("ready"); writer.write_array_header(1);
            paglets::msgpack::write_value(writer, rt::encode_ready(ready));
        }
        return rt::decode_state(writer.bytes());
    };
    CHECK(state(1, false).has_value());
    CHECK(!state(1, true).has_value());
    CHECK(!state(2, false).has_value());
    CHECK(state(2, true).has_value());
    CHECK(!rt::decode_state(Bytes{1, 2, 3}).has_value());
}

PAGLETS_TEST("remaining: a held request cannot silently lose a nonportable capability") {
    Pair p;
    const auto sender = p.a->create("conformance.wasm");
    const auto receiver = p.a->create("conformance.wasm");
    rt::Cap endpoint;
    endpoint.kind = rt::Cap::Kind::endpoint;
    endpoint.target = receiver;
    endpoint.ops = {"*"};
    endpoint.transferable = true;
    const auto to = p.a->runtime->add_capability(sender, endpoint);
    REQUIRE_OK(to);
    rt::Cap resource;
    resource.kind = rt::Cap::Kind::resource;
    resource.target = "system.local-resource";
    resource.resource_type = "dir";
    resource.resource = "root";
    resource.transferable = true;
    const auto cap = p.a->runtime->add_capability(sender, resource);
    REQUIRE_OK(cap);
    {
        std::lock_guard lock(p.mu);
        p.hold = true;
    }
    REQUIRE(p.a->runtime->dispatch(receiver, "b"));
    REQUIRE(eventually([&] { auto info = p.a->runtime->info(receiver); return info && info->state == rt::PagletState::inactive && info->lane < 0; }));
    REQUIRE(p.a->code(sender, "request", Cmd{.handle = *to, .name = "count", .caps = {*cap}}) > 0);
    p.release();
    CHECK(journal_eventually(*p.a, sender, "reply:unsupported:"));
    REQUIRE(eventually([&] { return p.moves.load() == 1; }));
    CHECK_EQ(dec<std::int64_t>(p.b->call(receiver, "count").payload), 1);
}

namespace {

constexpr std::array<std::pair<abi::LifecycleOp, std::string_view>, 3> end_hooks{{
    {abi::LifecycleOp::dispatch, "dispatch"},
    {abi::LifecycleOp::deactivate, "deactivate"},
    {abi::LifecycleOp::dispose, "dispose"},
}};

void configure_end_hook(Fixture& f, const rt::PagletId& id, std::string_view name, int mode) {
    REQUIRE(f.cmd(id, "end_hook", Cmd{.name = std::string(name), .code = mode}).status == 0);
}

void run_end_hook(Fixture& f, const rt::PagletId& id, abi::LifecycleOp operation) {
    CHECK_EQ(f.code(id, "lifecycle", Cmd{.name = "b", .code = static_cast<std::int32_t>(operation)}), 0);
}

}  // namespace

PAGLETS_TEST("lifecycle: traps and budget expiry in every end hook fail the paglet without departure") {
    for (auto [operation, name] : end_hooks) {
        for (int mode : {1, 2}) {
            rt::Config config;
            config.handler_budget = 200ms;
            Fixture f(config);
            std::atomic<int> departures{0};
            rt::MobilityHooks hooks;
            hooks.host_id = "a";
            hooks.depart = [&](rt::Departure) { ++departures; };
            f.runtime->set_mobility(std::move(hooks));
            const auto id = f.create("conformance.wasm");
            configure_end_hook(f, id, name, mode);
            run_end_hook(f, id, operation);
            REQUIRE(eventually([&] { return f.runtime->ending(id).has_value(); }));
            const auto end = f.runtime->ending(id);
            REQUIRE(end->failed);
            if (mode == 2) CHECK(end->reason.find("time budget") != std::string::npos);
            CHECK(!f.runtime->info(id));
            CHECK_EQ(departures.load(), 0);
            CHECK_EQ(f.call(id, "count").status, static_cast<std::int32_t>(abi::not_found));
        }
    }
}

PAGLETS_TEST("lifecycle: operator termination during every end hook fails it before publishing an image") {
    for (auto [operation, name] : end_hooks) {
        rt::Config config;
        config.handler_budget = 10s;
        Fixture f(config);
        std::atomic<int> departures{0};
        rt::MobilityHooks hooks;
        hooks.host_id = "a";
        hooks.depart = [&](rt::Departure) { ++departures; };
        f.runtime->set_mobility(std::move(hooks));
        const auto id = f.create("conformance.wasm");
        configure_end_hook(f, id, name, 2);
        run_end_hook(f, id, operation);
        REQUIRE(eventually([&] { return f.logged_exactly("end-hook:" + std::string(name) + ":entered"); }));
        if (operation == abi::LifecycleOp::dispatch) {
            CHECK_EQ(f.runtime->dispose(id).error(), static_cast<std::int32_t>(abi::bad_state));
            CHECK_EQ(f.runtime->deactivate(id).error(), static_cast<std::int32_t>(abi::bad_state));
            const auto until = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count() + 10000;
            CHECK_EQ(f.runtime->pin(id, "late", until).error(), static_cast<std::int32_t>(abi::bad_state));
        }
        REQUIRE(f.runtime->terminate(id, "operator stopped the hook"));
        REQUIRE(eventually([&] { return f.runtime->ending(id).has_value(); }));
        REQUIRE(f.runtime->ending(id)->failed);
        CHECK_EQ(f.runtime->ending(id)->reason, std::string("operator stopped the hook"));
        CHECK_EQ(departures.load(), 0);
    }
}

PAGLETS_TEST("lifecycle: end hooks refuse nested transitions and paglet or job spawns without leaks") {
    for (auto [operation, name] : end_hooks) {
        Pair p;
        const auto id = p.a->create("conformance.wasm");
        configure_end_hook(*p.a, id, name, 3);
        run_end_hook(*p.a, id, operation);
        REQUIRE(eventually([&] {
            return p.a->logged_exactly("end-hook:" + std::string(name) + ":job:" + std::to_string(abi::bad_state));
        }));
        for (std::string_view nested : {"dispose", "deactivate", "dispatch", "checkpoint", "clone", "child", "job"}) {
            CHECK(p.a->logged_exactly("end-hook:" + std::string(name) + ":" + std::string(nested) + ":" +
                                      std::to_string(abi::bad_state)));
        }
        if (operation == abi::LifecycleOp::dispatch) {
            REQUIRE(eventually([&] { return p.b->runtime->info(id).has_value() && !p.a->runtime->info(id); }));
            CHECK_EQ(p.a->runtime->list().size(), std::size_t{0});
            CHECK_EQ(p.b->runtime->list().size(), std::size_t{1});
        } else if (operation == abi::LifecycleOp::dispose) {
            REQUIRE(eventually([&] { return p.a->runtime->ending(id).has_value(); }));
            CHECK(!p.a->runtime->ending(id)->failed);
            CHECK_EQ(p.a->runtime->list().size(), std::size_t{0});
        } else {
            p.a->runtime->wait_idle();
            CHECK_EQ(p.a->runtime->list().size(), std::size_t{1});
            CHECK(p.a->runtime->info(id)->state == rt::PagletState::inactive);
        }
        CHECK(p.a->runtime->jobs().empty());
    }
}

PAGLETS_TEST("lifecycle: a failed arrival image write ends the paglet and reports a failed receipt") {
    const auto dir = std::filesystem::temp_directory_path() /
        ("paglets-arrival-write-" + std::to_string(std::random_device{}()));
    {
        Fixture source;
        rt::Config config;
        config.state_dir = dir;
        Fixture target(config);
        std::promise<rt::Departure> promise;
        auto departure = promise.get_future();
        rt::MobilityHooks hooks;
        hooks.host_id = "a";
        hooks.depart = [&](rt::Departure d) { promise.set_value(std::move(d)); };
        source.runtime->set_mobility(std::move(hooks));
        const auto id = source.create("conformance.wasm");
        target.module("conformance.wasm");
        REQUIRE(source.runtime->dispatch(id, "b"));
        REQUIRE(departure.wait_for(10s) == std::future_status::ready);
        auto d = departure.get();
        rt::Arrival arrival{std::move(d.state), std::move(d.image), "a", false, {}, {}, {}};
        arrival.request = 23;
        std::promise<bool> receipt;
        auto received = receipt.get_future();
        rt::MobilityHooks destination;
        destination.arrived = [&](const std::string& from, std::uint64_t request, bool succeeded, std::uint64_t ns) {
            CHECK(from == "a");
            CHECK(request == 23);
            CHECK(ns == 0);  // no instance was activated
            receipt.set_value(succeeded);
        };
        target.runtime->set_mobility(std::move(destination));
        REQUIRE(target.runtime->prepare_arrival(std::move(arrival), {}));
        std::filesystem::create_directories(dir / "paglets" / (id + ".paglet.tmp"));
        CHECK(!target.runtime->commit_arrival(id));
        REQUIRE(received.wait_for(10s) == std::future_status::ready);
        CHECK(!received.get());
        CHECK(!target.runtime->info(id));
        REQUIRE(target.runtime->ending(id));
        CHECK(target.runtime->ending(id)->failed);
    }
    {
        rt::Config config;
        config.state_dir = dir;
        Fixture restarted(config);
        CHECK(restarted.runtime->list().empty());
    }
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("lifecycle: host controls are fenced through queued dispatch, prepared arrival and the move decision") {
    for (bool accepted : {false, true}) {
        Fixture source;
        Fixture target;
        std::promise<rt::Departure> promise;
        auto departure = promise.get_future();
        rt::MobilityHooks hooks;
        hooks.host_id = "a";
        hooks.depart = [&](rt::Departure d) { promise.set_value(std::move(d)); };
        source.runtime->set_mobility(std::move(hooks));
        const auto id = source.create("conformance.wasm");
        target.module("conformance.wasm");
        REQUIRE(source.runtime->dispatch(id, "b"));
        CHECK_EQ(source.runtime->dispose(id).error(), static_cast<std::int32_t>(abi::bad_state));
        CHECK_EQ(source.runtime->deactivate(id).error(), static_cast<std::int32_t>(abi::bad_state));
        CHECK_EQ(source.runtime->dispatch(id, "b").error(), static_cast<std::int32_t>(abi::bad_state));
        REQUIRE(departure.wait_for(10s) == std::future_status::ready);
        auto d = departure.get();
        const auto move = d.move;
        rt::Arrival arrival{std::move(d.state), std::move(d.image), "a", false, {}, {}, {}};
        REQUIRE(target.runtime->prepare_arrival(std::move(arrival), {}));
        CHECK_EQ(target.runtime->dispose(id).error(), static_cast<std::int32_t>(abi::bad_state));
        CHECK_EQ(target.runtime->deactivate(id).error(), static_cast<std::int32_t>(abi::bad_state));
        if (accepted) {
            REQUIRE(target.runtime->commit_arrival(id));
            source.runtime->finish_departure(move, "b");
            REQUIRE(eventually([&] { return target.runtime->info(id).has_value() && !source.runtime->info(id); }));
            CHECK(contains(target.journal(id), "event:arrived:a:0"));
            REQUIRE(target.runtime->dispose(id));
            REQUIRE(eventually([&] { return target.runtime->ending(id).has_value(); }));
        } else {
            target.runtime->abort_arrival(id);
            source.runtime->finish_departure(move, std::nullopt, "test admission refusal");
            source.runtime->wait_idle();
            CHECK(source.runtime->info(id).has_value());
            CHECK(!target.runtime->info(id));
            REQUIRE(source.runtime->deactivate(id));
            source.runtime->wait_idle();
            CHECK(source.runtime->info(id)->state == rt::PagletState::inactive);
            REQUIRE(source.runtime->dispose(id));
            REQUIRE(eventually([&] { return source.runtime->ending(id).has_value(); }));
        }
    }
}

PAGLETS_TEST("remaining: clones discard parent continuations and suspended coroutine frames") {
    for (bool remote : {false, true}) {
        Pair pair;
        auto& f = *pair.a;
        const auto parent = f.create("conformance.wasm");
        for (int kind : {0, 1, 2}) REQUIRE(f.cmd(parent, "watch_cleanup", Cmd{.code = kind}).status == 0);
        REQUIRE(f.runtime->wait_idle());
        auto counts = [&](Fixture& host, const rt::PagletId& id) {
            return dec<std::vector<std::int64_t>>(host.call(id, "cleanup_counts").payload);
        };
        CHECK(counts(f, parent) == std::vector<std::int64_t>({0, 0, 0, 0, 0, 0}));
        auto handle = f.code(parent, "lifecycle", Cmd{.name = remote ? "b" : "",
                                                    .code = static_cast<int>(abi::LifecycleOp::clone)});
        REQUIRE(handle > 0);
        const auto clone = f.target_of(parent, static_cast<std::int32_t>(handle));
        Fixture& destination = remote ? *pair.b : f;
        REQUIRE(eventually([&] { return destination.runtime->info(clone).has_value(); }));
        CHECK(counts(destination, clone) == std::vector<std::int64_t>({1, 1, 1, 0, 0, 0}));
        CHECK(counts(f, parent) == std::vector<std::int64_t>({0, 0, 0, 0, 0, 0}));
        CHECK(destination.runtime->remote_jobs().empty());
        REQUIRE(f.cmd(parent, "answer", Cmd{}).status == 0);
        REQUIRE(f.cmd(parent, "answer", Cmd{}).status == 0);
        const auto jobs = f.runtime->jobs();
        REQUIRE(jobs.size() == 1);
        REQUIRE(f.runtime->cancel_job(jobs.front().id));
        REQUIRE(eventually([&] { return counts(f, parent) == std::vector<std::int64_t>({1, 1, 1, 1, 1, 1}); }));
        CHECK(counts(destination, clone) == std::vector<std::int64_t>({1, 1, 1, 0, 0, 0}));
    }
}
