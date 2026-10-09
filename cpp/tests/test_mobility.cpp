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

#include <condition_variable>
#include <deque>
#include <functional>
#include <future>

using namespace paglets::test;
namespace pw = paglets::wasm;

namespace {

// Hosts "a" and "b": remote messages and departures go through a pump
// thread, outside the runtimes' locks, as a mesh would carry them.
struct Pair {
    std::unique_ptr<Fixture> a = std::make_unique<Fixture>();
    std::unique_ptr<Fixture> b = std::make_unique<Fixture>();
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::function<void()>> work;
    bool stopping = false;
    std::thread pump;
    // Departures wait here while `hold` is set.
    bool hold = false;
    std::condition_variable hold_cv;
    std::atomic<int> moves{0};
    std::function<std::optional<rt::Cap>(const rt::Cap&)> recreate;

    Pair() {
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
                if (Fixture* to = host(m.host)) to->runtime->deliver_remote(std::move(m));
            });
        };
        hooks.depart = [this, name](rt::Departure d) {
            post([this, name, d = std::move(d)]() mutable { move(name, std::move(d)); });
        };
        f.runtime->set_mobility(std::move(hooks));
    }

    void move(const std::string& from, rt::Departure d) {
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
    CHECK(!rt::decode_state(Bytes{1, 2, 3}).has_value());
}
