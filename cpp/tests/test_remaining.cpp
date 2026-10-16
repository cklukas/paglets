// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_fixture.hpp"
#include <paglets/runtime/mobility.hpp>
#include <paglets/runtime/modules.hpp>

#include <filesystem>
#include <condition_variable>
#include <future>
#include <limits>
#include <random>

using namespace paglets::test;

namespace {
template<class F> bool until(F f) {
    const auto stop = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < stop) {
        if (f()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return f();
}
std::int64_t epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
}

PAGLETS_TEST("remaining: required exports have the correct kind") {
    Fixture f;
    auto hash = f.module("conformance.wasm");
    auto original = f.runtime->modules().info(hash);
    REQUIRE(original.has_value());
    for (const auto name : abi::required_exports) {
        auto info = *original;
        for (auto& e : info.exports) if (e.name == name) e.kind = paglets::wasm::ExternKind::global;
        CHECK(!rt::check_paglet_module(info, rt::TrustClass::roaming));
    }
}

PAGLETS_TEST("remaining: module store refuses unimplemented Wasm system imports") {
    Fixture f;
    auto added = f.runtime->add_module_file(guest_path("system_import.wasm"));
    REQUIRE(!added);
    CHECK(added.error().find("paglets_sys.call") != std::string::npos);
}

PAGLETS_TEST("remaining: rejected reply fields leave the reply capability usable") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    const auto r = f.call(id, "invalid_reply");
    REQUIRE(r.status == 0);
    CHECK(dec<std::vector<std::int32_t>>(r.payload) == std::vector<std::int32_t>(3, abi::invalid_argument));
}

PAGLETS_TEST("remaining: invalid transfer tickets are rejected before accepting guest lifecycle work") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    std::atomic<int> departures{0};
    rt::MobilityHooks hooks;
    hooks.host_id = "a";
    hooks.depart = [&](rt::Departure) { ++departures; };
    f.runtime->set_mobility(std::move(hooks));
    for (const auto ticket : {"?retries=1", "b?retries=0", "b?retries=17", "b?arrival=maybe", "b?unknown=1", "b?retries"}) {
        for (const auto op : {abi::LifecycleOp::dispatch, abi::LifecycleOp::clone}) {
            CHECK_EQ(f.code(id, "lifecycle", Cmd{.name = ticket, .code = static_cast<int>(op)}), abi::invalid_argument);
        }
        CHECK_EQ(f.runtime->dispatch(id, ticket).error(), abi::invalid_argument);
    }
    CHECK_EQ(departures.load(), 0);
    CHECK(f.runtime->info(id).has_value());
}

PAGLETS_TEST("remaining: native services cannot spoof reserved messages") {
    struct Probe final : rt::SystemPaglet {
        std::string_view name() const override { return "sender-probe"; }
        void handle(rt::SystemContext& ctx, rt::ServiceCall& call) override {
            const std::string target(call.payload().begin(), call.payload().end());
            auto r = ctx.send(target, "paglets.recovered", {});
            (void)call.reply(0, enc(r ? 0 : r.error()));
        }
    };
    Fixture f;
    auto service = f.runtime->add_system_paglet(std::make_shared<Probe>());
    REQUIRE_OK(service);
    const auto id = f.create("conformance.wasm");
    CHECK_EQ(dec<std::int32_t>(f.call(*service, "probe", text(id)).payload), abi::invalid_argument);
}

PAGLETS_TEST("remaining: expired remote requests never enter the receiver handler") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    std::mutex mu;
    std::vector<rt::RemoteMessage> replies;
    rt::MobilityHooks hooks;
    hooks.host_id = "b";
    hooks.deliver = [&](rt::RemoteMessage m) { std::lock_guard lock(mu); replies.push_back(std::move(m)); };
    f.runtime->set_mobility(std::move(hooks));
    rt::RemoteMessage m;
    m.kind = rt::RemoteMessage::Kind::request;
    m.target = id;
    m.name = "count";
    m.requester_host = "a";
    m.correlation = 42;
    m.timeout_ms = 1000;
    m.deadline_ms = epoch_ms() - 1;
    auto wire = rt::decode_remote(rt::encode_remote(m));
    REQUIRE_OK(wire);
    REQUIRE(f.runtime->deliver_remote(std::move(*wire)) == abi::timeout);
    CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
    std::lock_guard lock(mu);
    REQUIRE(replies.size() == 1);
    CHECK_EQ(replies.front().status, abi::timeout);
    CHECK_EQ(replies.front().correlation, std::uint64_t{42});
}

PAGLETS_TEST("remaining: local and remote requests expire while waiting in the mailbox") {
    for (bool remote : {false, true}) {
        Fixture f;
        const auto id = f.create("conformance.wasm");
        REQUIRE(f.runtime->wait_idle());
        auto busy = f.runtime->request(id, "busy", enc(Cmd{.ms = 200}), 2s);
        std::future<rt::Reply> expired;
        if (remote) {
            rt::RemoteMessage m;
            m.kind = rt::RemoteMessage::Kind::request;
            m.target = id;
            m.name = "count";
            m.timeout_ms = 20;
            m.deadline_ms = epoch_ms() + 20;
            REQUIRE(f.runtime->deliver_remote(std::move(m)) == 0);
        } else {
            expired = f.runtime->request(id, "count", {}, 20ms);
        }
        REQUIRE(busy.get().status == 0);
        if (!remote) CHECK_EQ(expired.get().status, abi::timeout);
        REQUIRE(f.runtime->wait_idle());
        CHECK_EQ(dec<std::int64_t>(f.call(id, "count").payload), 1);
    }
}

PAGLETS_TEST("remaining: remote queue time reduces the forwarded request budget") {
    rt::RemoteMessage m;
    m.kind = rt::RemoteMessage::Kind::request;
    m.timeout_ms = 1000;
    m.deadline_ms = epoch_ms() + 60'000;  // clock skew must not extend the relative budget
    auto first = rt::decode_remote(rt::encode_remote(m));
    REQUIRE_OK(first);
    REQUIRE(first->expires_at.has_value());
    // Model a queued message whose original relative budget has nearly elapsed.
    first->expires_at = std::chrono::steady_clock::now() + 100ms;
    auto forwarded = rt::decode_remote(rt::encode_remote(*first));
    REQUIRE_OK(forwarded);
    CHECK(forwarded->timeout_ms > 0);
    CHECK(forwarded->timeout_ms <= 100);
    CHECK_EQ(forwarded->deadline_ms, m.deadline_ms);
    first->expires_at = std::chrono::steady_clock::now() - 1ms;
    auto expired = rt::decode_remote(rt::encode_remote(*first));
    REQUIRE_OK(expired);
    CHECK_EQ(expired->timeout_ms, std::int64_t{0});
    CHECK(expired->deadline_ms < epoch_ms());
    Fixture f;
    expired->target = f.create("conformance.wasm");
    expired->name = "count";
    CHECK_EQ(f.runtime->deliver_remote(std::move(*expired)), abi::timeout);
}

PAGLETS_TEST("remaining: requests held during movement retain their original expiry") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    std::promise<rt::Departure> leaving;
    auto departure = leaving.get_future();
    std::mutex mu;
    std::vector<rt::RemoteMessage> forwarded;
    rt::MobilityHooks hooks;
    hooks.host_id = "a";
    hooks.depart = [&](rt::Departure d) { leaving.set_value(std::move(d)); };
    hooks.deliver = [&](rt::RemoteMessage m) { std::lock_guard lock(mu); forwarded.push_back(std::move(m)); };
    f.runtime->set_mobility(std::move(hooks));
    REQUIRE(f.runtime->dispatch(id, "b"));
    REQUIRE(departure.wait_for(5s) == std::future_status::ready);
    const auto d = departure.get();
    auto expired = f.runtime->request(id, "count", {}, 20ms);
    auto pending = f.runtime->request(id, "count", {}, 2s);
    REQUIRE(expired.wait_for(1s) == std::future_status::ready);
    CHECK_EQ(expired.get().status, abi::timeout);
    f.runtime->finish_departure(d.move, "b");
    {
        std::lock_guard lock(mu);
        REQUIRE(forwarded.size() == 1);
        CHECK(forwarded.front().timeout_ms > 0);
        CHECK(forwarded.front().timeout_ms < 2000);
        CHECK(forwarded.front().deadline_ms > epoch_ms());
        CHECK(forwarded.front().expires_at.has_value());
    }
    f.runtime->shutdown();
    CHECK_EQ(pending.get().status, abi::gone);
}

PAGLETS_TEST("remaining: replies with no location hint reach mesh lookup") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    std::mutex mu;
    std::vector<rt::RemoteMessage> unresolved;
    rt::MobilityHooks hooks;
    hooks.host_id = "a";
    hooks.unresolved = [&](rt::RemoteMessage m) { std::lock_guard lock(mu); unresolved.push_back(std::move(m)); };
    f.runtime->set_mobility(std::move(hooks));
    rt::RemoteMessage reply;
    reply.kind = rt::RemoteMessage::Kind::reply;
    reply.target = std::string(32, 'f');
    reply.correlation = 71;
    reply.payload = text("original");
    REQUIRE(f.runtime->deliver_remote(reply) == 0);
    // Exercise the answering host too, not just forwarding an existing reply.
    rt::RemoteMessage request;
    request.kind = rt::RemoteMessage::Kind::request;
    request.target = id;
    request.name = "echo";
    request.requester = reply.target;
    request.correlation = 72;
    request.payload = text("answer");
    REQUIRE(f.runtime->deliver_remote(std::move(request)) == 0);
    REQUIRE(until([&] { std::lock_guard lock(mu); return unresolved.size() == 2; }));
    {
        std::lock_guard lock(mu);
        CHECK_EQ(unresolved[0].correlation, std::uint64_t{71});
        CHECK(unresolved[0].payload == text("original"));
        CHECK_EQ(unresolved[1].correlation, std::uint64_t{72});
        CHECK(unresolved[1].payload == text("answer"));
    }
    reply.hops = 4;
    // Do not re-enter lookup indefinitely at the forwarding limit.
    REQUIRE(f.runtime->deliver_remote(reply) == 0);
    std::lock_guard lock(mu);
    CHECK_EQ(unresolved.size(), std::size_t{2});
}

PAGLETS_TEST("remaining: timed deactivation wakes after restart") {
    const auto dir = std::filesystem::temp_directory_path() / ("paglets-wake-" + std::to_string(std::random_device{}()));
    rt::PagletId id;
    {
        Fixture f(rt::Config{.state_dir = dir});
        id = f.create("conformance.wasm");
        REQUIRE(f.code(id, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::deactivate), .ms = 800}) == 0);
        REQUIRE(f.runtime->wait_idle());
        REQUIRE(f.runtime->info(id)->state == rt::PagletState::inactive);
        f.runtime->shutdown();
    }
    {
        Fixture f(rt::Config{.state_dir = dir});
        REQUIRE(until([&] { auto p = f.runtime->info(id); return p && p->state == rt::PagletState::active; }));
        CHECK(contains(f.journal(id), "event:activated"));
    }
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("remaining: a consumed wake is not replayed after another restart") {
    const auto dir = std::filesystem::temp_directory_path() / ("paglets-wake-once-" + std::to_string(std::random_device{}()));
    rt::PagletId id;
    {
        Fixture f(rt::Config{.state_dir = dir});
        id = f.create("conformance.wasm");
        REQUIRE(f.code(id, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::deactivate), .ms = 200}) == 0);
        REQUIRE(f.runtime->wait_idle());
        REQUIRE(until([&] { auto p = f.runtime->info(id); return p && p->state == rt::PagletState::active; }));
        REQUIRE(f.runtime->wait_idle());
        f.runtime->shutdown();
    }
    {
        Fixture f(rt::Config{.state_dir = dir});
        // Recovery is lazy unless an outstanding wake or message needs it.
        std::this_thread::sleep_for(50ms);
        CHECK(f.runtime->info(id)->state == rt::PagletState::inactive);
    }
    std::filesystem::remove_all(dir);
}

PAGLETS_TEST("remaining: a queued old wake cannot interrupt a later timed deactivation") {
    struct Block final : rt::SystemPaglet {
        std::promise<void> entered;
        std::mutex mu;
        std::condition_variable cv;
        bool released = false;
        std::string_view name() const override { return "lane-blocker"; }
        void release() { { std::lock_guard lock(mu); released = true; } cv.notify_all(); }
        void handle(rt::SystemContext&, rt::ServiceCall& call) override {
            entered.set_value();
            std::unique_lock lock(mu);
            cv.wait(lock, [&] { return released; });
            (void)call.reply(0);
        }
    };
    auto blocker = std::make_shared<Block>();
    Fixture f(rt::Config{.threads = 1});
    // Release before Fixture destruction even if a REQUIRE fails.
    struct Release { std::shared_ptr<Block> b; ~Release() { b->release(); } } release{blocker};
    auto service = f.runtime->add_system_paglet(blocker);
    REQUIRE_OK(service);
    const auto id = f.create("conformance.wasm");
    REQUIRE(f.code(id, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::deactivate), .ms = 200}) == 0);
    REQUIRE(f.runtime->wait_idle());
    auto entered = blocker->entered.get_future();
    auto block = f.runtime->request(*service, "block");
    REQUIRE(entered.wait_for(1s) == std::future_status::ready);
    REQUIRE(until([&] { return f.runtime->info(id)->mailbox > 0; }));
    // A normal-priority request precedes the queued low-priority wake.
    auto sleep_again = f.runtime->request(id, "lifecycle", enc(Cmd{
        .code = static_cast<int>(abi::LifecycleOp::deactivate), .ms = 1000}));
    blocker->release();
    REQUIRE(block.get().status == 0);
    REQUIRE(sleep_again.get().status == 0);
    REQUIRE(f.runtime->wait_idle());
    CHECK(f.runtime->info(id)->state == rt::PagletState::inactive);
}

PAGLETS_TEST("remaining: timed deactivation wake travels to a dormant destination") {
    Fixture source, target;
    const auto id = source.create("conformance.wasm");
    target.module("conformance.wasm");
    REQUIRE(source.code(id, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::deactivate), .ms = 800}) == 0);
    REQUIRE(source.runtime->wait_idle());
    std::promise<rt::Departure> leaving;
    auto departure = leaving.get_future();
    rt::MobilityHooks hooks;
    hooks.host_id = "a";
    hooks.depart = [&](rt::Departure d) { leaving.set_value(std::move(d)); };
    source.runtime->set_mobility(std::move(hooks));
    REQUIRE(source.runtime->dispatch(id, "b"));
    REQUIRE(departure.wait_for(5s) == std::future_status::ready);
    auto d = departure.get();
    REQUIRE(d.state.wake_at_ms > 0);
    auto state = rt::decode_state(rt::encode_state(d.state));
    REQUIRE_OK(state);
    rt::Arrival incoming{std::move(*state), std::move(d.image), "a"};
    REQUIRE_OK(target.runtime->prepare_arrival(std::move(incoming), {}));
    REQUIRE_OK(target.runtime->commit_arrival(id, false));
    source.runtime->finish_departure(d.move, "b");
    REQUIRE(until([&] { auto p = target.runtime->info(id); return p && p->state == rt::PagletState::active; }));
    CHECK(contains(target.journal(id), "event:arrived:a:0"));
}

PAGLETS_TEST("remaining: extreme timer delays stay in the future") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    CHECK_EQ(f.code(id, "timer", Cmd{.name = "invalid", .ms = -1}), abi::invalid_argument);
    const auto before = epoch_ms();
    const auto handle = f.code(id, "timer", Cmd{.name = "far-future", .ms = std::numeric_limits<std::int64_t>::max()});
    REQUIRE(handle > 0);
    const auto caps = f.runtime->capabilities(id);
    auto timer = std::ranges::find_if(caps, [&](const auto& c) { return c.first == handle; });
    REQUIRE(timer != caps.end());
    CHECK(timer->second.fire_at > before);
    const auto horizon = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::duration::max() / 4).count();
    CHECK(timer->second.fire_at <= epoch_ms() + horizon);
    std::this_thread::sleep_for(20ms);
    CHECK(!contains(f.journal(id), "timer:far-future"));
    CHECK_EQ(f.code(id, "drop", Cmd{.handle = static_cast<std::int32_t>(handle)}), 0);
}

PAGLETS_TEST("remaining: unhandled one-way messages and timers are logged") {
    Fixture f;
    const auto id = f.create("conformance.wasm");
    REQUIRE_OK(f.runtime->send(id, "unhandled_oneway"));
    REQUIRE(f.call(id, "count").status == 0);
    CHECK(f.logged("handler returned not_handled for message unhandled_oneway"));
    REQUIRE(f.code(id, "timer", Cmd{.name = "unhandled_timer", .ms = 10}) > 0);
    REQUIRE(until([&] { return f.logged("handler returned not_handled for timer unhandled_timer"); }));
    CHECK_EQ(f.call(id, "unhandled_request").status, abi::unknown_message);
    CHECK(!f.logged("not_handled for message unhandled_request"));
    REQUIRE(f.call(id, "echo").status == 0);
    CHECK(!f.logged("not_handled for message echo"));
}

PAGLETS_TEST("remaining: recovery precedes a restored request timeout reply") {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("paglets-recovery-order-" + std::to_string(std::random_device{}()));
    rt::PagletId id;
    {
        Fixture f(rt::Config{.state_dir = dir});
        id = f.create("conformance.wasm");
        REQUIRE(f.code(id, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::restart),
                                           .ms = 1, .async = true}) == 0);
        REQUIRE(f.code(id, "request", Cmd{.handle = abi::self_handle, .name = "defer", .ms = 60'000}) > 0);
        REQUIRE(f.code(id, "lifecycle", Cmd{.code = static_cast<int>(abi::LifecycleOp::checkpoint)}) == 0);
        REQUIRE(f.runtime->wait_idle());
        f.runtime->shutdown(false);  // recover the explicitly stored checkpoint
    }
    {
        Fixture f(rt::Config{.state_dir = dir});
        REQUIRE(journal_eventually(f, id, "reply:timeout:"));
        const auto journal = f.journal(id);
        const auto recovered = index_of(journal, "event:recovered:ok");
        const auto timeout = index_of(journal, "reply:timeout:");
        REQUIRE(recovered > 0);
        CHECK_EQ(journal[static_cast<std::size_t>(recovered - 1)], std::string("event:activated"));
        CHECK(timeout > recovered);
    }
    std::filesystem::remove_all(dir);
}
