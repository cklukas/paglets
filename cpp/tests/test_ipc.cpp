// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Shared-memory rings between the host and worker processes (here: two
// threads of one process with the same mapping).

#include "test.hpp"

#include "../host/src/ipc.hpp"
#include "../host/src/shm_ring.hpp"

#include <numeric>
#include <stdexcept>
#include <thread>

namespace ipc = paglets::ipc;

namespace {

struct Pair {
    ipc::RingChannel host;
    ipc::RingChannel worker;
};

Pair make_pair(std::size_t capacity) {
    auto memory = ipc::SharedMemory::create(ipc::ring_region_size(capacity));
    if (!memory) throw std::runtime_error(memory.error());
    auto doorbell = ipc::duplex_pair();
    if (!doorbell) throw std::runtime_error(doorbell.error());
    // The worker side maps the same memory a second time, as a worker does.
    auto handle = ipc::duplicate_handle(memory->handle());
    if (!handle) throw std::runtime_error(handle.error());
    auto second = ipc::SharedMemory::attach(*handle, memory->size());
    if (!second) throw std::runtime_error(second.error());
    Pair p;
    p.host = ipc::RingChannel(std::move(*memory), capacity, doorbell->host, true);
    p.worker = ipc::RingChannel(std::move(*second), capacity, doorbell->child, false);
    return p;
}

std::vector<std::uint8_t> pattern(std::size_t n, std::uint8_t seed) {
    std::vector<std::uint8_t> v(n);
    for (std::size_t i = 0; i < n; ++i) v[i] = static_cast<std::uint8_t>(seed + i * 7);
    return v;
}

}  // namespace

PAGLETS_TEST("ipc rings: frames larger than the ring, wrap-around and chunking") {
    auto p = make_pair(4096);
    const std::vector<std::size_t> sizes = {0, 1, 100, 4000, 4096, 5000, 100'000, 3};
    std::thread echo([&] {
        for (std::size_t i = 0; i < sizes.size(); ++i) {
            auto frame = p.worker.receive();
            if (!frame) return;
            if (!p.worker.send(*frame)) return;
        }
    });
    for (std::size_t i = 0; i < sizes.size(); ++i) {
        const auto sent = pattern(sizes[i], static_cast<std::uint8_t>(i));
        REQUIRE(p.host.send(sent).has_value());
        auto back = p.host.receive();
        REQUIRE_OK(back);
        CHECK(*back == sent);
    }
    echo.join();
}

PAGLETS_TEST("ipc rings: a stream of frames beyond the ring's capacity, then back") {
    // A channel is used by one thread per side; here the host streams while
    // the worker reads, then the worker streams while the host reads, so
    // writers block on full rings and are woken by the readers.
    auto p = make_pair(8192);
    constexpr int frames = 2000;
    std::thread worker([&] {
        for (int i = 0; i < frames; ++i) {
            auto f = p.worker.receive();
            if (!f || *f != pattern(static_cast<std::size_t>(i % 900), 2)) return;
        }
        for (int i = 0; i < frames; ++i) {
            if (!p.worker.send(pattern(static_cast<std::size_t>(i % 700), 1))) return;
        }
    });
    for (int i = 0; i < frames; ++i) REQUIRE(p.host.send(pattern(static_cast<std::size_t>(i % 900), 2)).has_value());
    for (int i = 0; i < frames; ++i) {
        auto f = p.host.receive();
        REQUIRE_OK(f);
        CHECK(*f == pattern(static_cast<std::size_t>(i % 700), 1));
    }
    worker.join();
}

PAGLETS_TEST("ipc rings: a closed peer is noticed") {
    auto p = make_pair(4096);
    CHECK(!p.host.closed_by_peer());
    p.worker.close();
    CHECK(p.host.closed_by_peer());
    auto f = p.host.receive();
    CHECK(!f.has_value());
}