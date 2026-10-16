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
#include <cstdlib>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

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

PAGLETS_TEST("ipc rings: a full default paglet image leaves room for its metadata") {
    auto p = make_pair(1024 * 1024);
    const auto frame = pattern(256u * 1024 * 1024 + 1024 * 1024, 19);
    std::expected<std::vector<std::uint8_t>, std::string> received = std::unexpected("not received");
    std::thread reader([&] {
        received = p.worker.receive();
        p.worker.close();  // also unblocks a writer if a frame is refused
    });
    const auto sent = p.host.send(frame);
    if (!sent) p.host.close();
    reader.join();
    REQUIRE_OK(sent);
    REQUIRE_OK(received);
    CHECK(*received == frame);
}

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
#ifndef _WIN32
PAGLETS_TEST("ipc process: workers receive no service secrets, stray descriptors or host stdin") {
    const char* old = std::getenv("PAGLETS_TEST_SECRET");
    const bool had = old != nullptr;
    const std::string previous = old ? old : "";
    REQUIRE(::setenv("PAGLETS_TEST_SECRET", "should-not-see", 1) == 0);
    struct Restore {
        bool had; std::string previous;
        ~Restore() { if (had) ::setenv("PAGLETS_TEST_SECRET", previous.c_str(), 1); else ::unsetenv("PAGLETS_TEST_SECRET"); }
    } restore{had, previous};
    int hidden[2];
    REQUIRE(::pipe(hidden) == 0);
    const int leaked = ::fcntl(hidden[0], F_DUPFD, 128);  // deliberately lacks close-on-exec
    ::close(hidden[0]); ::close(hidden[1]);
    REQUIRE(leaked >= 128);
    struct Close { int fd; ~Close() { ::close(fd); } } close{leaked};
    auto pair = ipc::duplex_pair();
    REQUIRE_OK(pair);
    const char* configured = std::getenv("TMPDIR");
    const std::string temporary = configured ? configured : "";
    auto child = ipc::Process::spawn("/bin/sh", {"-c",
        "printf '%s\\n' \"${PAGLETS_TEST_SECRET-not-inherited}\" >&3; "
        "if [ -e /dev/fd/$1 ]; then printf 'open\\n' >&3; else printf 'closed\\n' >&3; fi; "
        "if [ \"$TMPDIR\" = \"$2\" ]; then printf 'tmp-ok\\n' >&3; else printf 'tmp-lost\\n' >&3; fi; "
        "if [ /dev/fd/0 -ef /dev/null ]; then printf 'null-stdin' >&3; else printf 'host-stdin' >&3; fi",
        "worker-probe", std::to_string(leaked), temporary}, {pair->child.write}, -1, true);
    ipc::close_ends(pair->child);
    struct Ends { ipc::Ends value; ~Ends() { ipc::close_ends(value); } } ends{pair->host};
    REQUIRE_OK(child);
    std::string output;
    std::uint8_t c;
    while (ipc::read_all(pair->host.read, &c, 1)) output += static_cast<char>(c);
    child->stop(std::chrono::seconds(1));
    CHECK(output.starts_with("not-inherited\n"));
    CHECK(output.find("\nclosed\n") != std::string::npos);
    CHECK(output.find("\ntmp-ok\n") != std::string::npos);
    CHECK(output.ends_with("null-stdin"));
}
#endif
