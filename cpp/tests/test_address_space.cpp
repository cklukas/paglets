// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"
#include "../host/src/wasm_address_space.hpp"

#include <algorithm>
#include <cerrno>
#include <sstream>
#if defined(__linux__) && defined(__aarch64__)
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace wa = paglets::wasm::detail;

PAGLETS_TEST("address space: software bounds checks do not probe or warn") {
    std::ostringstream out;
    bool called = false;
    wa::warn_address_space(false, out, [&] {
        called = true;
        return ENOMEM;
    });
    CHECK(!called);
    CHECK(out.str().empty());
}

PAGLETS_TEST("address space: a successful hardware probe is quiet") {
    std::ostringstream out;
    int calls = 0;
    wa::warn_address_space(true, out, [&] {
        ++calls;
        return 0;
    });
    CHECK_EQ(calls, 1);
    CHECK(out.str().empty());
}

PAGLETS_TEST("address space: a failed probe explains virtual density and the build option") {
    for (const int error : {ENOMEM, EPERM}) {
        std::ostringstream out;
        wa::warn_address_space(true, out, [=] { return error; });
        CHECK(out.str().find("virtual address space") != std::string::npos);
        CHECK(out.str().find("process limit") != std::string::npos);
        CHECK(out.str().find("-DPAGLETS_WAMR_HW_BOUND_CHECK=OFF") != std::string::npos);
        CHECK(out.str().find("8 GB") != std::string::npos);
    }
}

#if defined(__linux__) && defined(__aarch64__)
PAGLETS_TEST("address space: the native probe fails under a real address-space limit") {
    // Only async-signal-safe syscalls and the allocation-free native probe
    // run after fork. The parent's limits and mappings remain untouched.
    rlimit limit{};
    REQUIRE(::getrlimit(RLIMIT_AS, &limit) == 0);
    limit.rlim_cur = std::min(limit.rlim_cur, rlim_t{2} << 30);
    int ends[2];
    REQUIRE(::pipe(ends) == 0);
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        ::close(ends[0]);
        ::alarm(5);
        int error = ::setrlimit(RLIMIT_AS, &limit) == 0 ? wa::probe_address_space() : -errno;
        const auto wrote = ::write(ends[1], &error, sizeof(error));
        ::_exit(wrote == sizeof(error) ? 0 : 2);
    }
    ::close(ends[1]);
    int error = 0, status = 0;
    ssize_t received;
    do {
        received = ::read(ends[0], &error, sizeof(error));
    } while (received < 0 && errno == EINTR);
    ::close(ends[0]);
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    REQUIRE(waited == child);
    REQUIRE(WIFEXITED(status));
    REQUIRE(WEXITSTATUS(status) == 0);
    REQUIRE(received == sizeof(error));
    REQUIRE(error == ENOMEM);
    std::ostringstream out;
    wa::warn_address_space(true, out, [=] { return error; });
    CHECK(out.str().find("virtual address-space probe failed") != std::string::npos);
}
#else
PAGLETS_TEST("address space: other platforms do not use the ARM64 Linux probe") {
    CHECK_EQ(wa::probe_address_space(), 0);
}
#endif
