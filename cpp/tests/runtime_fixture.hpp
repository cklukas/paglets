// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Test fixture around a runtime with the conformance guest, shared by the
// runtime and system paglet tests.

#pragma once

#include "test.hpp"

#include <conformance_cmd.hpp>
#include <paglets/abi.hpp>
#include <paglets/runtime/runtime.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace paglets::test {

namespace rt = paglets::runtime;
namespace abi = paglets::abi;
using conformance::Cmd;
using Bytes = std::vector<std::uint8_t>;
using namespace std::chrono_literals;

inline Bytes text(std::string_view s) {
    return Bytes(s.begin(), s.end());
}

template <class T>
Bytes enc(const T& v) {
    return abi::encode(v);
}

template <class T>
T dec(const Bytes& b) {
    T v{};
    if (!abi::decode(b, v)) throw std::runtime_error("reply does not decode");
    return v;
}

struct Fixture {
    explicit Fixture(rt::Config config = {}) {
        config.log = [this](const rt::LogRecord& r) {
            std::lock_guard lock(log_mu);
            if (std::getenv("PAGLETS_TEST_LOG") != nullptr) std::cerr << "    log: " << r.text << "\n";
            log.push_back(r.text);
        };
        if (config.threads == 2) config.threads = 3;
        // The whole suite also runs with worker processes (CTest unit_workers).
        if (const char* worker = std::getenv("PAGLETS_TEST_WORKER"); worker != nullptr && *worker != '\0') {
            config.worker_executable = worker;
        }
        runtime = std::make_unique<rt::Runtime>(std::move(config));
    }

    std::string module(const std::string& file) {
        const auto path = paglets::test::guest_path(file);
        if (path.empty()) paglets::test::skip(file + " not available");
        auto hash = runtime->add_module_file(path);
        if (!hash) throw std::runtime_error(hash.error());
        return *hash;
    }

    rt::PagletId create(const std::string& file, Bytes args = {}) {
        auto id = runtime->create(module(file), rt::CreateOptions{std::move(args)});
        if (!id) throw std::runtime_error(id.error());
        return *id;
    }

    rt::Reply call(const rt::PagletId& id, std::string_view name, Bytes payload = {}) {
        return runtime->call(id, name, std::move(payload), 10s);
    }

    rt::Reply cmd(const rt::PagletId& id, std::string_view name, const Cmd& c) { return call(id, name, enc(c)); }

    std::int64_t code(const rt::PagletId& id, std::string_view name, const Cmd& c) {
        auto r = cmd(id, name, c);
        if (r.status != 0) throw std::runtime_error("status " + std::string(abi::error_name(r.status)));
        return dec<std::int64_t>(r.payload);
    }

    std::vector<std::string> journal(const rt::PagletId& id) {
        runtime->wait_idle();
        auto r = call(id, "journal");
        if (r.status != 0) throw std::runtime_error("journal: " + std::string(abi::error_name(r.status)));
        return dec<std::vector<std::string>>(r.payload);
    }

    // The paglet ID behind an endpoint handle held by `holder`.
    rt::PagletId target_of(const rt::PagletId& holder, std::int32_t handle) {
        auto r = cmd(holder, "inspect", Cmd{.handle = handle});
        if (r.status != 0) throw std::runtime_error("inspect failed");
        const auto info = dec<abi::CapInfo>(r.payload);
        return info.target.substr(std::string("paglet:").size());
    }

    bool logged_exactly(std::string_view line) {
        std::lock_guard lock(log_mu);
        return std::ranges::find(log, line) != log.end();
    }

    bool logged(std::string_view needle) {
        std::lock_guard lock(log_mu);
        return std::ranges::any_of(log, [&](const std::string& l) { return l.find(needle) != std::string::npos; });
    }

    std::mutex log_mu;
    std::vector<std::string> log;
    std::unique_ptr<rt::Runtime> runtime;
};

inline bool contains(const std::vector<std::string>& v, std::string_view s) {
    return std::ranges::find(v, s) != v.end();
}

// Polls the journal until it contains `entry` (slow sanitizer builds need
// more time than the nominal delays).
inline bool journal_eventually(Fixture& f, const rt::PagletId& id, std::string_view entry,
                               std::chrono::milliseconds timeout = 5000ms) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < until) {
        if (contains(f.journal(id), entry)) return true;
        std::this_thread::sleep_for(20ms);
    }
    return false;
}

inline std::ptrdiff_t index_of(const std::vector<std::string>& v, std::string_view s) {
    auto it = std::ranges::find(v, s);
    return it == v.end() ? -1 : it - v.begin();
}

}  // namespace paglets::test
