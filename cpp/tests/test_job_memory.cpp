// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "runtime_fixture.hpp"
#include "../host/src/runtime_jobs.hpp"

#include <fstream>
#include <future>

PAGLETS_TEST("jobs: AOT preserves memory growth during large decimal conversion") {
#ifdef PAGLETS_TEST_WAMRC
    using namespace paglets;
    using namespace paglets::test;
    const std::filesystem::path compiler = PAGLETS_TEST_WAMRC;
    if (!std::filesystem::exists(compiler) || !wasm::Module::aot_supported()) skip("no AOT compiler");
    const auto path = guest_path("pi.wasm");
    if (path.empty()) skip("pi.wasm not available");
    std::ifstream input(path, std::ios::binary);
    runtime::Bytes bytes(std::istreambuf_iterator<char>(input), {});
    auto module = wasm::Module::load(std::move(bytes), wasm::ImportPolicy::system());
    REQUIRE_OK(module);
    runtime::JobPool::Settings settings;
    settings.slots = 1;
    settings.aot_compiler = compiler;
    settings.acquire = [&](const std::string&) { return module; };
    if (const char* worker = std::getenv("PAGLETS_TEST_WORKER")) settings.worker = worker;
    std::shared_ptr<std::promise<runtime::JobOutcome>> pending;
    runtime::JobPool pool(settings, [&](runtime::JobOutcome o) {
                              const auto target = pending;
                              target->set_value(std::move(o));
                          },
                          [](std::int32_t, const std::string& text) { std::cerr << "    " << text << '\n'; });
    for (int sample = 0; sample != 4; ++sample) {
        if (sample == 1) REQUIRE(pool.compile_now(*module, 300s));
        pending = std::make_shared<std::promise<runtime::JobOutcome>>();
        auto reply = pending->get_future();
        runtime::JobRequest request;
        request.module = (*module)->hash_hex();
        request.function = "to_decimal";
        request.memory_pages = 512;
        // Deterministic conversion input, avoiding the cost of calculating Pi.
        request.input = runtime::Bytes(100002, '0');
        request.input[0] = '3';
        request.input[1] = '.';
        REQUIRE_OK(pool.submit(std::move(request)));
        REQUIRE(reply.wait_for(20s) == std::future_status::ready);
        const auto result = reply.get();
        if (result.status != 0) throw std::runtime_error(result.error);
        CHECK(result.aot == (sample != 0));
        const std::string expected = "3." + std::string(120409, '0');
        CHECK(std::string(result.output.begin(), result.output.end()) == expected);
    }
#else
    paglets::test::skip("no AOT compiler");
#endif
}
