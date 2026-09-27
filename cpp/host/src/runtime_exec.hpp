// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internal to the runtime: where paglet instances run. Every scheduler lane
// owns an executor, either in the host process or backed by one worker
// process (plan, section 3.4). The scheduler only sees placed instances.

#pragma once

#include "ipc.hpp"

#include <paglets/wasm/engine.hpp>
#include <paglets/wasm/snapshot.hpp>

#include <cstdint>
#include <expected>
#include <filesystem>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::runtime {

// A paglet instance placed on an executor. Used by the lane thread only,
// except terminate(), which any thread may call.
class Placed {
public:
    virtual ~Placed() = default;

    // Calls an export with (lead..., ptr, len) as Instance::call_with_data.
    virtual std::expected<std::int32_t, std::string> call(std::string_view export_name,
                                                          std::span<const std::uint32_t> lead,
                                                          std::span<const std::uint8_t> data) = 0;
    virtual std::expected<wasm::Snapshot, std::string> capture() = 0;
    virtual void terminate() = 0;
    // True if the instance is gone with its worker process (checked before
    // each delivery, so only a call in progress fails with a crash).
    virtual bool lost() = 0;
};

class Executor {
public:
    virtual ~Executor() = default;

    // Instantiates the module: fresh (running its initializer) or from an
    // image. Host imports of the instance go to `imports`, which must outlive
    // the returned object.
    virtual std::expected<std::unique_ptr<Placed>, std::string> place(const std::string& paglet,
                                                                      const std::shared_ptr<wasm::Module>& module,
                                                                      const wasm::Limits& limits,
                                                                      const wasm::Snapshot* image,
                                                                      wasm::HostImports& imports) = 0;

    // Drops the compiled modules of the executor that are not in `keep`
    // (a worker process compiles its own copies). Called on the lane thread.
    virtual void retain_modules(const std::set<std::string>& keep) { (void)keep; }
    // Hashes of the modules a worker process has compiled (none in-process).
    virtual std::vector<std::string> loaded_modules() const { return {}; }

    // Process ID of the worker process, if there is one running.
    virtual std::optional<int> process_id() const { return std::nullopt; }
};

std::unique_ptr<Executor> make_in_process_executor();

// Runs instances in a worker process started from `executable`; the process
// is (re)started when needed.
std::expected<std::unique_ptr<Executor>, std::string> make_worker_executor(
    std::filesystem::path executable, bool sandbox, std::function<void(const std::string&)> warn);

// Main loop of a worker process (paglets-worker): calls arrive through the
// shared-memory rings in `shm_handle` (wake-ups on `doorbell`),
// terminations on `control`. With `sandbox` it applies the OS sandbox
// before it accepts work.
int run_worker(ipc::Ends doorbell, ipc::Ends control, std::intptr_t shm_handle, std::size_t ring_capacity,
               bool sandbox);

}  // namespace paglets::runtime
