// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Drives one ABI v1 paglet instance synchronously, without the runtime: for
// tools, measurements and engine-level tests. It delivers events and
// requests and captures the reply; of the imports it provides log,
// self_info, reply and cap_drop (everything else answers `unsupported`).

#pragma once

#include <paglets/wasm/engine.hpp>

#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::wasm {

class DirectHarness final : public HostImports {
public:
    struct Reply {
        std::int32_t status = 0;  // 0 or an abi::Error
        std::vector<std::uint8_t> payload;
    };

    explicit DirectHarness(Instance& target, std::string id = "direct");
    ~DirectHarness() override;
    DirectHarness(const DirectHarness&) = delete;
    DirectHarness& operator=(const DirectHarness&) = delete;

    // Delivers `created` (a fresh instance) with the given arguments.
    std::expected<std::int32_t, std::string> start(std::span<const std::uint8_t> args = {});

    // Delivers a request and returns its reply. A trap is an error.
    std::expected<Reply, std::string> call(std::string_view name, std::span<const std::uint8_t> payload = {});

    Document self_info() override;
    std::int32_t reply(std::int32_t handle, std::span<const std::uint8_t> msg) override;
    std::int32_t cap_drop(std::int32_t handle) override;

private:
    Instance& instance_;
    std::string id_;
    std::int32_t open_reply_ = 0;
    std::optional<Reply> reply_;
};

}  // namespace paglets::wasm
