// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wasm/direct.hpp>

#include <paglets/abi.hpp>

namespace paglets::wasm {

namespace {
constexpr std::int32_t reply_handle = 2;
}

DirectHarness::DirectHarness(Instance& target, std::string id) : instance_(target), id_(std::move(id)) {
    instance_.set_imports(this);
}

DirectHarness::~DirectHarness() {
    instance_.set_imports(nullptr);
}

std::expected<std::int32_t, std::string> DirectHarness::start(std::span<const std::uint8_t> args) {
    abi::StartEvent e;
    e.args.assign(args.begin(), args.end());
    const std::array<std::uint32_t, 1> lead{static_cast<std::uint32_t>(abi::EventKind::created)};
    return instance_.call_with_data("paglets_on_event", lead, abi::encode(e));
}

std::expected<DirectHarness::Reply, std::string> DirectHarness::call(std::string_view name,
                                                                     std::span<const std::uint8_t> payload) {
    abi::Delivered d;
    d.kind = abi::MessageKind::request;
    d.name = std::string(name);
    d.payload.assign(payload.begin(), payload.end());
    d.sender = abi::SenderRecord{"host", "local", "system", "", "direct"};
    d.reply = reply_handle;
    open_reply_ = reply_handle;
    reply_.reset();
    auto result = instance_.call_with_data("paglets_on_message", std::span<const std::uint32_t>{}, abi::encode(d));
    open_reply_ = 0;
    if (!result) return std::unexpected(result.error());
    if (reply_) return std::move(*reply_);
    if (*result == abi::not_handled) return Reply{abi::unknown_message, {}};
    if (*result < 0) return Reply{*result, {}};
    return Reply{abi::gone, {}};
}

HostImports::Document DirectHarness::self_info() {
    abi::SelfInfo info{id_, instance_.module()->hash_hex(), "local", "roaming", "direct", abi::version};
    return abi::encode(info);
}

std::int32_t DirectHarness::reply(std::int32_t handle, std::span<const std::uint8_t> msg) {
    if (handle != reply_handle || open_reply_ != reply_handle) return abi::bad_handle;
    abi::OutMessage m;
    if (!abi::decode(msg, m)) return abi::malformed;
    reply_ = Reply{abi::ok, std::move(m.payload)};
    open_reply_ = 0;
    return abi::ok;
}

std::int32_t DirectHarness::cap_drop(std::int32_t handle) {
    if (handle != reply_handle || open_reply_ != reply_handle) return abi::bad_handle;
    open_reply_ = 0;
    return abi::ok;
}

}  // namespace paglets::wasm
