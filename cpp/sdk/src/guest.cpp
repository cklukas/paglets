// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/guest.hpp>

#include <cstdlib>
#include <cstring>

extern "C" {

__attribute__((import_module("paglets"), import_name("log"))) void paglets_host_log(const char* text,
                                                                                    std::uint32_t len);

__attribute__((export_name("paglets_abi_version"))) std::uint32_t paglets_abi_version() {
    return paglets::guest::abi_version;
}

__attribute__((export_name("paglets_alloc"))) void* paglets_alloc(std::uint32_t size) {
    return std::malloc(size == 0 ? 1 : size);
}

__attribute__((export_name("paglets_free"))) void paglets_free(void* ptr) {
    std::free(ptr);
}

// Returns (reply_ptr << 32) | reply_len. The reply stays valid until the
// next call of paglets_handle.
__attribute__((export_name("paglets_handle"))) std::uint64_t paglets_handle(const std::uint8_t* request,
                                                                            std::uint32_t len) {
    static std::vector<std::uint8_t> reply;
    reply = paglets::guest::handle_message(std::span<const std::uint8_t>(request, len));
    const auto ptr = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(reply.data()));
    return (ptr << 32) | static_cast<std::uint32_t>(reply.size());
}

}  // extern "C"

namespace paglets::guest {

void log(std::string_view text) {
    paglets_host_log(text.data(), static_cast<std::uint32_t>(text.size()));
}

std::vector<std::uint8_t> reply_error(std::string_view message) {
    msgpack::Writer w;
    w.write_array_header(2);
    w.write_str("error");
    w.write_str(message);
    return w.take();
}

}  // namespace paglets::guest
