// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "wasm_address_space.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <sstream>
#if defined(__linux__) && defined(__aarch64__)
#include <sys/mman.h>
#endif

namespace paglets::wasm::detail {
int probe_address_space() {
#if defined(__linux__) && defined(__aarch64__)
    constexpr auto size = std::size_t{1} << 40;
    void* region = ::mmap(nullptr, size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (region == MAP_FAILED) return errno;
    if (::munmap(region, size) != 0) return errno;
#endif
    return 0;
}

void warn_address_space(bool hardware_checks, std::ostream& out, const std::function<int()>& probe) {
    if (!hardware_checks) return;
    const int error = probe();
    if (!error) return;
    out << "paglets: hardware bounds checks reserve 8 GB of virtual address space per instance. "
           "A 1 TB virtual address-space probe failed ("
        << std::strerror(error)
        << "). A limited address space (such as a 39-bit kernel) or a process limit can restrict "
           "paglet/job density. Rebuild with -DPAGLETS_WAMR_HW_BOUND_CHECK=OFF for software bounds checks.\n";
}
const std::string& startup_address_space_warning() {
    static const std::string warning = [] {
        std::ostringstream out;
        warn_address_space(PAGLETS_WAMR_HW_BOUND_CHECK != 0, out);
        return out.str();
    }();
    return warning;
}
}  // namespace paglets::wasm::detail
