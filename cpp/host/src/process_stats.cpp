// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "process_stats.hpp"

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(_WIN32)
#include <windows.h>
#include <psapi.h>
#else
#include <fstream>
#include <unistd.h>
#endif

namespace paglets {

std::uint64_t resident_bytes() {
#if defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) !=
        KERN_SUCCESS) {
        return 0;
    }
    return info.resident_size;
#elif defined(_WIN32)
    PROCESS_MEMORY_COUNTERS pmc{};
    if (!GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof pmc)) return 0;
    return pmc.WorkingSetSize;
#else
    std::ifstream statm("/proc/self/statm");
    std::uint64_t size = 0;
    std::uint64_t resident = 0;
    if (!(statm >> size >> resident)) return 0;
    return resident * static_cast<std::uint64_t>(sysconf(_SC_PAGESIZE));
#endif
}

}  // namespace paglets
