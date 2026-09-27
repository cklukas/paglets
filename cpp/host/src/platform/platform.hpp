// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Host platform layer (plan, WP10): system information with one shape on
// macOS, Linux and Windows. One implementation file per platform
// (platform_linux.cpp, platform_macos.cpp, platform_windows.cpp).

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace paglets::platform {

struct Summary {
    std::string os;  // linux, macos, windows
    std::string os_version;
    std::string host_name;
    std::string architecture;  // x86_64, arm64, ...
    std::string cpu_model;
    std::uint32_t cpu_count = 0;
    std::uint64_t memory_total = 0;
    std::uint64_t uptime_s = 0;
};

// Cumulative CPU time counters (any unit, consistent between calls).
struct CpuTimes {
    std::uint64_t busy = 0;
    std::uint64_t total = 0;
};

struct CpuSample {
    CpuTimes all;
    std::vector<CpuTimes> cores;
};

struct Memory {
    std::uint64_t total = 0;
    std::uint64_t available = 0;
};

struct Volume {
    std::string name;
    std::string mount_point;
    std::string filesystem;
    std::uint64_t total = 0;
    std::uint64_t free = 0;
    std::uint64_t available = 0;
    bool read_only = false;
};

struct Process {
    std::uint32_t pid = 0;
    std::string name;
    std::uint64_t memory = 0;
    std::optional<std::uint64_t> cpu_ms;
};

Summary summary();
CpuSample cpu_sample();
Memory memory();
std::optional<std::vector<double>> load_average();  // absent on Windows
std::vector<Volume> volumes();
// Every process the host may inspect; `total` receives the number of
// processes including those it may not.
std::vector<Process> processes(std::uint32_t& total);

// CPU use between two samples, 0 to 100.
double cpu_percent(const CpuTimes& before, const CpuTimes& after);

}  // namespace paglets::platform
