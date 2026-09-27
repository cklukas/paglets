// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `server-info` system paglet (planning/cpp-system-paglets.md):
// system information with the same schema on macOS, Linux and Windows.
// Sizes are bytes, times UTC milliseconds or seconds as named; fields a
// platform cannot provide are absent (optional) rather than zero.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::server_info {

struct SummaryRequest {};

struct Summary {
    std::string os;  // "linux", "macos" or "windows"
    std::string os_version;
    std::string host_name;
    std::string architecture;  // x86_64, arm64
    std::string cpu_model;
    std::uint32_t cpu_count = 0;  // logical processors
    std::uint64_t memory_total = 0;
    std::uint64_t uptime_s = 0;
    std::int64_t time_ms = 0;  // current time, Unix milliseconds
};

struct LoadRequest {};

struct Load {
    // CPU use since the previous `load` request to this host (or over a short
    // sample for the first one), 0 to 100.
    double cpu_percent = 0;
    std::vector<double> per_core;
    std::optional<std::vector<double>> load_average;  // 1, 5, 15 minutes; not on Windows
    std::uint64_t memory_total = 0;
    std::uint64_t memory_available = 0;
    std::uint64_t memory_used = 0;
};

struct VolumesRequest {};

struct Volume {
    std::string name;         // device or volume label
    std::string mount_point;  // "/", "/Volumes/Data", "C:\"
    std::string filesystem;   // ext4, apfs, NTFS, ...
    std::uint64_t total = 0;
    std::uint64_t free = 0;       // free bytes
    std::uint64_t available = 0;  // free bytes usable without privileges
    bool read_only = false;
};

struct Volumes {
    std::vector<Volume> volumes;
};

struct ProcessesRequest {
    std::uint32_t limit = 100;  // largest resident memory first
};

struct Process {
    std::uint32_t pid = 0;
    std::string name;
    std::uint64_t memory = 0;             // resident memory
    std::optional<std::uint64_t> cpu_ms;  // CPU time used so far
};

struct Processes {
    std::vector<Process> processes;
    std::uint32_t total = 0;  // processes on the host
};

struct Contract {
    static constexpr std::string_view service = "server-info";
    Summary summary(const SummaryRequest&);
    Load load(const LoadRequest&);
    Volumes volumes(const VolumesRequest&);
    Processes processes(const ProcessesRequest&);
};

}  // namespace paglets::services::server_info
