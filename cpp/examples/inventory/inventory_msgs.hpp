// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Messages of the Inventory Collector, Process Finder and Mesh Top demos
// (planning/cpp-demo-paglets.md, demos 8, 10 and 11).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace inventory_msgs {

// `start`: collect from every host (or `hosts`): the system summary, load,
// volumes and the `top` largest processes; with `process`, only processes
// whose name contains it (Process Finder).
struct Collect {
    std::vector<std::string> hosts;
    std::string process;
    std::int64_t top = 5;
    std::int64_t timeout_ms = 20'000;
};

struct Started {
    bool accepted = false;
    std::string reason;
    std::int64_t hosts = 0;
};

struct VolumeInfo {
    std::string mount_point;
    std::string filesystem;
    std::int64_t total = 0;
    std::int64_t available = 0;
};

struct ProcessInfo {
    std::int64_t pid = 0;
    std::string name;
    std::int64_t memory = 0;
};

// One host, in the same shape on every operating system.
struct HostInventory {
    std::string host;
    std::string host_name;
    std::string error;
    std::string os;
    std::string os_version;
    std::string architecture;
    std::string cpu_model;
    std::int64_t cpus = 0;
    std::int64_t memory_total = 0;
    std::int64_t memory_available = 0;
    double cpu_percent = 0;
    std::int64_t uptime_s = 0;
    std::vector<VolumeInfo> volumes;
    std::int64_t processes_total = 0;
    std::vector<ProcessInfo> processes;
};

struct Report {
    bool finished = false;
    std::vector<HostInventory> hosts;
};

}  // namespace inventory_msgs
