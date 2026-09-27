// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Platform layer for Linux: /proc, uname, sysinfo, statvfs.

#include "platform.hpp"

#if defined(__linux__)

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string_view>

#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#include <sys/utsname.h>
#include <unistd.h>

namespace paglets::platform {

namespace {

std::string first_line(const std::string& path) {
    std::ifstream in(path);
    std::string line;
    std::getline(in, line);
    return line;
}

std::string trim(std::string_view s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string_view::npos) return {};
    const auto e = s.find_last_not_of(" \t\r\n");
    return std::string(s.substr(b, e - b + 1));
}

// A value from /proc/meminfo in bytes (the file gives kB).
std::uint64_t meminfo(std::string_view key) {
    std::ifstream in("/proc/meminfo");
    std::string line;
    while (std::getline(in, line)) {
        if (line.starts_with(key) && line.size() > key.size() && line[key.size()] == ':') {
            return std::strtoull(line.c_str() + key.size() + 1, nullptr, 10) * 1024;
        }
    }
    return 0;
}

std::string architecture_of(std::string machine) {
    if (machine == "aarch64" || machine == "arm64") return "arm64";
    if (machine == "amd64") return "x86_64";
    return machine;
}

std::string cpu_model() {
    std::ifstream in("/proc/cpuinfo");
    std::string line;
    std::string fallback;
    while (std::getline(in, line)) {
        const auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        const std::string key = trim(std::string_view(line).substr(0, colon));
        const std::string value = trim(std::string_view(line).substr(colon + 1));
        if (key == "model name" && !value.empty()) return value;
        if ((key == "Model" || key == "Hardware") && fallback.empty()) fallback = value;
    }
    if (fallback.empty()) {
        // Device tree model (Raspberry Pi and other boards).
        std::ifstream dt("/proc/device-tree/model");
        std::getline(dt, fallback, '\0');
    }
    return fallback;
}

// "\040" and friends in /proc/self/mounts.
std::string unescape_mount(std::string_view s) {
    std::string out;
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 3 < s.size() && std::isdigit(static_cast<unsigned char>(s[i + 1])) &&
            std::isdigit(static_cast<unsigned char>(s[i + 2])) && std::isdigit(static_cast<unsigned char>(s[i + 3]))) {
            out += static_cast<char>((s[i + 1] - '0') * 64 + (s[i + 2] - '0') * 8 + (s[i + 3] - '0'));
            i += 3;
        } else {
            out += s[i];
        }
    }
    return out;
}

bool pseudo_filesystem(std::string_view type) {
    static const std::set<std::string_view> pseudo = {
        "proc",      "sysfs",     "cgroup",      "cgroup2",         "devpts",      "devtmpfs", "securityfs",
        "pstore",    "bpf",       "tracefs",     "debugfs",         "configfs",    "fusectl",  "mqueue",
        "hugetlbfs", "autofs",    "binfmt_misc", "rpc_pipefs",      "nsfs",        "tmpfs",    "ramfs",
        "efivarfs",  "selinuxfs", "squashfs",    "fuse.gvfsd-fuse", "fuse.portal", "nfsd",     "overlay_internal"};
    return pseudo.contains(type);
}

}  // namespace

Summary summary() {
    Summary s;
    s.os = "linux";
    utsname u{};
    if (::uname(&u) == 0) {
        s.os_version = std::string(u.sysname) + " " + u.release;
        s.architecture = architecture_of(u.machine);
    }
    // The distribution name, where available.
    std::ifstream release("/etc/os-release");
    std::string line;
    while (std::getline(release, line)) {
        if (line.starts_with("PRETTY_NAME=")) {
            std::string name = line.substr(12);
            if (name.size() >= 2 && name.front() == '"' && name.back() == '"') name = name.substr(1, name.size() - 2);
            if (!name.empty()) s.os_version = name + " (" + s.os_version + ")";
            break;
        }
    }
    char host[256] = {};
    if (::gethostname(host, sizeof host - 1) == 0) s.host_name = host;
    s.cpu_model = cpu_model();
    const long n = ::sysconf(_SC_NPROCESSORS_ONLN);
    s.cpu_count = n > 0 ? static_cast<std::uint32_t>(n) : 0;
    s.memory_total = meminfo("MemTotal");
    struct sysinfo si {};
    if (::sysinfo(&si) == 0) s.uptime_s = static_cast<std::uint64_t>(si.uptime);
    return s;
}

CpuSample cpu_sample() {
    CpuSample sample;
    std::ifstream in("/proc/stat");
    std::string line;
    while (std::getline(in, line)) {
        if (!line.starts_with("cpu")) break;
        std::istringstream fields(line);
        std::string label;
        fields >> label;
        std::uint64_t v[8] = {};  // user nice system idle iowait irq softirq steal
        for (auto& x : v) fields >> x;
        CpuTimes t;
        for (auto x : v) t.total += x;
        t.busy = t.total - v[3] - v[4];
        if (label == "cpu") {
            sample.all = t;
        } else {
            sample.cores.push_back(t);
        }
    }
    return sample;
}

Memory memory() {
    return Memory{meminfo("MemTotal"), meminfo("MemAvailable")};
}

std::optional<std::vector<double>> load_average() {
    double avg[3] = {};
    if (::getloadavg(avg, 3) != 3) return std::nullopt;
    return std::vector<double>{avg[0], avg[1], avg[2]};
}

std::vector<Volume> volumes() {
    std::vector<Volume> out;
    std::set<std::string> seen;
    std::ifstream in("/proc/self/mounts");
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream fields(line);
        std::string device;
        std::string mount;
        std::string type;
        std::string options;
        fields >> device >> mount >> type >> options;
        if (pseudo_filesystem(type)) continue;
        Volume v;
        v.name = unescape_mount(device);
        v.mount_point = unescape_mount(mount);
        v.filesystem = type;
        if (!seen.insert(v.mount_point).second) continue;
        struct statvfs st {};
        if (::statvfs(v.mount_point.c_str(), &st) != 0 || st.f_blocks == 0) continue;
        const std::uint64_t unit = st.f_frsize != 0 ? st.f_frsize : st.f_bsize;
        v.total = static_cast<std::uint64_t>(st.f_blocks) * unit;
        v.free = static_cast<std::uint64_t>(st.f_bfree) * unit;
        v.available = static_cast<std::uint64_t>(st.f_bavail) * unit;
        v.read_only = (st.f_flag & ST_RDONLY) != 0;
        out.push_back(std::move(v));
    }
    return out;
}

std::vector<Process> processes(std::uint32_t& total) {
    std::vector<Process> out;
    total = 0;
    const long page = ::sysconf(_SC_PAGESIZE);
    const long ticks = ::sysconf(_SC_CLK_TCK);
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator("/proc", ec)) {
        const std::string name = entry.path().filename().string();
        if (name.empty() || !std::all_of(name.begin(), name.end(), [](char c) { return c >= '0' && c <= '9'; })) {
            continue;
        }
        ++total;
        Process p;
        p.pid = static_cast<std::uint32_t>(std::strtoul(name.c_str(), nullptr, 10));
        p.name = first_line(entry.path().string() + "/comm");
        std::ifstream statm(entry.path().string() + "/statm");
        std::uint64_t size = 0;
        std::uint64_t resident = 0;
        if (!(statm >> size >> resident)) continue;  // the process ended meanwhile
        p.memory = resident * static_cast<std::uint64_t>(page);
        // /proc/<pid>/stat: fields 14 and 15 (utime, stime) after the
        // parenthesized command name, which may contain spaces.
        const std::string stat = first_line(entry.path().string() + "/stat");
        const auto close = stat.rfind(')');
        if (close != std::string::npos && ticks > 0) {
            std::istringstream rest(stat.substr(close + 2));
            std::string field;
            std::uint64_t utime = 0;
            std::uint64_t stime = 0;
            for (int i = 3; i <= 15 && rest >> field; ++i) {
                if (i == 14) utime = std::strtoull(field.c_str(), nullptr, 10);
                if (i == 15) stime = std::strtoull(field.c_str(), nullptr, 10);
            }
            p.cpu_ms = (utime + stime) * 1000 / static_cast<std::uint64_t>(ticks);
        }
        out.push_back(std::move(p));
    }
    return out;
}

}  // namespace paglets::platform

#endif
