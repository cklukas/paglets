// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Platform layer for macOS: sysctl, Mach host statistics, getfsstat,
// libproc.

#include "platform.hpp"

#if defined(__APPLE__)

#include <array>
#include <cstdlib>
#include <cstring>
#include <ctime>

#include <libproc.h>
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <mach/mach_time.h>
#include <mach/processor_info.h>
#include <mach/vm_page_size.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <unistd.h>

namespace paglets::platform {

namespace {

std::string sysctl_string(const char* name) {
    std::size_t size = 0;
    if (::sysctlbyname(name, nullptr, &size, nullptr, 0) != 0 || size == 0) return {};
    std::string value(size, '\0');
    if (::sysctlbyname(name, value.data(), &size, nullptr, 0) != 0) return {};
    value.resize(std::strlen(value.c_str()));
    return value;
}

template <class T>
T sysctl_value(const char* name) {
    T value{};
    std::size_t size = sizeof value;
    if (::sysctlbyname(name, &value, &size, nullptr, 0) != 0) return T{};
    return value;
}

mach_port_t host_port() {
    static const mach_port_t port = mach_host_self();
    return port;
}

}  // namespace

Summary summary() {
    Summary s;
    s.os = "macos";
    utsname u{};
    std::string darwin;
    if (::uname(&u) == 0) {
        darwin = std::string(u.sysname) + " " + u.release;
        s.architecture = u.machine;
    }
    const std::string product = sysctl_string("kern.osproductversion");
    s.os_version = product.empty() ? darwin : "macOS " + product + " (" + darwin + ")";
    char host[256] = {};
    if (::gethostname(host, sizeof host - 1) == 0) s.host_name = host;
    s.cpu_model = sysctl_string("machdep.cpu.brand_string");
    s.cpu_count = static_cast<std::uint32_t>(sysctl_value<int>("hw.logicalcpu"));
    s.memory_total = sysctl_value<std::uint64_t>("hw.memsize");
    const timeval boot = sysctl_value<timeval>("kern.boottime");
    const std::time_t now = std::time(nullptr);
    if (boot.tv_sec > 0 && now > boot.tv_sec) s.uptime_s = static_cast<std::uint64_t>(now - boot.tv_sec);
    return s;
}

CpuSample cpu_sample() {
    CpuSample sample;
    natural_t count = 0;
    processor_info_array_t info = nullptr;
    mach_msg_type_number_t info_count = 0;
    if (host_processor_info(host_port(), PROCESSOR_CPU_LOAD_INFO, &count, &info, &info_count) != KERN_SUCCESS) {
        return sample;
    }
    const auto* load = reinterpret_cast<const processor_cpu_load_info_data_t*>(info);
    for (natural_t i = 0; i < count; ++i) {
        CpuTimes t;
        for (int state = 0; state < CPU_STATE_MAX; ++state) t.total += load[i].cpu_ticks[state];
        t.busy = t.total - load[i].cpu_ticks[CPU_STATE_IDLE];
        sample.all.total += t.total;
        sample.all.busy += t.busy;
        sample.cores.push_back(t);
    }
    vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(info),
                  static_cast<vm_size_t>(info_count) * sizeof(integer_t));
    return sample;
}

Memory memory() {
    Memory m;
    m.total = sysctl_value<std::uint64_t>("hw.memsize");
    vm_statistics64_data_t vm{};
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    if (host_statistics64(host_port(), HOST_VM_INFO64, reinterpret_cast<host_info64_t>(&vm), &count) == KERN_SUCCESS) {
        const std::uint64_t pages =
            static_cast<std::uint64_t>(vm.free_count) + vm.inactive_count + vm.speculative_count;
        m.available = pages * static_cast<std::uint64_t>(vm_page_size);
    }
    return m;
}

std::optional<std::vector<double>> load_average() {
    double avg[3] = {};
    if (::getloadavg(avg, 3) != 3) return std::nullopt;
    return std::vector<double>{avg[0], avg[1], avg[2]};
}

std::vector<Volume> volumes() {
    std::vector<Volume> out;
    const int n = ::getfsstat(nullptr, 0, MNT_NOWAIT);
    if (n <= 0) return out;
    std::vector<struct statfs> mounts(static_cast<std::size_t>(n) + 8);
    const int got = ::getfsstat(mounts.data(), static_cast<int>(mounts.size() * sizeof(struct statfs)), MNT_NOWAIT);
    for (int i = 0; i < got; ++i) {
        const struct statfs& m = mounts[static_cast<std::size_t>(i)];
        const std::string mount_point = m.f_mntonname;
        const std::string type = m.f_fstypename;
        // System volumes (VM, Preboot, Update, ...) are not browsable.
        if (m.f_blocks == 0 || type == "devfs" || type == "autofs") continue;
        if ((m.f_flags & MNT_DONTBROWSE) != 0 && mount_point != "/") continue;
        Volume v;
        v.name = m.f_mntfromname;
        v.mount_point = mount_point;
        v.filesystem = type;
        const std::uint64_t unit = m.f_bsize;
        v.total = static_cast<std::uint64_t>(m.f_blocks) * unit;
        v.free = static_cast<std::uint64_t>(m.f_bfree) * unit;
        v.available = static_cast<std::uint64_t>(m.f_bavail) * unit;
        v.read_only = (m.f_flags & MNT_RDONLY) != 0;
        out.push_back(std::move(v));
    }
    return out;
}

std::vector<Process> processes(std::uint32_t& total) {
    std::vector<Process> out;
    total = 0;
    const int bytes = ::proc_listpids(PROC_ALL_PIDS, 0, nullptr, 0);
    if (bytes <= 0) return out;
    std::vector<pid_t> pids(static_cast<std::size_t>(bytes) / sizeof(pid_t) + 64);
    const int filled = ::proc_listpids(PROC_ALL_PIDS, 0, pids.data(), static_cast<int>(pids.size() * sizeof(pid_t)));
    if (filled <= 0) return out;
    pids.resize(static_cast<std::size_t>(filled) / sizeof(pid_t));
    mach_timebase_info_data_t timebase{};
    mach_timebase_info(&timebase);
    for (pid_t pid : pids) {
        if (pid <= 0) continue;
        ++total;
        proc_taskinfo info{};
        if (::proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &info, sizeof info) != static_cast<int>(sizeof info)) continue;
        Process p;
        p.pid = static_cast<std::uint32_t>(pid);
        std::array<char, 2 * MAXCOMLEN + 1> name{};
        if (::proc_name(pid, name.data(), static_cast<std::uint32_t>(name.size())) > 0) p.name = name.data();
        p.memory = info.pti_resident_size;
        // Task times are in Mach absolute time units.
        const std::uint64_t ticks = info.pti_total_user + info.pti_total_system;
        if (timebase.denom != 0) p.cpu_ms = ticks * timebase.numer / timebase.denom / 1'000'000;
        out.push_back(std::move(p));
    }
    return out;
}

}  // namespace paglets::platform

#endif
