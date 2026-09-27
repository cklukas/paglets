// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Platform layer for Windows: RtlGetVersion and per-processor times from
// ntdll, memory status, logical drives, ToolHelp process snapshots.

#include "platform.hpp"

#if defined(_WIN32)

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
// After windows.h:
#include <psapi.h>
#include <tlhelp32.h>

#include <cwchar>
#include <string>

namespace paglets::platform {

namespace {

std::string narrow(const wchar_t* text, int length = -1) {
    if (text == nullptr) return {};
    const int n = WideCharToMultiByte(CP_UTF8, 0, text, length, nullptr, 0, nullptr, nullptr);
    if (n <= 0) return {};
    std::string out(static_cast<std::size_t>(n), '\0');
    WideCharToMultiByte(CP_UTF8, 0, text, length, out.data(), n, nullptr, nullptr);
    if (length < 0 && !out.empty() && out.back() == '\0') out.pop_back();
    return out;
}

std::uint64_t to_u64(const FILETIME& t) {
    return (static_cast<std::uint64_t>(t.dwHighDateTime) << 32) | t.dwLowDateTime;
}

// ntdll entry points without import libraries.
struct OsVersion {
    ULONG size;
    ULONG major;
    ULONG minor;
    ULONG build;
    ULONG platform;
    WCHAR service_pack[128];
};
using RtlGetVersionFn = LONG(WINAPI*)(OsVersion*);

struct ProcessorTimes {
    LARGE_INTEGER idle;
    LARGE_INTEGER kernel;  // includes idle
    LARGE_INTEGER user;
    LARGE_INTEGER dpc;
    LARGE_INTEGER interrupt;
    ULONG interrupt_count;
};
constexpr int system_processor_performance_information = 8;
using NtQuerySystemInformationFn = LONG(WINAPI*)(int, void*, ULONG, ULONG*);

FARPROC ntdll(const char* name) {
    HMODULE module = GetModuleHandleW(L"ntdll.dll");
    return module == nullptr ? nullptr : GetProcAddress(module, name);
}

std::string cpu_model() {
    HKEY key = nullptr;
    if (RegOpenKeyExW(HKEY_LOCAL_MACHINE, L"HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", 0, KEY_READ, &key) !=
        ERROR_SUCCESS) {
        return {};
    }
    wchar_t value[256] = {};
    DWORD size = sizeof value - sizeof(wchar_t);
    DWORD type = 0;
    std::string model;
    if (RegQueryValueExW(key, L"ProcessorNameString", nullptr, &type, reinterpret_cast<BYTE*>(value), &size) ==
            ERROR_SUCCESS &&
        type == REG_SZ) {
        model = narrow(value);
        const auto b = model.find_first_not_of(' ');
        model = b == std::string::npos ? std::string() : model.substr(b);
    }
    RegCloseKey(key);
    return model;
}

}  // namespace

Summary summary() {
    Summary s;
    s.os = "windows";
    if (auto get = reinterpret_cast<RtlGetVersionFn>(reinterpret_cast<void*>(ntdll("RtlGetVersion")))) {
        OsVersion v{};
        v.size = sizeof v;
        if (get(&v) == 0) {
            s.os_version =
                "Windows " + std::to_string(v.major) + "." + std::to_string(v.minor) + "." + std::to_string(v.build);
        }
    }
    wchar_t host[256] = {};
    DWORD host_size = 255;
    if (GetComputerNameExW(ComputerNameDnsHostname, host, &host_size)) s.host_name = narrow(host);
    SYSTEM_INFO info{};
    GetNativeSystemInfo(&info);
    switch (info.wProcessorArchitecture) {
        case PROCESSOR_ARCHITECTURE_AMD64: s.architecture = "x86_64"; break;
        case PROCESSOR_ARCHITECTURE_ARM64: s.architecture = "arm64"; break;
        case PROCESSOR_ARCHITECTURE_INTEL: s.architecture = "x86"; break;
        default: s.architecture = "unknown"; break;
    }
    s.cpu_model = cpu_model();
    s.cpu_count = static_cast<std::uint32_t>(GetActiveProcessorCount(ALL_PROCESSOR_GROUPS));
    MEMORYSTATUSEX status{};
    status.dwLength = sizeof status;
    if (GlobalMemoryStatusEx(&status)) s.memory_total = status.ullTotalPhys;
    s.uptime_s = GetTickCount64() / 1000;
    return s;
}

CpuSample cpu_sample() {
    CpuSample sample;
    auto query =
        reinterpret_cast<NtQuerySystemInformationFn>(reinterpret_cast<void*>(ntdll("NtQuerySystemInformation")));
    SYSTEM_INFO info{};
    GetSystemInfo(&info);
    if (query != nullptr && info.dwNumberOfProcessors > 0) {
        std::vector<ProcessorTimes> times(info.dwNumberOfProcessors);
        ULONG returned = 0;
        if (query(system_processor_performance_information, times.data(),
                  static_cast<ULONG>(times.size() * sizeof(ProcessorTimes)), &returned) == 0) {
            times.resize(returned / sizeof(ProcessorTimes));
            for (const auto& t : times) {
                CpuTimes c;
                c.total = static_cast<std::uint64_t>(t.kernel.QuadPart + t.user.QuadPart);
                c.busy = c.total - static_cast<std::uint64_t>(t.idle.QuadPart);
                sample.cores.push_back(c);
            }
        }
    }
    FILETIME idle{};
    FILETIME kernel{};
    FILETIME user{};
    if (GetSystemTimes(&idle, &kernel, &user)) {
        sample.all.total = to_u64(kernel) + to_u64(user);
        sample.all.busy = sample.all.total - to_u64(idle);
    }
    return sample;
}

Memory memory() {
    Memory m;
    MEMORYSTATUSEX status{};
    status.dwLength = sizeof status;
    if (GlobalMemoryStatusEx(&status)) {
        m.total = status.ullTotalPhys;
        m.available = status.ullAvailPhys;
    }
    return m;
}

std::optional<std::vector<double>> load_average() {
    return std::nullopt;
}

std::vector<Volume> volumes() {
    std::vector<Volume> out;
    wchar_t drives[512] = {};
    const DWORD n = GetLogicalDriveStringsW(511, drives);
    if (n == 0 || n > 511) return out;
    // Removable drives without media must not show a dialog.
    DWORD old_mode = 0;
    SetThreadErrorMode(SEM_FAILCRITICALERRORS | SEM_NOOPENFILEERRORBOX, &old_mode);
    for (const wchar_t* root = drives; *root != L'\0'; root += wcslen(root) + 1) {
        const UINT type = GetDriveTypeW(root);
        if (type == DRIVE_NO_ROOT_DIR || type == DRIVE_UNKNOWN) continue;
        ULARGE_INTEGER available{};
        ULARGE_INTEGER total{};
        ULARGE_INTEGER free{};
        if (!GetDiskFreeSpaceExW(root, &available, &total, &free)) continue;
        wchar_t label[MAX_PATH + 1] = {};
        wchar_t filesystem[MAX_PATH + 1] = {};
        DWORD flags = 0;
        Volume v;
        if (GetVolumeInformationW(root, label, MAX_PATH, nullptr, nullptr, &flags, filesystem, MAX_PATH)) {
            v.name = narrow(label);
            v.filesystem = narrow(filesystem);
            v.read_only = (flags & FILE_READ_ONLY_VOLUME) != 0;
        }
        v.mount_point = narrow(root);
        v.total = total.QuadPart;
        v.free = free.QuadPart;
        v.available = available.QuadPart;
        out.push_back(std::move(v));
    }
    SetThreadErrorMode(old_mode, nullptr);
    return out;
}

std::vector<Process> processes(std::uint32_t& total) {
    std::vector<Process> out;
    total = 0;
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    if (snapshot == INVALID_HANDLE_VALUE) return out;
    PROCESSENTRY32W entry{};
    entry.dwSize = sizeof entry;
    for (BOOL more = Process32FirstW(snapshot, &entry); more; more = Process32NextW(snapshot, &entry)) {
        ++total;
        if (entry.th32ProcessID == 0) continue;  // the idle process
        HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, entry.th32ProcessID);
        if (process == nullptr) continue;
        Process p;
        p.pid = entry.th32ProcessID;
        p.name = narrow(entry.szExeFile);
        PROCESS_MEMORY_COUNTERS counters{};
        if (GetProcessMemoryInfo(process, &counters, sizeof counters)) p.memory = counters.WorkingSetSize;
        FILETIME created{};
        FILETIME exited{};
        FILETIME kernel{};
        FILETIME user{};
        if (GetProcessTimes(process, &created, &exited, &kernel, &user)) {
            p.cpu_ms = (to_u64(kernel) + to_u64(user)) / 10'000;  // 100 ns units
        }
        CloseHandle(process);
        out.push_back(std::move(p));
    }
    CloseHandle(snapshot);
    return out;
}

}  // namespace paglets::platform

#endif
