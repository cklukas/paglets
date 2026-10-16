// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-worker: runs paglet instances for a paglets-host process. It is
// started by the host with two connected sockets (calls and terminations)
// and ends when the host closes them. On Linux it runs in a seccomp sandbox.
//
//   paglets-worker <doorbell-read> <doorbell-write> <control-read> <ring-memory> <ring-capacity> [--no-sandbox]
//
// The values are file descriptors (POSIX) or inherited HANDLEs (Windows).
//   paglets-worker --check-sandbox      verify the sandbox (exit code 0: denials work)

#include "runtime_exec.hpp"
#include "sandbox.hpp"

#include <cstdlib>
#include <cstring>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#include <sddl.h>
#include <filesystem>
#include <thread>
#include <mutex>
#include <condition_variable>
#endif

#ifdef _WIN32
namespace {
int check_child(int argc, char** argv) {
    if (argc != 5) return 2;
    const auto handle = static_cast<std::intptr_t>(std::strtoll(argv[2], nullptr, 10));
    std::wstring path;
    const std::string hex = argv[3];
    if (hex.size() % 4) return 2;
    for (std::size_t i = 0; i < hex.size(); i += 4) {
        unsigned v = 0;
        for (std::size_t j = 0; j < 4; ++j) {
            const auto c = hex[i + j];
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return 2;
            v = (v << 4) | (c <= '9' ? c - '0' : c - 'a' + 10);
        }
        path += static_cast<wchar_t>(v);
    }
    // Match normal worker startup: WAMR initializes Winsock and entropy
    // before the worker removes its remaining token privileges.
    paglets::wasm::ensure_runtime();
    std::uint8_t warm[8]; paglets::wasm::fill_random(warm);
    std::string error;
    // Windows may create its own hidden console even with null launch handles.
    // The parent replaces all of its standard handles with the controlled
    // disk file, so a disk handle here would demonstrate actual inheritance.
    for (DWORD id : {STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE}) {
        HANDLE h = GetStdHandle(id);
        if (h && h != INVALID_HANDLE_VALUE && GetFileType(h) == FILE_TYPE_DISK)
            error = "a host standard file was inherited";
    }
    wchar_t secret[2]; SetLastError(ERROR_SUCCESS);
    if (GetEnvironmentVariableW(L"PAGLETS_TEST_SECRET", secret, 2) || GetLastError() != ERROR_ENVVAR_NOT_FOUND)
        error = "a service secret was inherited from the host environment";
    if (error.empty()) {
        if (auto network = paglets::runtime::check_windows_network(); !network) error = network.error();
    }
    if (auto s = paglets::runtime::sandbox_worker(); !s) error = s.error();
    if (error.empty()) {
        if (auto s = paglets::runtime::check_sandbox(); !s) error = s.error();
    }
    if (error.empty()) {
        HANDLE file = CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                   nullptr, OPEN_EXISTING, 0, nullptr);
        const DWORD e = GetLastError();
        if (file != INVALID_HANDLE_VALUE) { CloseHandle(file); error = "reading a host file is allowed"; }
        else if (e != ERROR_ACCESS_DENIED) error = "read denial probe failed for an unrelated reason: " + std::to_string(e);
    }
    if (error.empty()) {
        const auto write_path = path + L".write";
        HANDLE file = CreateFileW(write_path.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW, FILE_FLAG_DELETE_ON_CLOSE, nullptr);
        const DWORD e = GetLastError();
        if (file != INVALID_HANDLE_VALUE) { CloseHandle(file); error = "creating files in the host's temp directory is allowed"; }
        else if (e != ERROR_ACCESS_DENIED) error = "write denial probe failed for an unrelated reason: " + std::to_string(e);
    }
    if (error.empty()) {
        HANDLE parent = OpenProcess(PROCESS_VM_READ | PROCESS_VM_WRITE, FALSE, static_cast<DWORD>(std::strtoul(argv[4], nullptr, 10)));
        if (parent) { CloseHandle(parent); error = "opening the host's memory is allowed"; }
        else if (GetLastError() != ERROR_ACCESS_DENIED) error = "host memory probe did not return access denied";
    }
    const std::string report = error.empty() ? "sandbox ok" : "sandbox incomplete: " + error;
    paglets::ipc::Channel channel({-1, handle});
    (void)channel.send({reinterpret_cast<const std::uint8_t*>(report.data()), report.size()});
    std::_Exit(error.empty() ? 0 : 1);
}
int check_windows_worker() {
    std::wstring executable(32768, L'\0');
    const DWORD n = GetModuleFileNameW(nullptr, executable.data(), static_cast<DWORD>(executable.size()));
    if (!n || n >= executable.size()) return 1;
    executable.resize(n);
    const auto probe = std::filesystem::temp_directory_path() /
        (L"paglets-sandbox-read-" + std::to_wstring(GetCurrentProcessId()) + L"-" + std::to_wstring(GetTickCount64()));
    PSECURITY_DESCRIPTOR descriptor = nullptr;
    // An ordinary AppContainer could read this; an LPAC must disregard AC.
    if (!ConvertStringSecurityDescriptorToSecurityDescriptorW(L"D:P(A;;GA;;;WD)(A;;GR;;;AC)",
                                                               SDDL_REVISION_1, &descriptor, nullptr)) return 1;
    SECURITY_ATTRIBUTES attributes{sizeof(attributes), descriptor, FALSE};
    HANDLE file = CreateFileW(probe.c_str(), GENERIC_READ | GENERIC_WRITE | DELETE, FILE_SHARE_READ, &attributes,
                               CREATE_NEW, FILE_FLAG_DELETE_ON_CLOSE, nullptr);
    LocalFree(descriptor);
    if (file == INVALID_HANDLE_VALUE) return 1;
    struct Close { HANDLE h; ~Close() { CloseHandle(h); } } close{file};
    DWORD wrote = 0;
    if (!WriteFile(file, "controlled sandbox probe", 24, &wrote, nullptr) || wrote != 24) return 1;
    if (!SetEnvironmentVariableW(L"PAGLETS_TEST_SECRET", L"controlled sentinel")) return 1;
    auto pair = paglets::ipc::duplex_pair();
    if (!pair) return 1;
    auto job = paglets::ipc::create_worker_job();
    if (!job) { paglets::ipc::close_ends(pair->host); paglets::ipc::close_ends(pair->child); return 1; }
    Close job_close{reinterpret_cast<HANDLE>(*job)};
    std::string hex;
    for (wchar_t c : probe.native()) for (int shift : {12, 8, 4, 0}) hex += "0123456789abcdef"[(c >> shift) & 15];
    // Make every host standard handle the controlled inheritable file.
    struct RestoreStandard {
        HANDLE values[3]{GetStdHandle(STD_INPUT_HANDLE), GetStdHandle(STD_OUTPUT_HANDLE), GetStdHandle(STD_ERROR_HANDLE)};
        void restore() {
            SetStdHandle(STD_INPUT_HANDLE, values[0]); SetStdHandle(STD_OUTPUT_HANDLE, values[1]);
            SetStdHandle(STD_ERROR_HANDLE, values[2]);
        }
        ~RestoreStandard() { restore(); }
    } standard;
    if (!SetHandleInformation(file, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) ||
        !SetStdHandle(STD_INPUT_HANDLE, file) || !SetStdHandle(STD_OUTPUT_HANDLE, file) ||
        !SetStdHandle(STD_ERROR_HANDLE, file)) return 1;
    auto child = paglets::ipc::Process::spawn(executable,
        {"--check-sandbox-child", std::to_string(pair->child.write), hex, std::to_string(GetCurrentProcessId())},
        {pair->child.write}, *job, true);
    standard.restore();
    paglets::ipc::close_ends(pair->child);
    paglets::ipc::Channel channel(pair->host);
    if (!child) { std::cerr << child.error() << "\n"; return 1; }
    std::mutex mu;
    std::condition_variable_any cv;
    std::jthread watchdog([&](std::stop_token stop) {
        std::unique_lock lock(mu);
        cv.wait_for(lock, stop, std::chrono::seconds(15), [] { return false; });
        if (!stop.stop_requested()) child->stop(std::chrono::milliseconds(0));
    });
    auto report = channel.receive();
    watchdog.request_stop(); cv.notify_all(); watchdog.join();
    child->stop(std::chrono::seconds(1));
    if (!report) { std::cerr << "sandbox child: " << report.error() << "\n"; return 1; }
    const std::string text(report->begin(), report->end());
    std::cout << text << "\n";
    return text == "sandbox ok" ? 0 : 1;
}
}  // namespace
#endif

int main(int argc, char** argv) {
#ifdef _WIN32
    if (argc > 1 && std::strcmp(argv[1], "--check-sandbox-child") == 0) return check_child(argc, argv);
    if (argc == 2 && std::strcmp(argv[1], "--check-sandbox") == 0) return check_windows_worker();
#endif
    if (argc == 2 && std::strcmp(argv[1], "--check-sandbox") == 0) {
        if (auto ok = paglets::runtime::sandbox_worker(); !ok) {
            std::cerr << "paglets-worker: " << ok.error() << "\n";
            return 1;
        }
        if (auto ok = paglets::runtime::check_sandbox(); !ok) {
            std::cerr << "paglets-worker: sandbox incomplete: " << ok.error() << "\n";
            return 1;
        }
        std::cout << "sandbox ok" << std::endl;
        // Exit handlers (for example LeakSanitizer's) may need what the
        // sandbox refuses; workers leave the same way.
        std::_Exit(0);
    }
    // <doorbell-read> <doorbell-write> <control-read> <ring-memory> <ring-capacity> [--no-sandbox]
    const bool sandbox = !(argc == 7 && std::strcmp(argv[6], "--no-sandbox") == 0);
    if (argc != 6 && !(argc == 7 && !sandbox)) {
        std::cerr << "paglets-worker is started by paglets-host\n";
        return 2;
    }
    auto number = [&](int i) { return static_cast<std::intptr_t>(std::strtoll(argv[i], nullptr, 10)); };
    return paglets::runtime::run_worker(paglets::ipc::Ends{number(1), number(2)}, paglets::ipc::Ends{number(3), -1},
                                        number(4), static_cast<std::size_t>(number(5)), sandbox);
}
