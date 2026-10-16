// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "ipc.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <thread>
#include <cstdlib>
#include <limits>

#ifdef _WIN32
#include <windows.h>
#include <map>
#include "windows_sandbox.hpp"
#else
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace paglets::ipc {

namespace {

#ifdef _WIN32
HANDLE to_handle(std::intptr_t h) {
    return reinterpret_cast<HANDLE>(h);
}
std::intptr_t from_handle(HANDLE h) {
    return reinterpret_cast<std::intptr_t>(h);
}
std::string last_error(const char* what) {
    return std::string(what) + " failed (error " + std::to_string(GetLastError()) + ")";
}

// Command-line quoting as parsed by CommandLineToArgvW and the C runtime.
void append_quoted(std::wstring& cmd, const std::wstring& arg) {
    if (!cmd.empty()) cmd.push_back(L' ');
    if (!arg.empty() && arg.find_first_of(L" \t\n\v\"") == std::wstring::npos) {
        cmd += arg;
        return;
    }
    cmd.push_back(L'"');
    std::size_t backslashes = 0;
    for (wchar_t c : arg) {
        if (c == L'\\') {
            ++backslashes;
        } else if (c == L'"') {
            cmd.append(backslashes * 2 + 1, L'\\');
            cmd.push_back(c);
            backslashes = 0;
        } else {
            cmd.append(backslashes, L'\\');
            cmd.push_back(c);
            backslashes = 0;
        }
    }
    cmd.append(backslashes * 2, L'\\');
    cmd.push_back(L'"');
}
#else
#ifdef MSG_NOSIGNAL
constexpr int send_flags = MSG_NOSIGNAL;
#else
constexpr int send_flags = 0;  // macOS: SO_NOSIGPIPE is set on the socket
#endif
#endif

}  // namespace

// ---------------------------------------------------------------------------
// Byte streams

void close_handle(std::intptr_t handle) {
    if (handle < 0) return;
#ifdef _WIN32
    CloseHandle(to_handle(handle));
#else
    ::close(static_cast<int>(handle));
#endif
}

std::expected<std::intptr_t, std::string> duplicate_handle(std::intptr_t handle) {
#ifdef _WIN32
    HANDLE copy = nullptr;
    if (!DuplicateHandle(GetCurrentProcess(), to_handle(handle), GetCurrentProcess(), &copy, 0, FALSE,
                         DUPLICATE_SAME_ACCESS)) {
        return std::unexpected(last_error("DuplicateHandle"));
    }
    return from_handle(copy);
#else
    const int copy = ::fcntl(static_cast<int>(handle), F_DUPFD_CLOEXEC, 0);
    if (copy < 0) return std::unexpected(std::string("dup: ") + std::strerror(errno));
    return copy;
#endif
}

void close_ends(Ends& ends) {
    if (ends.write != ends.read) close_handle(ends.write);
    close_handle(ends.read);
    ends = Ends{};
}

bool write_all(std::intptr_t handle, const std::uint8_t* p, std::size_t n) {
    while (n > 0) {
#ifdef _WIN32
        DWORD written = 0;
        const DWORD chunk = static_cast<DWORD>(std::min<std::size_t>(n, 1u << 30));
        if (!WriteFile(to_handle(handle), p, chunk, &written, nullptr)) return false;
#else
        const ssize_t written = ::send(static_cast<int>(handle), p, n, send_flags);
        if (written < 0) {
            if (errno == EINTR) continue;
            return false;
        }
#endif
        p += written;
        n -= static_cast<std::size_t>(written);
    }
    return true;
}

bool read_all(std::intptr_t handle, std::uint8_t* p, std::size_t n) {
    while (n > 0) {
#ifdef _WIN32
        DWORD got = 0;
        const DWORD chunk = static_cast<DWORD>(std::min<std::size_t>(n, 1u << 30));
        if (!ReadFile(to_handle(handle), p, chunk, &got, nullptr) || got == 0) return false;
#else
        const ssize_t got = ::recv(static_cast<int>(handle), p, n, 0);
        if (got == 0) return false;
        if (got < 0) {
            if (errno == EINTR) continue;
            return false;
        }
#endif
        p += got;
        n -= static_cast<std::size_t>(got);
    }
    return true;
}

bool readable_or_closed(std::intptr_t read_handle) {
    if (read_handle < 0) return true;
#ifdef _WIN32
    DWORD available = 0;
    return !PeekNamedPipe(to_handle(read_handle), nullptr, 0, nullptr, &available, nullptr) || available > 0;
#else
    pollfd p{static_cast<int>(read_handle), POLLIN, 0};
    return ::poll(&p, 1, 0) > 0 && (p.revents & (POLLIN | POLLHUP | POLLERR)) != 0;
#endif
}

std::expected<DuplexPair, std::string> duplex_pair() {
#ifdef _WIN32
    // Two pipes: host -> child and child -> host. Nothing is inheritable
    // until Process::spawn passes the child's ends explicitly.
    HANDLE down_read = nullptr;
    HANDLE down_write = nullptr;
    HANDLE up_read = nullptr;
    HANDLE up_write = nullptr;
    if (!CreatePipe(&down_read, &down_write, nullptr, 0)) return std::unexpected(last_error("CreatePipe"));
    if (!CreatePipe(&up_read, &up_write, nullptr, 0)) {
        CloseHandle(down_read);
        CloseHandle(down_write);
        return std::unexpected(last_error("CreatePipe"));
    }
    return DuplexPair{Ends{from_handle(up_read), from_handle(down_write)},
                      Ends{from_handle(down_read), from_handle(up_write)}};
#else
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return std::unexpected(std::string("socketpair: ") + std::strerror(errno));
    }
    for (int fd : fds) {
        ::fcntl(fd, F_SETFD, FD_CLOEXEC);
#ifdef SO_NOSIGPIPE
        const int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
    }
    return DuplexPair{Ends{fds[0], fds[0]}, Ends{fds[1], fds[1]}};
#endif
}

// ---------------------------------------------------------------------------
// Channel

Channel& Channel::operator=(Channel&& other) noexcept {
    if (this != &other) {
        close();
        ends_ = std::exchange(other.ends_, Ends{});
    }
    return *this;
}

Channel::~Channel() {
    close();
}

void Channel::close() {
    close_ends(ends_);
}

std::expected<void, std::string> Channel::send(std::span<const std::uint8_t> frame) {
    if (ends_.write < 0) return std::unexpected(std::string("ipc: channel closed"));
    if (frame.size() > max_frame) return std::unexpected(std::string("ipc: frame too large"));
    const auto n = static_cast<std::uint32_t>(frame.size());
    const std::array<std::uint8_t, 4> header{static_cast<std::uint8_t>(n), static_cast<std::uint8_t>(n >> 8),
                                             static_cast<std::uint8_t>(n >> 16), static_cast<std::uint8_t>(n >> 24)};
    if (!write_all(ends_.write, header.data(), header.size()) || !write_all(ends_.write, frame.data(), frame.size())) {
        return std::unexpected(std::string("ipc: peer closed the connection"));
    }
    return {};
}

std::expected<std::vector<std::uint8_t>, std::string> Channel::receive() {
    if (ends_.read < 0) return std::unexpected(std::string("ipc: channel closed"));
    std::array<std::uint8_t, 4> header{};
    if (!read_all(ends_.read, header.data(), header.size())) {
        return std::unexpected(std::string("ipc: peer closed the connection"));
    }
    const std::uint32_t n = header[0] | (header[1] << 8) | (header[2] << 16) | (std::uint32_t{header[3]} << 24);
    if (n > max_frame) return std::unexpected(std::string("ipc: frame too large"));
    std::vector<std::uint8_t> frame(n);
    if (!read_all(ends_.read, frame.data(), n)) return std::unexpected(std::string("ipc: peer closed the connection"));
    return frame;
}

// ---------------------------------------------------------------------------
// Processes

Process::Process(Process&& other) noexcept
    : sandbox_(std::move(other.sandbox_)), pid_(std::exchange(other.pid_, -1)), handle_(std::exchange(other.handle_, -1)) {}

Process& Process::operator=(Process&& other) noexcept {
    if (this != &other) {
        stop(std::chrono::milliseconds(0));
        sandbox_ = std::move(other.sandbox_);
        pid_ = std::exchange(other.pid_, -1);
        handle_ = std::exchange(other.handle_, -1);
    }
    return *this;
}

Process::~Process() {
    stop(std::chrono::milliseconds(0));
}

std::vector<std::intptr_t> Process::child_values(const std::vector<std::intptr_t>& handles) {
    std::vector<std::intptr_t> values;
    for (std::size_t i = 0; i < handles.size(); ++i) {
#ifdef _WIN32
        values.push_back(handles[i]);
#else
        values.push_back(static_cast<std::intptr_t>(3 + i));
#endif
    }
    return values;
}

std::expected<Process, std::string> Process::spawn(const std::filesystem::path& executable,
                                                   const std::vector<std::string>& args,
                                                   const std::vector<std::intptr_t>& handles, std::intptr_t job, bool sandbox) {
    Process p;
    if (handles.size() > static_cast<std::size_t>(std::numeric_limits<int>::max() - 3))
        return std::unexpected(std::string("too many child handles"));
#ifdef _WIN32
    std::shared_ptr<WindowsSandbox> isolated;
    auto actual_executable = executable;
    if (sandbox) {
        auto prepared = prepare_windows_sandbox(executable, job);
        if (!prepared) return std::unexpected(prepared.error());
        isolated = *prepared;
        actual_executable = isolated->executable;
    }
    std::vector<HANDLE> inherit;
    std::vector<std::uint8_t> attribute_storage;
    struct Attributes {
        std::vector<HANDLE>& handles;
        LPPROC_THREAD_ATTRIBUTE_LIST list = nullptr;
        ~Attributes() {
            if (list) DeleteProcThreadAttributeList(list);
            for (HANDLE h : handles) SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0);
        }
    } cleanup{inherit};
    for (auto h : handles) {
        if (std::find(inherit.begin(), inherit.end(), to_handle(h)) != inherit.end()) continue;
        if (!SetHandleInformation(to_handle(h), HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)) {
            return std::unexpected(last_error("SetHandleInformation"));
        }
        inherit.push_back(to_handle(h));
    }
    const DWORD count = (inherit.empty() ? 0 : 1) + (sandbox ? 2 : 0);
    SIZE_T size = 0;
    InitializeProcThreadAttributeList(nullptr, count, 0, &size);
    attribute_storage.resize(size);
    auto* attributes = reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>(attribute_storage.data());
    if (count && !InitializeProcThreadAttributeList(attributes, count, 0, &size)) {
        return std::unexpected(last_error("InitializeProcThreadAttributeList"));
    }
    // Only the listed handles are inherited, whatever else is inheritable.
    cleanup.list = count ? attributes : nullptr;
    const bool listed = inherit.empty() || UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherit.data(),
                                                  inherit.size() * sizeof(HANDLE), nullptr, nullptr);
    if (!listed) {
        return std::unexpected(last_error("UpdateProcThreadAttribute"));
    }
    SECURITY_CAPABILITIES caps{};
    SID_AND_ATTRIBUTES registry{};
    ULONG all_packages_policy = PROCESS_CREATION_ALL_APPLICATION_PACKAGES_OPT_OUT;
    if (isolated) {
        registry.Sid = isolated->registry.data(); registry.Attributes = SE_GROUP_ENABLED;
        caps.AppContainerSid = isolated->sid.data(); caps.Capabilities = &registry; caps.CapabilityCount = 1;
        if (!UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
                                       &caps, sizeof caps, nullptr, nullptr) ||
            !UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_ALL_APPLICATION_PACKAGES_POLICY,
                                       &all_packages_policy, sizeof all_packages_policy, nullptr, nullptr))
            return std::unexpected(last_error("AppContainer attributes"));
    }
    STARTUPINFOEXW si{};
    si.StartupInfo.cb = count ? sizeof si : sizeof(STARTUPINFOW);
    si.lpAttributeList = count ? attributes : nullptr;
    if (sandbox) si.StartupInfo.dwFlags |= STARTF_USESTDHANDLES;  // all three remain null
    std::wstring cmd;
    append_quoted(cmd, actual_executable.wstring());
    for (const auto& a : args) {
        const int n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, a.data(), static_cast<int>(a.size()), nullptr, 0);
        if (!a.empty() && n == 0) return std::unexpected(last_error("UTF-8 worker argument"));
        std::wstring wide(n, L'\0');
        if (n) MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, a.data(), static_cast<int>(a.size()), wide.data(), n);
        append_quoted(cmd, wide);
    }
    PROCESS_INFORMATION pi{};
    std::map<std::wstring, std::wstring> environment;
    for (const wchar_t* name : {L"SYSTEMROOT", L"SYSTEMDRIVE", L"WINDIR", L"USERPROFILE", L"LOCALAPPDATA", L"APPDATA", L"PATH", L"TEMP", L"TMP", L"TMPDIR", L"LANG", L"LC_ALL", L"TZ",
                                 L"ASAN_OPTIONS", L"UBSAN_OPTIONS", L"TSAN_OPTIONS", L"LSAN_OPTIONS"}) {
        const DWORD n = GetEnvironmentVariableW(name, nullptr, 0);
        if (n) {
            std::wstring value(n, L'\0');
            const DWORD copied = GetEnvironmentVariableW(name, value.data(), n);
            if (copied >= n) return std::unexpected(std::string("worker environment changed during launch"));
            value.resize(copied); environment.emplace(name, std::move(value));
        }
    }
    if (isolated) {
        wchar_t system[32768];
        const UINT n = GetSystemDirectoryW(system, 32768);
        if (!n || n >= 32768) return std::unexpected(last_error("system DLL directory"));
        environment[L"PATH"] = actual_executable.parent_path().wstring() + L";" + system;
    }
    std::wstring block;
    for (const auto& [key, value] : environment) { block += key + L"=" + value; block += L'\0'; }
    block += L'\0';
    if (environment.empty()) block += L'\0';
    const auto directory = actual_executable.parent_path();
    const BOOL created = CreateProcessW(actual_executable.wstring().c_str(), cmd.data(), nullptr, nullptr, !inherit.empty(),
                                        (count ? EXTENDED_STARTUPINFO_PRESENT : 0) | CREATE_SUSPENDED | CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT, block.data(),
                                        isolated ? directory.c_str() : nullptr, &si.StartupInfo, &pi);
    if (!created) return std::unexpected(last_error("CreateProcess"));
    if (job >= 0 && !AssignProcessToJobObject(to_handle(job), pi.hProcess)) {
        const std::string error = last_error("AssignProcessToJobObject");
        TerminateProcess(pi.hProcess, 1);
        WaitForSingleObject(pi.hProcess, INFINITE);
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);
        return std::unexpected(error);
    }
    if (ResumeThread(pi.hThread) == static_cast<DWORD>(-1)) {
        const auto error = last_error("ResumeThread");
        TerminateProcess(pi.hProcess, 1); WaitForSingleObject(pi.hProcess, INFINITE);
        CloseHandle(pi.hThread); CloseHandle(pi.hProcess);
        return std::unexpected(error);
    }
    CloseHandle(pi.hThread);
    p.sandbox_ = std::move(isolated);
    p.pid_ = static_cast<int>(pi.dwProcessId);
    p.handle_ = from_handle(pi.hProcess);
#else
    (void)job;
    // The child's descriptors 3, 4, ... are filled from copies above them,
    // so no source is overwritten by an earlier dup2.
    std::vector<int> sources;
    auto close_sources = [&] {
        for (int fd : sources) ::close(fd);
    };
    for (auto h : handles) {
        const int copy = ::fcntl(static_cast<int>(h), F_DUPFD_CLOEXEC, std::max(64, static_cast<int>(3 + handles.size())));
        if (copy < 0) {
            close_sources();
            return std::unexpected(std::string("cannot pass a descriptor: ") + std::strerror(errno));
        }
        sources.push_back(copy);
    }
    posix_spawn_file_actions_t actions;
    int rc = posix_spawn_file_actions_init(&actions);
    if (rc) { close_sources(); return std::unexpected(std::string("spawn file actions: ") + std::strerror(rc)); }
    auto action = [&](int result) { if (!rc) rc = result; };
    for (int fd = 0; fd < 3; ++fd) {
        if (sandbox || ::fcntl(fd, F_GETFD) < 0)
            action(posix_spawn_file_actions_addopen(&actions, fd, "/dev/null", fd == 0 ? O_RDONLY : O_WRONLY, 0));
        else action(posix_spawn_file_actions_adddup2(&actions, fd, fd));
    }
    for (std::size_t i = 0; i < sources.size(); ++i)
        action(posix_spawn_file_actions_adddup2(&actions, sources[i], static_cast<int>(3 + i)));
    posix_spawnattr_t attributes;
    const int initialized = posix_spawnattr_init(&attributes);
    if (!rc) rc = initialized;
#ifdef __APPLE__
    if (!initialized) action(posix_spawnattr_setflags(&attributes, POSIX_SPAWN_CLOEXEC_DEFAULT));
#else
    action(posix_spawn_file_actions_addclosefrom_np(&actions, static_cast<int>(3 + sources.size())));
#endif
    std::vector<std::string> environment;
    for (const char* name : {"PATH", "TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "TZ", "LD_LIBRARY_PATH",
                             "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "DYLD_FRAMEWORK_PATH",
                             "ASAN_OPTIONS", "UBSAN_OPTIONS", "TSAN_OPTIONS", "LSAN_OPTIONS"}) {
        if (const char* value = std::getenv(name)) environment.push_back(std::string(name) + "=" + value);
    }
    std::vector<char*> env;
    for (auto& entry : environment) env.push_back(entry.data());
    env.push_back(nullptr);
    const std::string exe = executable.string();
    std::vector<std::string> storage(args.begin(), args.end());
    std::vector<char*> argv{const_cast<char*>(exe.c_str())};
    for (auto& a : storage) argv.push_back(a.data());
    argv.push_back(nullptr);
    pid_t pid = -1;
    if (!rc) rc = ::posix_spawn(&pid, exe.c_str(), &actions, &attributes, argv.data(), env.data());
    if (!initialized) posix_spawnattr_destroy(&attributes);
    posix_spawn_file_actions_destroy(&actions);
    close_sources();
    if (rc != 0) return std::unexpected("cannot start " + exe + ": " + std::strerror(rc));
    p.pid_ = pid;
#endif
    return p;
}

bool Process::exited() {
    if (pid_ <= 0) return true;
#ifdef _WIN32
    if (WaitForSingleObject(to_handle(handle_), 0) != WAIT_OBJECT_0) return false;
    CloseHandle(to_handle(handle_));
    handle_ = -1;
#else
    int status = 0;
    const pid_t r = ::waitpid(pid_, &status, WNOHANG);
    if (r == 0) return false;
#endif
    pid_ = -1;
    sandbox_.reset();
    return true;
}

void Process::stop(std::chrono::milliseconds grace) {
    if (pid_ <= 0) return;
    const auto until = std::chrono::steady_clock::now() + grace;
    while (!exited()) {
        if (std::chrono::steady_clock::now() >= until) {
#ifdef _WIN32
            TerminateProcess(to_handle(handle_), 1);
            WaitForSingleObject(to_handle(handle_), INFINITE);
            CloseHandle(to_handle(handle_));
            handle_ = -1;
#else
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
#endif
            pid_ = -1;
            sandbox_.reset();
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

std::expected<std::intptr_t, std::string> create_worker_job() {
#ifdef _WIN32
    HANDLE job = CreateJobObjectW(nullptr, nullptr);
    if (job == nullptr) return std::unexpected(last_error("CreateJobObject"));
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_ACTIVE_PROCESS |
                                              JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
    limits.BasicLimitInformation.ActiveProcessLimit = 1;  // the worker cannot start processes
    JOBOBJECT_BASIC_UI_RESTRICTIONS ui{};
    ui.UIRestrictionsClass = JOB_OBJECT_UILIMIT_DESKTOP | JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
                             JOB_OBJECT_UILIMIT_EXITWINDOWS | JOB_OBJECT_UILIMIT_GLOBALATOMS |
                             JOB_OBJECT_UILIMIT_HANDLES | JOB_OBJECT_UILIMIT_READCLIPBOARD |
                             JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS | JOB_OBJECT_UILIMIT_WRITECLIPBOARD;
    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof limits) ||
        !SetInformationJobObject(job, JobObjectBasicUIRestrictions, &ui, sizeof ui)) {
        const std::string error = last_error("SetInformationJobObject");
        CloseHandle(job);
        return std::unexpected(error);
    }
    return from_handle(job);
#else
    return -1;
#endif
}

}  // namespace paglets::ipc
