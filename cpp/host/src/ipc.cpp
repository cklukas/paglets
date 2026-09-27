// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "ipc.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
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
    : pid_(std::exchange(other.pid_, -1)), handle_(std::exchange(other.handle_, -1)) {}

Process& Process::operator=(Process&& other) noexcept {
    if (this != &other) {
        stop(std::chrono::milliseconds(0));
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
                                                   const std::vector<std::intptr_t>& handles, std::intptr_t job) {
    Process p;
#ifdef _WIN32
    std::vector<HANDLE> inherit;
    for (auto h : handles) {
        if (std::find(inherit.begin(), inherit.end(), to_handle(h)) != inherit.end()) continue;
        if (!SetHandleInformation(to_handle(h), HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT)) {
            return std::unexpected(last_error("SetHandleInformation"));
        }
        inherit.push_back(to_handle(h));
    }
    SIZE_T size = 0;
    InitializeProcThreadAttributeList(nullptr, 1, 0, &size);
    std::vector<std::uint8_t> attribute_storage(size);
    auto* attributes = reinterpret_cast<LPPROC_THREAD_ATTRIBUTE_LIST>(attribute_storage.data());
    if (!InitializeProcThreadAttributeList(attributes, 1, 0, &size)) {
        return std::unexpected(last_error("InitializeProcThreadAttributeList"));
    }
    // Only the listed handles are inherited, whatever else is inheritable.
    const bool listed = UpdateProcThreadAttribute(attributes, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, inherit.data(),
                                                  inherit.size() * sizeof(HANDLE), nullptr, nullptr);
    if (!listed) {
        DeleteProcThreadAttributeList(attributes);
        return std::unexpected(last_error("UpdateProcThreadAttribute"));
    }
    STARTUPINFOEXW si{};
    si.StartupInfo.cb = sizeof si;
    si.lpAttributeList = attributes;
    std::wstring cmd;
    append_quoted(cmd, executable.wstring());
    for (const auto& a : args) append_quoted(cmd, std::wstring(a.begin(), a.end()));
    PROCESS_INFORMATION pi{};
    const BOOL created = CreateProcessW(executable.wstring().c_str(), cmd.data(), nullptr, nullptr, TRUE,
                                        EXTENDED_STARTUPINFO_PRESENT | CREATE_SUSPENDED | CREATE_NO_WINDOW, nullptr,
                                        nullptr, &si.StartupInfo, &pi);
    DeleteProcThreadAttributeList(attributes);
    for (HANDLE h : inherit) SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0);
    if (!created) return std::unexpected(last_error("CreateProcess"));
    if (job >= 0 && !AssignProcessToJobObject(to_handle(job), pi.hProcess)) {
        const std::string error = last_error("AssignProcessToJobObject");
        TerminateProcess(pi.hProcess, 1);
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);
        return std::unexpected(error);
    }
    ResumeThread(pi.hThread);
    CloseHandle(pi.hThread);
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
        const int copy = ::fcntl(static_cast<int>(h), F_DUPFD_CLOEXEC, 64);
        if (copy < 0) {
            close_sources();
            return std::unexpected(std::string("cannot pass a descriptor: ") + std::strerror(errno));
        }
        sources.push_back(copy);
    }
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    for (std::size_t i = 0; i < sources.size(); ++i) {
        posix_spawn_file_actions_adddup2(&actions, sources[i], static_cast<int>(3 + i));
    }
    const std::string exe = executable.string();
    std::vector<std::string> storage(args.begin(), args.end());
    std::vector<char*> argv{const_cast<char*>(exe.c_str())};
    for (auto& a : storage) argv.push_back(a.data());
    argv.push_back(nullptr);
    pid_t pid = -1;
    const int rc = ::posix_spawn(&pid, exe.c_str(), &actions, nullptr, argv.data(), environ);
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
