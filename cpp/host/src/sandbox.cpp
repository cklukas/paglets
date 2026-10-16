// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "sandbox.hpp"

#include <cstdint>
#include <cstdlib>
#include <string>

#if defined(__linux__) && (defined(__x86_64__) || defined(__aarch64__))

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/sched.h>
#include <linux/seccomp.h>
#include <sched.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>
#include <thread>

namespace paglets::runtime {

namespace {

#if defined(__x86_64__)
constexpr std::uint32_t audit_arch = AUDIT_ARCH_X86_64;
#else
constexpr std::uint32_t audit_arch = AUDIT_ARCH_AARCH64;
#endif

// After startup a worker only needs memory, threads, clocks and local IPC.
// New kernel interfaces are denied automatically.
constexpr long allowed[] = {
    SYS_read, SYS_write, SYS_readv, SYS_writev, SYS_close,
    SYS_recvfrom, SYS_sendto, SYS_recvmsg, SYS_sendmsg, SYS_shutdown,
    SYS_fstat, SYS_lseek,
    // UBSan probes vtable memory through an anonymous pipe. Pipes create
    // process-local descriptors without granting file or network access.
    SYS_pipe2,
#ifdef SYS_pipe
    SYS_pipe,
#endif
    SYS_mmap, SYS_munmap, SYS_mprotect, SYS_mremap, SYS_madvise, SYS_brk,
    SYS_rt_sigaction, SYS_rt_sigprocmask, SYS_rt_sigreturn, SYS_sigaltstack,
    SYS_futex, SYS_set_robust_list, SYS_set_tid_address,
    SYS_clock_gettime, SYS_clock_getres, SYS_clock_nanosleep, SYS_nanosleep,
    SYS_gettimeofday, SYS_getrandom, SYS_getpid, SYS_gettid,
    SYS_sched_yield, SYS_sched_getaffinity, SYS_getrusage,
    SYS_ppoll, SYS_pselect6, SYS_restart_syscall, SYS_exit, SYS_exit_group,
#ifdef SYS_poll
    SYS_poll,
#endif
#ifdef SYS_select
    SYS_select,
#endif
#ifdef SYS_getrlimit
    SYS_getrlimit,
#endif
#ifdef SYS_rseq
    SYS_rseq,
#endif
#ifdef SYS_futex_waitv
    SYS_futex_waitv,
#endif
#ifdef SYS_arch_prctl
    SYS_arch_prctl,
#endif
};

constexpr std::uint32_t errno_result(int e) {
    return SECCOMP_RET_ERRNO | (static_cast<std::uint32_t>(e) & SECCOMP_RET_DATA);
}

std::vector<sock_filter> build_filter() {
    std::vector<sock_filter> f;
    auto stmt = [&](std::uint16_t code, std::uint32_t k) { f.push_back(BPF_STMT(code, k)); };
    auto jump = [&](std::uint16_t code, std::uint32_t k, std::uint8_t jt, std::uint8_t jf) {
        f.push_back(BPF_JUMP(code, k, jt, jf));
    };
    // Only the native architecture's system call numbers are meaningful.
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, arch));
    jump(BPF_JMP | BPF_JEQ | BPF_K, audit_arch, 1, 0);
    stmt(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS);
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, nr));
#if defined(__x86_64__)
    // x32 system calls share the architecture; refuse them.
    jump(BPF_JMP | BPF_JGE | BPF_K, 0x40000000u, 0, 1);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
#endif
    for (long nr : allowed) {
        jump(BPF_JMP | BPF_JEQ | BPF_K, static_cast<std::uint32_t>(nr), 0, 1);
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
    }
    // Descriptor flags/duplication are local; ownership and asynchronous
    // signal commands (F_SETOWN/F_SETSIG) are unnecessary authority.
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_fcntl, 0, 14);
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[1]));
    for (int command : {F_GETFD, F_SETFD, F_GETFL, F_SETFL, F_DUPFD, F_DUPFD_CLOEXEC}) {
        jump(BPF_JMP | BPF_JEQ | BPF_K, command, 0, 1);
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
    }
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    // Signals are needed to stop WAMR threads, but only within this worker.
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_tgkill, 0, 4);
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0]));
    jump(BPF_JMP | BPF_JEQ | BPF_K, static_cast<std::uint32_t>(::getpid()), 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
#ifdef SYS_prlimit64
    // C library stack discovery may query this process's limits. It cannot
    // change limits or inspect another process through this exception.
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_prlimit64, 0, 10);
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0]));
    jump(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[2]));
    jump(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[2]) + 4);
    jump(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
#endif
    // clone3 passes its flags in memory, which a filter cannot read: ENOSYS
    // makes the C library fall back to clone.
#ifdef SYS_clone3
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone3, 0, 1);
    stmt(BPF_RET | BPF_K, errno_result(ENOSYS));
#endif
    // Accept only the flags used for pthreads, with CLONE_THREAD mandatory.
    // Both supported architectures pass flags as the first argument.
    constexpr std::uint32_t thread_flags = CLONE_VM | CLONE_FS | CLONE_FILES | CLONE_SIGHAND |
        CLONE_THREAD | CLONE_SYSVSEM | CLONE_SETTLS | CLONE_PARENT_SETTID | CLONE_CHILD_CLEARTID | CLONE_CHILD_SETTID;
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone, 0, 9);
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0]) + 4);
    jump(BPF_JMP | BPF_JEQ | BPF_K, 0, 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0]));
    jump(BPF_JMP | BPF_JSET | BPF_K, CLONE_THREAD, 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    jump(BPF_JMP | BPF_JSET | BPF_K, ~thread_flags, 0, 1);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    return f;
}

}  // namespace

std::expected<void, std::string> sandbox_worker() {
    if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
        return std::unexpected(std::string("prctl(PR_SET_NO_NEW_PRIVS): ") + std::strerror(errno));
    }
    auto filter = build_filter();
    sock_fprog prog{static_cast<unsigned short>(filter.size()), filter.data()};
    if (::syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER, SECCOMP_FILTER_FLAG_TSYNC, &prog) != 0) {
        return std::unexpected(std::string("seccomp: ") + std::strerror(errno));
    }
    return {};
}

std::expected<void, std::string> check_sandbox() {
    if (int fd = ::open("/etc/hostname", O_RDONLY); fd >= 0 || errno != EPERM) {
        if (fd >= 0) ::close(fd);
        return std::unexpected(std::string("open is allowed"));
    }
    if (int s = ::socket(AF_INET, SOCK_STREAM, 0); s >= 0 || errno != EPERM) {
        if (s >= 0) ::close(s);
        return std::unexpected(std::string("socket is allowed"));
    }
    const pid_t pid = ::fork();
    if (pid == 0) ::_exit(0);
    if (pid > 0) {
        ::waitpid(pid, nullptr, 0);
        return std::unexpected(std::string("fork is allowed"));
    }
    char* argv[] = {const_cast<char*>("/bin/true"), nullptr};
    if (::execve("/bin/true", argv, nullptr) == 0 || errno != EPERM) {
        return std::unexpected(std::string("execve is allowed"));
    }
    // These were outside the old deny list. Neither a harmless unlisted
    // syscall nor a new asynchronous I/O interface bypasses the allow list.
    if (::syscall(SYS_getsid, 0) != -1 || errno != EPERM)
        return std::unexpected(std::string("an unlisted system call is allowed"));
#ifdef SYS_io_uring_setup
    if (::syscall(SYS_io_uring_setup, 1, nullptr) != -1 || errno != EPERM)
        return std::unexpected(std::string("io_uring_setup is allowed"));
#endif
    bool thread_ran = false;
    std::thread thread([&] { thread_ran = true; });
    thread.join();
    if (!thread_ran) return std::unexpected(std::string("worker threads are not allowed"));

    int pipe_fds[2];
    if (::pipe2(pipe_fds, O_CLOEXEC) != 0)
        return std::unexpected(std::string("anonymous pipes are not allowed"));
    const char sent = 'p';
    char received = 0;
    const bool pipe_ok = ::write(pipe_fds[1], &sent, 1) == 1 &&
        ::read(pipe_fds[0], &received, 1) == 1 && received == sent;
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
    if (!pipe_ok) return std::unexpected(std::string("anonymous pipe exchange failed"));

    return {};
}

}  // namespace paglets::runtime

#elif defined(__APPLE__)

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

// sandbox_init is deprecated as public API but remains the mechanism the
// system's own services and browsers use to apply an SBPL profile.
extern "C" int sandbox_init(const char* profile, std::uint64_t flags, char** errorbuf);
extern "C" void sandbox_free_error(char* errorbuf);

namespace paglets::runtime {

namespace {

// Everything the worker already holds keeps working (mapped memory, the
// connected socket pair); new processes, programs, network use and file
// contents are refused.
constexpr const char* profile = R"SB(
(version 1)
(allow default)
(deny process-fork)
(deny process-exec*)
(deny network*)
(deny file-write*)
(deny file-read-data)
)SB";

}  // namespace

std::expected<void, std::string> sandbox_worker() {
    char* error = nullptr;
    if (sandbox_init(profile, 0, &error) != 0) {
        std::string message = error != nullptr ? error : "unknown error";
        if (error != nullptr) sandbox_free_error(error);
        return std::unexpected("sandbox_init: " + message);
    }
    return {};
}

std::expected<void, std::string> check_sandbox() {
    if (int fd = ::open("/etc/hosts", O_RDONLY); fd >= 0) {
        ::close(fd);
        return std::unexpected(std::string("reading files is allowed"));
    }
    const char* temporary = std::getenv("TMPDIR");
    if (temporary == nullptr || *temporary == '\0')
        return std::unexpected(std::string("TMPDIR is required for the sandbox file-creation probe"));
    std::string path(temporary);
    if (path.back() != '/') path += '/';
    path += "paglets-sandbox-check-" + std::to_string(::getpid());
    if (int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600); fd >= 0) {
        ::close(fd);
        ::unlink(path.c_str());
        return std::unexpected(std::string("creating files is allowed"));
    }
    if (int s = ::socket(AF_INET, SOCK_STREAM, 0); s >= 0) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(9);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        const int rc = ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
        const int e = errno;
        ::close(s);
        // Refused by the sandbox, not by the (absent) peer.
        if (rc == 0 || (e != EPERM && e != EACCES)) {
            return std::unexpected("network connections are allowed (" + std::string(std::strerror(e)) + ")");
        }
    }
    const pid_t pid = ::fork();
    if (pid == 0) ::_exit(0);
    if (pid > 0) {
        ::waitpid(pid, nullptr, 0);
        return std::unexpected(std::string("fork is allowed"));
    }
    char* argv[] = {const_cast<char*>("/usr/bin/true"), nullptr};
    if (::execve("/usr/bin/true", argv, nullptr) == 0) {
        return std::unexpected(std::string("execve is allowed"));
    }
    return {};
}

}  // namespace paglets::runtime

#elif defined(_WIN32)

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include "windows_sandbox.hpp"

#include <vector>

namespace paglets::runtime {

namespace {

std::string last_error(const char* what) {
    return std::string(what) + " failed (error " + std::to_string(GetLastError()) + ")";
}

std::vector<std::uint8_t> token_information(HANDLE token, TOKEN_INFORMATION_CLASS what) {
    DWORD size = 0;
    GetTokenInformation(token, what, nullptr, 0, &size);
    std::vector<std::uint8_t> buffer(size);
    if (size == 0 || !GetTokenInformation(token, what, buffer.data(), size, &size)) buffer.clear();
    return buffer;
}

}  // namespace

// The host assigns the job before resuming the worker. Windows forbids
// nesting another job with UI restrictions, so verify and reuse those limits.
// The host also supplies a distinct LPAC identity without network capabilities.
std::expected<void, std::string> sandbox_worker() {
    HANDLE identity = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &identity)) return std::unexpected(last_error("OpenProcessToken"));
    auto verified = ipc::verify_windows_identity(identity);
    CloseHandle(identity);
    if (!verified) return verified;
    constexpr DWORD required_limits = JOB_OBJECT_LIMIT_ACTIVE_PROCESS | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
    constexpr DWORD required_ui = JOB_OBJECT_UILIMIT_DESKTOP | JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
        JOB_OBJECT_UILIMIT_EXITWINDOWS | JOB_OBJECT_UILIMIT_GLOBALATOMS | JOB_OBJECT_UILIMIT_HANDLES |
        JOB_OBJECT_UILIMIT_READCLIPBOARD | JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS | JOB_OBJECT_UILIMIT_WRITECLIPBOARD;
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
    JOBOBJECT_BASIC_UI_RESTRICTIONS ui{};
    const bool restricted =
        QueryInformationJobObject(nullptr, JobObjectExtendedLimitInformation, &limits, sizeof limits, nullptr) &&
        QueryInformationJobObject(nullptr, JobObjectBasicUIRestrictions, &ui, sizeof ui, nullptr) &&
        (limits.BasicLimitInformation.LimitFlags & required_limits) == required_limits &&
        limits.BasicLimitInformation.ActiveProcessLimit == 1 && (ui.UIRestrictionsClass & required_ui) == required_ui;
    if (!restricted) {
        HANDLE job = CreateJobObjectW(nullptr, nullptr);
        if (job == nullptr) return std::unexpected(last_error("CreateJobObject"));
        limits = {};
        limits.BasicLimitInformation.LimitFlags = required_limits;
        limits.BasicLimitInformation.ActiveProcessLimit = 1;
        ui.UIRestrictionsClass = required_ui;
        if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof limits) ||
            !SetInformationJobObject(job, JobObjectBasicUIRestrictions, &ui, sizeof ui) ||
            !AssignProcessToJobObject(job, GetCurrentProcess())) {
            const std::string error = last_error("job object");
            CloseHandle(job);
            return std::unexpected(error);
        }
        // The job stays with the process; its handle is intentionally kept open.
    }

    HANDLE token = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_ADJUST_DEFAULT | TOKEN_QUERY, &token)) {
        return std::unexpected(last_error("OpenProcessToken"));
    }
    auto privileges = token_information(token, TokenPrivileges);
    if (!privileges.empty()) {
        auto* p = reinterpret_cast<TOKEN_PRIVILEGES*>(privileges.data());
        for (DWORD i = 0; i < p->PrivilegeCount; ++i) p->Privileges[i].Attributes = SE_PRIVILEGE_REMOVED;
        if (!AdjustTokenPrivileges(token, FALSE, p, 0, nullptr, nullptr) || GetLastError() != ERROR_SUCCESS) {
            const std::string error = last_error("AdjustTokenPrivileges");
            CloseHandle(token);
            return std::unexpected(error);
        }
    }
    std::uint8_t sid[SECURITY_MAX_SID_SIZE];
    DWORD sid_size = sizeof sid;
    if (!CreateWellKnownSid(WinLowLabelSid, nullptr, sid, &sid_size)) {
        const std::string error = last_error("CreateWellKnownSid");
        CloseHandle(token);
        return std::unexpected(error);
    }
    TOKEN_MANDATORY_LABEL label{};
    label.Label.Attributes = SE_GROUP_INTEGRITY;
    label.Label.Sid = sid;
    if (!SetTokenInformation(token, TokenIntegrityLevel, &label, sizeof label + GetLengthSid(sid))) {
        const std::string error = last_error("SetTokenInformation(TokenIntegrityLevel)");
        CloseHandle(token);
        return std::unexpected(error);
    }
    CloseHandle(token);
    return {};
}

std::expected<void, std::string> check_windows_network() {
    WSADATA winsock{};
    if (WSAStartup(MAKEWORD(2, 2), &winsock) != 0)
        return std::unexpected(std::string("Winsock could not initialize for its denial probe"));
    struct CleanupWinsock { ~CleanupWinsock() { WSACleanup(); } } cleanup_winsock;
    for (int family : {AF_INET, AF_INET6}) for (bool loopback : {true, false}) {
        SOCKET socket = ::socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (socket == INVALID_SOCKET) {
            const int e = WSAGetLastError();
            if (e == WSAEACCES) continue;
            return std::unexpected(std::string("socket failed for a reason other than access denial: ") + std::to_string(e));
        }
        u_long nonblocking = 1;
        if (ioctlsocket(socket, FIONBIO, &nonblocking) != 0) { closesocket(socket); return std::unexpected(std::string("nonblocking socket probe failed")); }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(9);
        address.sin_addr.s_addr = htonl(loopback ? INADDR_LOOPBACK : 0xc0000201);  // 192.0.2.1 (TEST-NET)
        sockaddr_in6 address6{};
        address6.sin6_family = AF_INET6;
        address6.sin6_port = htons(9);
        if (loopback) address6.sin6_addr.s6_addr[15] = 1;
        else { address6.sin6_addr.s6_addr[0] = 0x20; address6.sin6_addr.s6_addr[1] = 1;
               address6.sin6_addr.s6_addr[2] = 0x0d; address6.sin6_addr.s6_addr[3] = 0xb8;
               address6.sin6_addr.s6_addr[15] = 1; }  // 2001:db8::1 (documentation)
        const auto rc = family == AF_INET
            ? connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof address)
            : connect(socket, reinterpret_cast<sockaddr*>(&address6), sizeof address6);
        const auto e = WSAGetLastError();
        closesocket(socket);
        if (rc == 0 || e != WSAEACCES) return std::unexpected(std::string("network connections are not denied"));
    }

    return {};
}

std::expected<void, std::string> check_sandbox() {
    HANDLE token = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        return std::unexpected(last_error("OpenProcessToken"));
    }
    auto identity = ipc::verify_windows_identity(token);
    const auto privileges = token_information(token, TokenPrivileges);
    const auto label = token_information(token, TokenIntegrityLevel);
    CloseHandle(token);
    if (!identity) return identity;
    if (!privileges.empty() && reinterpret_cast<const TOKEN_PRIVILEGES*>(privileges.data())->PrivilegeCount > 0) {
        return std::unexpected(std::string("the token still has privileges"));
    }
    if (label.empty()) return std::unexpected(std::string("no integrity level"));
    const auto* l = reinterpret_cast<const TOKEN_MANDATORY_LABEL*>(label.data());
    const DWORD rid = *GetSidSubAuthority(l->Label.Sid, *GetSidSubAuthorityCount(l->Label.Sid) - 1);
    if (rid > SECURITY_MANDATORY_LOW_RID) return std::unexpected(std::string("integrity level is above Low"));

    // Network denial was checked with the stronger launch token. Removing
    // SeChangeNotifyPrivilege can prevent Winsock from reopening its provider,
    // which is not a useful test of network capability enforcement.

    STARTUPINFOW si{};
    si.cb = sizeof si;
    PROCESS_INFORMATION pi{};
    std::wstring cmd = L"cmd.exe /c exit 0";
    if (CreateProcessW(nullptr, cmd.data(), nullptr, nullptr, FALSE, CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi)) {
        TerminateProcess(pi.hProcess, 0);
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);
        return std::unexpected(std::string("starting processes is allowed"));
    }
    return {};
}

}  // namespace paglets::runtime

#else

namespace paglets::runtime {

std::expected<void, std::string> sandbox_worker() {
    return std::unexpected(std::string("worker sandboxing is not available on this platform yet"));
}

std::expected<void, std::string> check_sandbox() {
    return std::unexpected(std::string("worker sandboxing is not available on this platform yet"));
}

}  // namespace paglets::runtime

#endif
