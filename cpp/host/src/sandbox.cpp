// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "sandbox.hpp"

#include <cstdint>
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

namespace paglets::runtime {

namespace {

#if defined(__x86_64__)
constexpr std::uint32_t audit_arch = AUDIT_ARCH_X86_64;
#else
constexpr std::uint32_t audit_arch = AUDIT_ARCH_AARCH64;
#endif

// System calls a worker never needs once it runs.
constexpr long denied[] = {
    SYS_execve,
    SYS_execveat,
#ifdef SYS_fork
    SYS_fork,
#endif
#ifdef SYS_vfork
    SYS_vfork,
#endif
#ifdef SYS_open
    SYS_open,
#endif
#ifdef SYS_creat
    SYS_creat,
#endif
    SYS_openat,
#ifdef SYS_openat2
    SYS_openat2,
#endif
#ifdef SYS_open_by_handle_at
    SYS_open_by_handle_at,
#endif
#ifdef SYS_unlink
    SYS_unlink,
#endif
    SYS_unlinkat,
#ifdef SYS_rename
    SYS_rename,
#endif
    SYS_renameat,
#ifdef SYS_renameat2
    SYS_renameat2,
#endif
#ifdef SYS_mkdir
    SYS_mkdir,
#endif
    SYS_mkdirat,
#ifdef SYS_rmdir
    SYS_rmdir,
#endif
#ifdef SYS_link
    SYS_link,
#endif
    SYS_linkat,
#ifdef SYS_symlink
    SYS_symlink,
#endif
    SYS_symlinkat,
#ifdef SYS_chmod
    SYS_chmod,
#endif
    SYS_fchmodat,
#ifdef SYS_chown
    SYS_chown,
#endif
    SYS_fchownat,
    SYS_truncate,
    SYS_socket,
    SYS_connect,
    SYS_bind,
    SYS_listen,
    SYS_accept,
    SYS_accept4,
    SYS_ptrace,
    SYS_process_vm_readv,
    SYS_process_vm_writev,
    SYS_mount,
    SYS_umount2,
    SYS_pivot_root,
    SYS_chroot,
    SYS_setuid,
    SYS_setgid,
    SYS_setreuid,
    SYS_setregid,
    SYS_setresuid,
    SYS_setresgid,
    SYS_keyctl,
    SYS_add_key,
    SYS_request_key,
    SYS_bpf,
    SYS_perf_event_open,
    SYS_init_module,
    SYS_finit_module,
    SYS_delete_module,
    SYS_reboot,
    SYS_kexec_load,
    SYS_unshare,
    SYS_setns,
#ifdef SYS_userfaultfd
    SYS_userfaultfd,
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
    for (long nr : denied) {
        jump(BPF_JMP | BPF_JEQ | BPF_K, static_cast<std::uint32_t>(nr), 0, 1);
        stmt(BPF_RET | BPF_K, errno_result(EPERM));
    }
    // clone3 passes its flags in memory, which a filter cannot read: ENOSYS
    // makes the C library fall back to clone.
#ifdef SYS_clone3
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone3, 0, 1);
    stmt(BPF_RET | BPF_K, errno_result(ENOSYS));
#endif
    // clone is allowed for threads only (CLONE_THREAD in the flags, the
    // first argument on both architectures).
    jump(BPF_JMP | BPF_JEQ | BPF_K, SYS_clone, 0, 4);
    stmt(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0]));
    jump(BPF_JMP | BPF_JSET | BPF_K, CLONE_THREAD, 1, 0);
    stmt(BPF_RET | BPF_K, errno_result(EPERM));
    stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
    stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
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
    const std::string path = "/tmp/paglets-sandbox-check-" + std::to_string(::getpid());
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

#include <windows.h>

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

// The host starts workers in a job object that ends them with the host;
// here the worker adds its own job (no processes, no UI) and restricts its
// token: every privilege removed, integrity level Low (no writes to
// ordinary files, registry keys or processes of the user). Network access
// is not restricted on Windows yet (that needs an AppContainer).
std::expected<void, std::string> sandbox_worker() {
    HANDLE job = CreateJobObjectW(nullptr, nullptr);
    if (job == nullptr) return std::unexpected(last_error("CreateJobObject"));
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
    limits.BasicLimitInformation.LimitFlags =
        JOB_OBJECT_LIMIT_ACTIVE_PROCESS | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
    limits.BasicLimitInformation.ActiveProcessLimit = 1;
    JOBOBJECT_BASIC_UI_RESTRICTIONS ui{};
    ui.UIRestrictionsClass = JOB_OBJECT_UILIMIT_DESKTOP | JOB_OBJECT_UILIMIT_DISPLAYSETTINGS |
                             JOB_OBJECT_UILIMIT_EXITWINDOWS | JOB_OBJECT_UILIMIT_GLOBALATOMS |
                             JOB_OBJECT_UILIMIT_HANDLES | JOB_OBJECT_UILIMIT_READCLIPBOARD |
                             JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS | JOB_OBJECT_UILIMIT_WRITECLIPBOARD;
    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limits, sizeof limits) ||
        !SetInformationJobObject(job, JobObjectBasicUIRestrictions, &ui, sizeof ui) ||
        !AssignProcessToJobObject(job, GetCurrentProcess())) {
        const std::string error = last_error("job object");
        CloseHandle(job);
        return std::unexpected(error);
    }
    // The job stays with the process; its handle is intentionally kept open.

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

std::expected<void, std::string> check_sandbox() {
    HANDLE token = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        return std::unexpected(last_error("OpenProcessToken"));
    }
    const auto privileges = token_information(token, TokenPrivileges);
    const auto label = token_information(token, TokenIntegrityLevel);
    CloseHandle(token);
    if (!privileges.empty() && reinterpret_cast<const TOKEN_PRIVILEGES*>(privileges.data())->PrivilegeCount > 0) {
        return std::unexpected(std::string("the token still has privileges"));
    }
    if (label.empty()) return std::unexpected(std::string("no integrity level"));
    const auto* l = reinterpret_cast<const TOKEN_MANDATORY_LABEL*>(label.data());
    const DWORD rid = *GetSidSubAuthority(l->Label.Sid, *GetSidSubAuthorityCount(l->Label.Sid) - 1);
    if (rid > SECURITY_MANDATORY_LOW_RID) return std::unexpected(std::string("integrity level is above Low"));

    wchar_t temp[MAX_PATH + 1];
    const DWORD n = GetTempPathW(MAX_PATH, temp);
    if (n > 0) {
        const std::wstring path =
            std::wstring(temp, n) + L"paglets-sandbox-check-" + std::to_wstring(GetCurrentProcessId());
        HANDLE f = CreateFileW(path.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW, FILE_FLAG_DELETE_ON_CLOSE, nullptr);
        if (f != INVALID_HANDLE_VALUE) {
            CloseHandle(f);
            return std::unexpected(std::string("creating files in the user's temporary directory is allowed"));
        }
    }
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
