// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "sandbox.hpp"

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
