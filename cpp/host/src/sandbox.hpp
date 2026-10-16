// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Operating-system sandbox of worker processes (plan, section 3.4). Workers
// only compute and talk to the host over already open sockets; the sandbox
// restricts access outside that computation even if a guest escapes WAMR.
//
// Linux: a seccomp filter (no_new_privs, allow list returning EPERM for other calls, process
// creation refused while threads are allowed). macOS: an SBPL profile
// (sandbox_init). Windows: job objects (no processes, no UI; the host's job
// ends workers with the host), a Low token with no privileges, and a unique
// LPAC identity. It has only registryRead for Winsock bootstrap, no network
// capabilities, and read/execute access to a protected runtime bundle. Windows
// still permits OS resources and the AppContainer's own private storage.

#pragma once

#include <expected>
#include <string>

namespace paglets::runtime {

// Applies the sandbox to the calling process; irreversible.
std::expected<void, std::string> sandbox_worker();

// Checks inside a sandboxed process that forbidden operations fail;
// returns a description of the first one that succeeded.
std::expected<void, std::string> check_sandbox();
#ifdef _WIN32
// Initializes the Winsock provider before token privileges are removed,
// then proves that the LPAC has no IP socket/connect authority.
std::expected<void, std::string> check_windows_network();
#endif

}  // namespace paglets::runtime
