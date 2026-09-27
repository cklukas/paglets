// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Operating-system sandbox of worker processes (plan, section 3.4). Workers
// only compute and talk to the host over already open sockets; the sandbox
// takes away everything else, so that a guest escaping WAMR still cannot
// open files, create sockets, run programs or start processes.
//
// Linux: a seccomp filter (no_new_privs, denylist returning EPERM, process
// creation refused while threads are allowed). Other platforms: not yet.

#pragma once

#include <expected>
#include <string>

namespace paglets::runtime {

// Applies the sandbox to the calling process; irreversible.
std::expected<void, std::string> sandbox_worker();

// Checks inside a sandboxed process that forbidden operations fail;
// returns a description of the first one that succeeded.
std::expected<void, std::string> check_sandbox();

}  // namespace paglets::runtime
