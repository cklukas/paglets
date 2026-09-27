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

int main(int argc, char** argv) {
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
