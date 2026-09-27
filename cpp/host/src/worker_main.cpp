// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-worker: runs paglet instances for a paglets-host process. It is
// started by the host with two connected sockets (calls and terminations)
// and ends when the host closes them. On Linux it runs in a seccomp sandbox.
//
//   paglets-worker <main-fd> <control-fd> [--no-sandbox]
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
    const bool sandbox = !(argc == 4 && std::strcmp(argv[3], "--no-sandbox") == 0);
    if (argc != 3 && !(argc == 4 && !sandbox)) {
        std::cerr << "paglets-worker is started by paglets-host\n";
        return 2;
    }
    return paglets::runtime::run_worker(std::atoi(argv[1]), std::atoi(argv[2]), sandbox);
}
