// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-worker: runs paglet instances for a paglets-host process. It is
// started by the host with two connected sockets (calls and terminations)
// and ends when the host closes them.
//
//   paglets-worker <main-fd> <control-fd>

#include "runtime_exec.hpp"

#include <cstdlib>
#include <iostream>

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "paglets-worker is started by paglets-host\n";
        return 2;
    }
    return paglets::runtime::run_worker(std::atoi(argv[1]), std::atoi(argv[2]));
}
