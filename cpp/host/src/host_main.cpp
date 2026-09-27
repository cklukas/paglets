// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-host: the paglets/cpp host binary. In milestone M0 it only reports
// its build configuration; the runtime is added from M1 on.

#include <paglets/wasm/engine.hpp>

#include <iostream>
#include <string_view>

namespace {

std::string_view compiler() {
#if defined(__clang__)
    return "clang " __clang_version__;
#elif defined(__GNUC__)
    return "gcc " __VERSION__;
#else
    return "unknown";
#endif
}

std::string_view platform() {
#if defined(__APPLE__)
    return "macos";
#elif defined(_WIN32)
    return "windows";
#elif defined(__linux__)
    return "linux";
#else
    return "unknown";
#endif
}

std::string_view architecture() {
#if defined(__aarch64__) || defined(_M_ARM64)
    return "arm64";
#elif defined(__x86_64__) || defined(_M_X64)
    return "x86-64";
#else
    return "unknown";
#endif
}

}  // namespace

int main(int argc, char** argv) {
    const std::string_view arg = argc > 1 ? argv[1] : "";
    if (arg == "--version") {
        std::cout << "paglets-host " << PAGLETS_VERSION << "\n";
        return 0;
    }
    if (!arg.empty() && arg != "--info") {
        std::cerr << "usage: paglets-host [--version | --info]\n";
        return 2;
    }
    paglets::wasm::ensure_runtime();
    std::cout << "paglets-host " << PAGLETS_VERSION << " (milestone M0)\n"
              << "  platform:   " << platform() << " " << architecture() << "\n"
              << "  compiler:   " << compiler() << "\n"
              << "  reflection: "
#if PAGLETS_HAVE_REFLECTION
              << "yes"
#else
              << "no"
#endif
              << "\n"
              << "  runtime:    " << PAGLETS_WAMR_TAG << " (" << PAGLETS_WAMR_MODE << ")\n"
              << "  paglet ABI: v0 (spike)\n";
    return 0;
}
