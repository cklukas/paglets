// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.
#pragma once

#ifdef _WIN32
#include <windows.h>
#include <expected>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace paglets::ipc {
struct WindowsSandbox {
    std::shared_ptr<void> files;
    std::filesystem::path executable;
    std::wstring profile;
    std::vector<unsigned char> sid;
    std::vector<unsigned char> registry;
    ~WindowsSandbox();
};
std::expected<std::shared_ptr<WindowsSandbox>, std::string>
prepare_windows_sandbox(const std::filesystem::path& executable, std::intptr_t job);
std::expected<void, std::string> verify_windows_identity(HANDLE token);
}  // namespace paglets::ipc
#endif
