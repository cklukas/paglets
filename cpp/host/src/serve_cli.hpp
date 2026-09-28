// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-host serve: a host of a mesh on the network.

#pragma once

#include <filesystem>

namespace paglets::cli {

// argv[1] is "serve"; `self` is the running executable (to find
// paglets-worker next to it).
int serve_command(int argc, char** argv, const std::filesystem::path& self);

}  // namespace paglets::cli
