// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Key and ledger commands of paglets-host: `keys`, `mesh` and `ledger`.

#pragma once

#include <string_view>

namespace paglets::cli {

bool is_mesh_command(std::string_view command);
// argv[1] is the command group (keys, mesh, ledger), argv[2] the action.
int mesh_command(int argc, char** argv);

}  // namespace paglets::cli
