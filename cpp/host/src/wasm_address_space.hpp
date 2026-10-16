// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#pragma once

#include <functional>
#include <iosfwd>
#include <string>

namespace paglets::wasm::detail {
// Zero on success (or on platforms not needing this diagnostic), otherwise
// errno. The Linux ARM64 probe reserves and immediately releases 1 TB of
// PROT_NONE address space, without touching or committing physical pages.
int probe_address_space();
void warn_address_space(bool hardware_checks, std::ostream& out,
                        const std::function<int()>& probe = probe_address_space);
// Cached before sandbox entry, for the worker startup acknowledgement.
const std::string& startup_address_space_warning();
}  // namespace paglets::wasm::detail
