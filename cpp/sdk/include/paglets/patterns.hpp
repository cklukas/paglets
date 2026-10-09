// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The patterns library of the guest SDK (WP18, planning/cpp-patterns.md):
// services, tasks, typed operations, mesh fan-out, locate and pin,
// notifications, and single-file mobility. Modules that use it are built
// with paglets_add_module(... PATTERNS), which adds the service codecs the
// patterns need.

#pragma once

#include <paglets/patterns/fanout.hpp>
#include <paglets/patterns/files.hpp>
#include <paglets/patterns/locate.hpp>
#include <paglets/patterns/notify.hpp>
#include <paglets/patterns/operations.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/patterns/task.hpp>
