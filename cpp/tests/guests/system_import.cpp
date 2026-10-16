// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/paglet.hpp>

extern "C" __attribute__((import_module("paglets_sys"), import_name("call")))
std::int32_t unsupported_system_call();

// Keep the import in the linked module without ever trying to execute it.
extern "C" __attribute__((export_name("test_system_import")))
std::int32_t test_system_import() { return unsupported_system_call(); }

class SystemImport : public paglets::Paglet {};
PAGLETS_PAGLET(SystemImport)
