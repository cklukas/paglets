// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Test guest that imports functions outside the standard import set; the host
// must refuse to load it.

#include <stdint.h>

__attribute__((import_module("wasi_snapshot_preview1"), import_name("sock_accept"))) int32_t sock_accept(int32_t fd,
                                                                                                         int32_t flags,
                                                                                                         int32_t* out);
__attribute__((import_module("env"), import_name("system"))) int32_t host_system(const char* cmd);

__attribute__((export_name("run"))) int32_t run(void) {
    int32_t out = 0;
    return sock_accept(3, 0, &out) + host_system("true");
}
