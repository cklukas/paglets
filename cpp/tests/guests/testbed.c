// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Test guest for engine behaviour: traps, endless loops, memory growth.

#include <stdint.h>

#define EXPORT(name) __attribute__((export_name(#name)))

static volatile uint64_t spin_counter;
static uint32_t calls;

EXPORT(add) int32_t add(int32_t a, int32_t b) {
    ++calls;
    return a + b;
}

EXPORT(calls) uint32_t get_calls(void) {
    return calls;
}

EXPORT(trap_now) void trap_now(void) {
    __builtin_trap();
}

EXPORT(spin) void spin(void) {
    for (;;) {
        ++spin_counter;
    }
}

// Returns the previous page count, or -1 when growth is refused.
EXPORT(grow) int32_t grow(int32_t pages) {
    return (int32_t)__builtin_wasm_memory_grow(0, (uint32_t)pages);
}

EXPORT(pages) int32_t pages(void) {
    return (int32_t)__builtin_wasm_memory_size(0);
}

static int32_t depth(int32_t n) {
    volatile char frame[256];
    frame[0] = (char)n;
    return n <= 0 ? frame[0] : depth(n - 1) + frame[0];
}

// Deep recursion to exhaust the Wasm stack.
EXPORT(recurse) int32_t recurse(int32_t n) {
    return depth(n);
}
