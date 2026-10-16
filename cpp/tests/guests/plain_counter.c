// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A plain C implementation of the paglet ABI, without the C++ guest SDK.
// It logs lifecycle events and counts deliveries in its memory image. It
// provides no request handlers: returning not_handled lets the host reject
// requests with unknown_message and retire their reply capabilities.

#include <stdint.h>
#include <stdlib.h>

#define EXPORT(name) __attribute__((export_name(#name)))
__attribute__((import_module("paglets"), import_name("log")))
void host_log(int32_t level, const void* text, uint32_t len);
#define LOG(text) host_log(1, text, sizeof(text) - 1)

static uint64_t deliveries;

EXPORT(paglets_abi_v1) void paglets_abi_v1(void) {}

EXPORT(paglets_alloc) void* paglets_alloc(uint32_t size) {
    return malloc(size ? size : 1);
}

EXPORT(paglets_free) void paglets_free(void* ptr) {
    free(ptr);
}

EXPORT(paglets_on_event) int32_t paglets_on_event(int32_t kind, const uint8_t* ptr, uint32_t len) {
    (void)ptr;
    (void)len;
    switch (kind) {
        case 1:  // created
            deliveries = 0;
            LOG("plain-c-created");
            break;
        case 2:  // activated: the restored image retains deliveries
            LOG("plain-c-activated");
            break;
        case 3:  // deactivating
            LOG("plain-c-deactivating");
            break;
    }
    return 0;  // handled
}

EXPORT(paglets_on_message) int32_t paglets_on_message(const uint8_t* ptr, uint32_t len) {
    (void)ptr;
    (void)len;
    ++deliveries;
    char text[64] = "plain-c-count: ";
    char digits[20];
    uint32_t count = 0;
    uint64_t value = deliveries;
    do {
        digits[count++] = (char)('0' + value % 10);
        value /= 10;
    } while (value);
    uint32_t end = sizeof("plain-c-count: ") - 1;
    while (count) text[end++] = digits[--count];
    host_log(1, text, end);
    return 1;  // not_handled; this observer has no application request API
}
