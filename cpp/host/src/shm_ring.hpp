// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Shared-memory transport between the host control process and a worker
// process: two single-producer single-consumer byte rings (one per
// direction) in a shared mapping, carrying the same length-prefixed frames
// as ipc::Channel. A waiting side spins briefly and then sleeps on a socket
// that carries one-byte wake-ups (and tells when the peer is gone), so a
// busy peer is reached without system calls.
//
// A channel is used by one thread on each side (both directions share the
// doorbell). Wake-up protocol, per waiting flag: the waiter sets the flag, re-checks
// its condition and then blocks on one byte; the peer, after changing the
// ring, takes the flag (1 -> 0) and only then sends one byte. A waiter that
// finds its condition true after setting the flag takes the flag back; if
// the peer took it first, the byte is on its way and is consumed. Every byte
// therefore belongs to exactly one wait.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <vector>

#include "ipc.hpp"

namespace paglets::ipc {

inline constexpr std::size_t default_ring_capacity = 1024 * 1024;

// A shared memory mapping backed by a file descriptor (POSIX) or a section
// handle (Windows), inheritable by a child process.
class SharedMemory {
public:
    SharedMemory() = default;
    SharedMemory(const SharedMemory&) = delete;
    SharedMemory& operator=(const SharedMemory&) = delete;
    SharedMemory(SharedMemory&& other) noexcept;
    SharedMemory& operator=(SharedMemory&& other) noexcept;
    ~SharedMemory();

    static std::expected<SharedMemory, std::string> create(std::size_t size);
    static std::expected<SharedMemory, std::string> attach(std::intptr_t handle, std::size_t size);

    std::uint8_t* data() const { return data_; }
    std::size_t size() const { return size_; }
    // File descriptor (POSIX) or HANDLE value (Windows) for the child.
    std::intptr_t handle() const { return handle_; }

private:
    void release();

    std::uint8_t* data_ = nullptr;
    std::size_t size_ = 0;
    std::intptr_t handle_ = -1;
};

struct RingControl {
    alignas(64) std::atomic<std::uint64_t> head{0};  // bytes written, by the producer
    alignas(64) std::atomic<std::uint64_t> tail{0};  // bytes read, by the consumer
    alignas(64) std::atomic<std::uint32_t> reader_waiting{0};
    std::atomic<std::uint32_t> writer_waiting{0};
};
static_assert(std::atomic<std::uint64_t>::is_always_lock_free, "rings need lock-free 64-bit atomics");

// Size of the mapping for two rings of `capacity` bytes (a power of two).
std::size_t ring_region_size(std::size_t capacity);

class RingChannel {
public:
    RingChannel() = default;
    // `host_side` selects which ring is outgoing; `doorbell` is this side of
    // a duplex stream for wake-ups (owned by the channel).
    RingChannel(SharedMemory memory, std::size_t capacity, Ends doorbell, bool host_side);
    RingChannel(const RingChannel&) = delete;
    RingChannel& operator=(const RingChannel&) = delete;
    RingChannel(RingChannel&& other) noexcept;
    RingChannel& operator=(RingChannel&& other) noexcept;
    ~RingChannel();

    bool valid() const { return doorbell_.read >= 0; }
    void close();

    // True if the peer is gone. Only meaningful while no answer is due.
    bool closed_by_peer() const;

    std::expected<void, std::string> send(std::span<const std::uint8_t> frame);
    std::expected<std::vector<std::uint8_t>, std::string> receive();

private:
    std::expected<void, std::string> write_bytes(const std::uint8_t* p, std::size_t n);
    std::expected<void, std::string> read_bytes(std::uint8_t* p, std::size_t n);
    template <class Condition>
    std::expected<void, std::string> wait(std::atomic<std::uint32_t>& flag, Condition&& ready);
    void wake(std::atomic<std::uint32_t>& flag);
    std::expected<void, std::string> consume_wake_up();

    SharedMemory memory_;
    std::size_t capacity_ = 0;
    RingControl* out_ = nullptr;
    std::uint8_t* out_data_ = nullptr;
    RingControl* in_ = nullptr;
    std::uint8_t* in_data_ = nullptr;
    Ends doorbell_;
};

}  // namespace paglets::ipc
