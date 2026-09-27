// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Operating-system plumbing between the host control process and its worker
// processes: byte streams over inherited handles (a socket pair on POSIX,
// two anonymous pipes on Windows), framed messages (a 32-bit little-endian
// length followed by a MessagePack document), and starting worker processes
// with exactly the handles they need. Calls travel through shared-memory
// rings (shm_ring.hpp); Channel carries frames over a stream (terminations).

#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace paglets::ipc {

inline constexpr std::size_t max_frame = 256u * 1024 * 1024;

// Operations of the control process (first array element of a request).
enum class Op : std::int32_t {
    load_module = 1,  // [op, hash, bytes]                        -> [0] | [1, error]
    create = 2,       // [op, paglet, hash, stack, pages, image?]  -> [0] | [1, error]
    call = 3,         // [op, paglet, export, [lead...], data]     -> imports..., [0, result] | [1, error]
    capture = 4,      // [op, paglet]                              -> [0, image] | [1, error]
    destroy = 5,      // [op, paglet]                              -> [0]
    terminate = 6,    // [op, paglet] on the control channel, no answer
};

// A frame from the worker during a call: [import_marker, name, a, b, data]
// answered by [result, document?]. Imports without a result (log) use
// notify_marker and get no answer.
inline constexpr std::int32_t import_marker = 100;
inline constexpr std::int32_t notify_marker = 101;

// One side of a duplex byte stream: the same socket on POSIX; the read end
// of one pipe and the write end of the other on Windows. Values are file
// descriptors or HANDLEs; -1 is none.
struct Ends {
    std::intptr_t read = -1;
    std::intptr_t write = -1;
};

// A duplex stream: the host keeps `host`, the worker inherits `child`.
struct DuplexPair {
    Ends host;
    Ends child;
};
std::expected<DuplexPair, std::string> duplex_pair();

void close_handle(std::intptr_t handle);
std::expected<std::intptr_t, std::string> duplicate_handle(std::intptr_t handle);
void close_ends(Ends& ends);
bool write_all(std::intptr_t handle, const std::uint8_t* p, std::size_t n);
bool read_all(std::intptr_t handle, std::uint8_t* p, std::size_t n);
// True if the peer closed its end, or the next read would not block.
bool readable_or_closed(std::intptr_t read_handle);

// Framed messages over a duplex stream; owns its ends.
class Channel {
public:
    Channel() = default;
    explicit Channel(Ends ends) : ends_(ends) {}
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;
    Channel(Channel&& other) noexcept : ends_(std::exchange(other.ends_, Ends{})) {}
    Channel& operator=(Channel&& other) noexcept;
    ~Channel();

    bool valid() const { return ends_.read >= 0; }
    void close();

    std::expected<void, std::string> send(std::span<const std::uint8_t> frame);
    std::expected<std::vector<std::uint8_t>, std::string> receive();

private:
    Ends ends_;
};

// A started worker process.
class Process {
public:
    Process() = default;
    Process(const Process&) = delete;
    Process& operator=(const Process&) = delete;
    Process(Process&& other) noexcept;
    Process& operator=(Process&& other) noexcept;
    ~Process();

    // Starts `executable`. `handles` are passed to the child, which sees
    // them under the values returned by child_values() (POSIX: descriptors
    // 3, 4, ... in order; Windows: unchanged, inherited through a handle
    // list), so arguments can name them. On Windows the process runs in
    // `job` (if given) from its first instruction.
    static std::expected<Process, std::string> spawn(const std::filesystem::path& executable,
                                                     const std::vector<std::string>& args,
                                                     const std::vector<std::intptr_t>& handles, std::intptr_t job = -1);
    static std::vector<std::intptr_t> child_values(const std::vector<std::intptr_t>& handles);

    bool running() const { return pid_ > 0; }
    int pid() const { return pid_; }
    // Non-blocking: true once the process has ended (it is reaped then).
    bool exited();
    // Waits up to `grace` for the process to end on its own, then kills it.
    void stop(std::chrono::milliseconds grace);

private:
    int pid_ = -1;
    std::intptr_t handle_ = -1;  // Windows process handle
};

// Windows: a job object for worker processes; they end with the host and
// cannot start processes. -1 on other platforms.
std::expected<std::intptr_t, std::string> create_worker_job();

}  // namespace paglets::ipc
