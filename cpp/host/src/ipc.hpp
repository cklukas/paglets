// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Framed messages over a stream socket between the host control process and
// its worker processes: a 32-bit little-endian length followed by a
// MessagePack document. POSIX only; Windows runs paglets in-process for now.

#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string>
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
// answered by [result, document?].
inline constexpr std::int32_t import_marker = 100;

class Channel {
public:
    Channel() = default;
    explicit Channel(int fd) : fd_(fd) {}
    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;
    Channel(Channel&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    Channel& operator=(Channel&& other) noexcept;
    ~Channel();

    bool valid() const { return fd_ >= 0; }
    int fd() const { return fd_; }
    void close();

    // True if the peer has closed its end (or the channel failed). Only
    // meaningful while no answer is outstanding.
    bool closed_by_peer() const;

    std::expected<void, std::string> send(std::span<const std::uint8_t> frame);
    std::expected<std::vector<std::uint8_t>, std::string> receive();

private:
    int fd_ = -1;
};

// A connected pair of stream sockets (close-on-exec, no SIGPIPE).
std::expected<std::pair<int, int>, std::string> socket_pair();

}  // namespace paglets::ipc
