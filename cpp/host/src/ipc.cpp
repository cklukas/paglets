// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "ipc.hpp"

#ifndef _WIN32

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace paglets::ipc {

namespace {

#ifdef MSG_NOSIGNAL
constexpr int send_flags = MSG_NOSIGNAL;
#else
constexpr int send_flags = 0;  // macOS: SO_NOSIGPIPE is set on the socket
#endif

std::expected<void, std::string> write_all(int fd, const std::uint8_t* p, std::size_t n) {
    while (n > 0) {
        const ssize_t w = ::send(fd, p, n, send_flags);
        if (w < 0) {
            if (errno == EINTR) continue;
            return std::unexpected(std::string("ipc send: ") + std::strerror(errno));
        }
        p += w;
        n -= static_cast<std::size_t>(w);
    }
    return {};
}

std::expected<void, std::string> read_all(int fd, std::uint8_t* p, std::size_t n) {
    while (n > 0) {
        const ssize_t r = ::recv(fd, p, n, 0);
        if (r == 0) return std::unexpected(std::string("ipc: peer closed the connection"));
        if (r < 0) {
            if (errno == EINTR) continue;
            return std::unexpected(std::string("ipc receive: ") + std::strerror(errno));
        }
        p += r;
        n -= static_cast<std::size_t>(r);
    }
    return {};
}

}  // namespace

Channel& Channel::operator=(Channel&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

Channel::~Channel() {
    close();
}

void Channel::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Channel::closed_by_peer() const {
    if (fd_ < 0) return true;
    pollfd p{fd_, POLLIN, 0};
    const int n = ::poll(&p, 1, 0);
    return n > 0 && (p.revents & (POLLIN | POLLHUP | POLLERR)) != 0;
}

std::expected<void, std::string> Channel::send(std::span<const std::uint8_t> frame) {
    if (fd_ < 0) return std::unexpected(std::string("ipc: channel closed"));
    if (frame.size() > max_frame) return std::unexpected(std::string("ipc: frame too large"));
    const auto n = static_cast<std::uint32_t>(frame.size());
    const std::uint8_t header[4] = {static_cast<std::uint8_t>(n), static_cast<std::uint8_t>(n >> 8),
                                    static_cast<std::uint8_t>(n >> 16), static_cast<std::uint8_t>(n >> 24)};
    if (auto ok = write_all(fd_, header, sizeof header); !ok) return ok;
    return write_all(fd_, frame.data(), frame.size());
}

std::expected<std::vector<std::uint8_t>, std::string> Channel::receive() {
    if (fd_ < 0) return std::unexpected(std::string("ipc: channel closed"));
    std::uint8_t header[4];
    if (auto ok = read_all(fd_, header, sizeof header); !ok) return std::unexpected(ok.error());
    const std::uint32_t n = header[0] | (header[1] << 8) | (header[2] << 16) | (std::uint32_t{header[3]} << 24);
    if (n > max_frame) return std::unexpected(std::string("ipc: frame too large"));
    std::vector<std::uint8_t> frame(n);
    if (auto ok = read_all(fd_, frame.data(), n); !ok) return std::unexpected(ok.error());
    return frame;
}

std::expected<std::pair<int, int>, std::string> socket_pair() {
    int fds[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return std::unexpected(std::string("socketpair: ") + std::strerror(errno));
    }
    for (int fd : fds) {
        ::fcntl(fd, F_SETFD, FD_CLOEXEC);
#ifdef SO_NOSIGPIPE
        const int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
    }
    return std::pair{fds[0], fds[1]};
}

}  // namespace paglets::ipc

#else  // _WIN32

namespace paglets::ipc {

Channel& Channel::operator=(Channel&& other) noexcept {
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
}
Channel::~Channel() = default;
void Channel::close() {
    fd_ = -1;
}
bool Channel::closed_by_peer() const {
    return true;
}
std::expected<void, std::string> Channel::send(std::span<const std::uint8_t>) {
    return std::unexpected(std::string("worker processes are not supported on Windows yet"));
}
std::expected<std::vector<std::uint8_t>, std::string> Channel::receive() {
    return std::unexpected(std::string("worker processes are not supported on Windows yet"));
}
std::expected<std::pair<int, int>, std::string> socket_pair() {
    return std::unexpected(std::string("worker processes are not supported on Windows yet"));
}

}  // namespace paglets::ipc

#endif
