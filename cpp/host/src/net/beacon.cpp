// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/net/beacon.hpp>

#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
constexpr socket_t no_socket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
constexpr socket_t no_socket = -1;
#endif

namespace paglets::net {

namespace {

constexpr std::size_t max_datagram = 1400;

void close_socket(socket_t s) {
#ifdef _WIN32
    closesocket(s);
#else
    ::close(s);
#endif
}

std::string last_error(std::string_view what) {
#ifdef _WIN32
    return std::string(what) + " (error " + std::to_string(WSAGetLastError()) + ")";
#else
    return std::string(what) + ": " + std::strerror(errno);
#endif
}

template <class T>
int set_option(socket_t s, int level, int name, const T& value) {
    return setsockopt(s, level, name, reinterpret_cast<const char*>(&value), static_cast<socklen_t>(sizeof value));
}

}  // namespace

struct Beacon::Impl {
    BeaconConfig config;
    Payload payload;
    Handler on_beacon;
    socket_t sock = no_socket;
    sockaddr_in group{};
    std::thread thread;
    std::mutex mu;
    std::condition_variable cv;
    bool stopping = false;
    bool wake = false;
    std::atomic<std::uint64_t> sent{0}, received{0};
#ifdef _WIN32
    bool wsa = false;
#endif

    void send_beacon() {
        const auto datagram = payload ? payload() : std::vector<std::uint8_t>{};
        if (datagram.empty() || datagram.size() > max_datagram) return;
        const auto n = sendto(sock, reinterpret_cast<const char*>(datagram.data()), static_cast<int>(datagram.size()),
                              0, reinterpret_cast<const sockaddr*>(&group), sizeof group);
        if (n >= 0 && static_cast<std::size_t>(n) == datagram.size()) ++sent;
    }

    void receive_pending() {
        while (true) {
#ifdef _WIN32
            WSAPOLLFD p{sock, POLLRDNORM, 0};
            if (WSAPoll(&p, 1, 0) <= 0) return;
#else
            pollfd p{sock, POLLIN, 0};
            if (poll(&p, 1, 0) <= 0) return;
#endif
            std::vector<std::uint8_t> buffer(max_datagram + 1);
            sockaddr_in from{};
            socklen_t len = sizeof from;
            const auto n = recvfrom(sock, reinterpret_cast<char*>(buffer.data()), static_cast<int>(buffer.size()), 0,
                                    reinterpret_cast<sockaddr*>(&from), &len);
            if (n <= 0 || static_cast<std::size_t>(n) > max_datagram) continue;
            buffer.resize(static_cast<std::size_t>(n));
            char ip[INET_ADDRSTRLEN] = {};
            inet_ntop(AF_INET, &from.sin_addr, ip, sizeof ip);
            ++received;
            if (on_beacon) on_beacon(std::move(buffer), ip);
        }
    }

    void run() {
        auto next_send = std::chrono::steady_clock::now();
        while (true) {
            {
                std::unique_lock lock(mu);
                if (stopping) return;
            }
            const auto now = std::chrono::steady_clock::now();
            bool send = false;
            {
                std::lock_guard lock(mu);
                send = wake || now >= next_send;
                wake = false;
            }
            if (send) {
                send_beacon();
                next_send = now + config.interval;
            }
            receive_pending();
            // Wait for datagrams (or the next send, or stop()).
#ifdef _WIN32
            WSAPOLLFD p{sock, POLLRDNORM, 0};
            (void)WSAPoll(&p, 1, 100);
#else
            pollfd p{sock, POLLIN, 0};
            (void)poll(&p, 1, 100);
#endif
        }
    }
};

Beacon::Beacon(BeaconConfig config, Payload payload, Handler on_beacon) : impl_(std::make_unique<Impl>()) {
    impl_->config = std::move(config);
    impl_->payload = std::move(payload);
    impl_->on_beacon = std::move(on_beacon);
}

Beacon::~Beacon() {
    stop();
}

std::expected<void, std::string> Beacon::start() {
    Impl& b = *impl_;
#ifdef _WIN32
    WSADATA data;
    if (WSAStartup(MAKEWORD(2, 2), &data) != 0) return std::unexpected(std::string("WSAStartup failed"));
    b.wsa = true;
#endif
    b.group.sin_family = AF_INET;
    b.group.sin_port = htons(static_cast<std::uint16_t>(b.config.port));
    if (inet_pton(AF_INET, b.config.group.c_str(), &b.group.sin_addr) != 1) {
        return std::unexpected("not an IPv4 multicast group: " + b.config.group);
    }
    b.sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (b.sock == no_socket) return std::unexpected(last_error("socket"));
    auto fail = [&](std::string_view what) -> std::expected<void, std::string> {
        auto why = last_error(what);
        close_socket(b.sock);
        b.sock = no_socket;
        return std::unexpected(why);
    };
    // Several hosts (or tests) on one machine share the port.
    const int one = 1;
    if (set_option(b.sock, SOL_SOCKET, SO_REUSEADDR, one) != 0) return fail("SO_REUSEADDR");
#ifdef SO_REUSEPORT
    (void)set_option(b.sock, SOL_SOCKET, SO_REUSEPORT, one);
#endif
    sockaddr_in any{};
    any.sin_family = AF_INET;
    any.sin_port = b.group.sin_port;
    any.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(b.sock, reinterpret_cast<const sockaddr*>(&any), sizeof any) != 0) return fail("bind");
    ip_mreq membership{};
    membership.imr_multiaddr = b.group.sin_addr;
    membership.imr_interface.s_addr = htonl(INADDR_ANY);
    if (set_option(b.sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, membership) != 0) return fail("joining the multicast group");
#ifdef _WIN32
    const DWORD ttl = static_cast<DWORD>(b.config.ttl);
    const DWORD loop = 1;
#else
    const unsigned char ttl = static_cast<unsigned char>(b.config.ttl);
    const unsigned char loop = 1;  // hosts on the same machine hear each other
#endif
    (void)set_option(b.sock, IPPROTO_IP, IP_MULTICAST_TTL, ttl);
    (void)set_option(b.sock, IPPROTO_IP, IP_MULTICAST_LOOP, loop);
    b.thread = std::thread([&b] { b.run(); });
    return {};
}

void Beacon::stop() {
    Impl& b = *impl_;
    {
        std::lock_guard lock(b.mu);
        b.stopping = true;
    }
    if (b.thread.joinable()) b.thread.join();
    if (b.sock != no_socket) {
        close_socket(b.sock);
        b.sock = no_socket;
    }
#ifdef _WIN32
    if (b.wsa) {
        WSACleanup();
        b.wsa = false;
    }
#endif
}

void Beacon::send_now() {
    std::lock_guard lock(impl_->mu);
    impl_->wake = true;
}

std::uint64_t Beacon::sent() const {
    return impl_->sent;
}

std::uint64_t Beacon::received() const {
    return impl_->received;
}

}  // namespace paglets::net
