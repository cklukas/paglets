// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The HTTP server's sockets and threads (WP34): POSIX and Winsock.

#include <paglets/web/http.hpp>

#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <shellapi.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace paglets::web {

namespace {

#ifdef _WIN32
using Socket = SOCKET;
constexpr Socket no_socket = INVALID_SOCKET;
void close_socket(Socket s) {
    ::closesocket(s);
}
int poll_fds(WSAPOLLFD* fds, unsigned long n, int timeout_ms) {
    return ::WSAPoll(fds, n, timeout_ms);
}
using PollFd = WSAPOLLFD;
struct Winsock {
    bool ok = false;
    Winsock() {
        WSADATA data;
        ok = ::WSAStartup(MAKEWORD(2, 2), &data) == 0;
    }
};
bool sockets_ready() {
    static Winsock w;
    return w.ok;
}
#else
using Socket = int;
constexpr Socket no_socket = -1;
void close_socket(Socket s) {
    ::close(s);
}
int poll_fds(pollfd* fds, nfds_t n, int timeout_ms) {
    return ::poll(fds, n, timeout_ms);
}
using PollFd = pollfd;
bool sockets_ready() {
    return true;
}
#endif

using Clock = std::chrono::steady_clock;

int remaining_ms(Clock::time_point deadline) {
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    return left <= 0 ? 0 : static_cast<int>(std::min<long long>(left, 250));
}

// Waits until `s` is readable (or writable); false at the deadline.
bool wait_socket(Socket s, bool write, Clock::time_point deadline) {
    while (Clock::now() < deadline) {
        PollFd p{};
        p.fd = s;
        p.events = write ? POLLOUT : POLLIN;
        const int r = poll_fds(&p, 1, remaining_ms(deadline));
        if (r > 0) return true;
        if (r < 0) {
#ifndef _WIN32
            if (errno == EINTR) continue;
#endif
            return false;
        }
    }
    return false;
}

bool send_all(Socket s, std::string_view data, Clock::time_point deadline) {
    while (!data.empty()) {
        if (!wait_socket(s, true, deadline)) return false;
#ifdef _WIN32
        const int n = ::send(s, data.data(), static_cast<int>(std::min<std::size_t>(data.size(), 1 << 20)), 0);
#elif defined(MSG_NOSIGNAL)
        const ssize_t n = ::send(s, data.data(), data.size(), MSG_NOSIGNAL);
#else
        const ssize_t n = ::send(s, data.data(), data.size(), 0);
#endif
        if (n <= 0) {
#ifndef _WIN32
            if (n < 0 && (errno == EINTR || errno == EAGAIN)) continue;
#endif
            return false;
        }
        data.remove_prefix(static_cast<std::size_t>(n));
    }
    return true;
}

// Reads into `buffer` until it holds `want` bytes in total (or the peer
// closed, or the deadline passed). For the head, `until_blank` stops at the
// empty line.
enum class ReadResult { done, closed, timeout, too_large };

ReadResult read_head(Socket s, std::string& buffer, std::size_t max, Clock::time_point deadline) {
    char chunk[4096];
    while (buffer.find("\r\n\r\n") == std::string::npos) {
        if (buffer.size() > max + 4) return ReadResult::too_large;
        if (!wait_socket(s, false, deadline)) return ReadResult::timeout;
#ifdef _WIN32
        const int n = ::recv(s, chunk, sizeof chunk, 0);
#else
        const ssize_t n = ::recv(s, chunk, sizeof chunk, 0);
#endif
        if (n == 0) return ReadResult::closed;
        if (n < 0) {
#ifndef _WIN32
            if (errno == EINTR || errno == EAGAIN) continue;
#endif
            return ReadResult::closed;
        }
        buffer.append(chunk, static_cast<std::size_t>(n));
    }
    return ReadResult::done;
}

ReadResult read_more(Socket s, std::string& buffer, std::size_t want, Clock::time_point deadline) {
    char chunk[16384];
    while (buffer.size() < want) {
        if (!wait_socket(s, false, deadline)) return ReadResult::timeout;
#ifdef _WIN32
        const int n = ::recv(s, chunk, static_cast<int>(std::min(sizeof chunk, want - buffer.size())), 0);
#else
        const ssize_t n = ::recv(s, chunk, std::min(sizeof chunk, want - buffer.size()), 0);
#endif
        if (n == 0) return ReadResult::closed;
        if (n < 0) {
#ifndef _WIN32
            if (errno == EINTR || errno == EAGAIN) continue;
#endif
            return ReadResult::closed;
        }
        buffer.append(chunk, static_cast<std::size_t>(n));
    }
    return ReadResult::done;
}

void set_nonblocking(Socket s) {
#ifdef _WIN32
    u_long on = 1;
    ::ioctlsocket(s, FIONBIO, &on);
#else
    const int flags = ::fcntl(s, F_GETFL, 0);
    ::fcntl(s, F_SETFL, flags | O_NONBLOCK);
    ::fcntl(s, F_SETFD, FD_CLOEXEC);
#ifdef SO_NOSIGPIPE
    int one = 1;
    ::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
#endif
}

std::string socket_error() {
#ifdef _WIN32
    return "socket error " + std::to_string(::WSAGetLastError());
#else
    return std::strerror(errno);
#endif
}

// Binds and listens on address:port; returns the socket and the bound port.
std::expected<std::pair<Socket, std::uint16_t>, std::string> listen_on(const std::string& address, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_NUMERICSERV;
    addrinfo* found = nullptr;
    const std::string service = std::to_string(port);
    if (int rc = ::getaddrinfo(address.c_str(), service.c_str(), &hints, &found); rc != 0 || found == nullptr) {
        return std::unexpected("cannot resolve " + address);
    }
    std::string why = "no address";
    for (addrinfo* a = found; a != nullptr; a = a->ai_next) {
        Socket s = ::socket(a->ai_family, a->ai_socktype, a->ai_protocol);
        if (s == no_socket) {
            why = socket_error();
            continue;
        }
        int one = 1;
#ifdef _WIN32
        ::setsockopt(s, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&one), sizeof one);
#else
        ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
#endif
        if (::bind(s, a->ai_addr, static_cast<int>(a->ai_addrlen)) != 0 || ::listen(s, 64) != 0) {
            why = socket_error();
            close_socket(s);
            continue;
        }
        sockaddr_storage bound{};
        socklen_t len = sizeof bound;
        std::uint16_t actual = port;
        if (::getsockname(s, reinterpret_cast<sockaddr*>(&bound), &len) == 0) {
            if (bound.ss_family == AF_INET) {
                actual = ntohs(reinterpret_cast<sockaddr_in*>(&bound)->sin_port);
            } else if (bound.ss_family == AF_INET6) {
                actual = ntohs(reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port);
            }
        }
        set_nonblocking(s);
        ::freeaddrinfo(found);
        return std::pair{s, actual};
    }
    ::freeaddrinfo(found);
    return std::unexpected(why);
}

}  // namespace

struct Server::Impl {
    ServerConfig config;
    Socket listener = no_socket;
    std::uint16_t port = 0;
    std::atomic<bool> stopping{false};

    std::mutex mu;
    std::condition_variable cv;
    std::deque<Socket> waiting;

    void handle(Socket s, const Handler& handler) {
        const auto deadline = Clock::now() + config.timeout;
        std::string buffer;
        auto refuse = [&](int status, std::string message) {
            Response r = Response::text(status, std::move(message) + "\n");
            (void)send_all(s, serialize(r, false), deadline);
        };
        switch (read_head(s, buffer, config.limits.max_head, deadline)) {
            case ReadResult::done: break;
            case ReadResult::too_large: refuse(431, "request header too large"); return;
            case ReadResult::timeout: refuse(408, "request timeout"); return;
            case ReadResult::closed: return;
        }
        const std::size_t end = buffer.find("\r\n\r\n");
        auto head = parse_head(std::string_view(buffer).substr(0, end), config.limits);
        if (!head) {
            refuse(head.error().status, head.error().message);
            return;
        }
        std::string body = buffer.substr(end + 4);
        if (body.size() > head->content_length) {
            refuse(400, "more data than Content-Length");
            return;
        }
        if (head->content_length > 0) {
            if (head->request.header("expect") == "100-continue") {
                (void)send_all(s, "HTTP/1.1 100 Continue\r\n\r\n", deadline);
            }
            if (read_more(s, body, head->content_length, deadline) != ReadResult::done) {
                refuse(408, "request body incomplete");
                return;
            }
        }
        head->request.body = std::move(body);
        Response response;
        try {
            response = handler(head->request);
        } catch (const std::exception& e) {
            response = Response::text(500, std::string("internal error: ") + e.what() + "\n");
        }
        (void)send_all(s, serialize(response, head->request.method == "HEAD"), deadline);
    }
};

Server::Server(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

Server::~Server() {
    stop();
    if (impl_->listener != no_socket) close_socket(impl_->listener);
}

std::expected<std::unique_ptr<Server>, std::string> Server::bind(ServerConfig config) {
    if (!sockets_ready()) return std::unexpected(std::string("sockets are not available"));
    if (config.ports.empty()) config.ports.push_back(0);
    std::string why;
    for (std::uint16_t port : config.ports) {
        auto bound = listen_on(config.address, port);
        if (!bound) {
            why = "port " + std::to_string(port) + ": " + bound.error();
            continue;
        }
        auto impl = std::make_unique<Impl>();
        impl->config = std::move(config);
        impl->listener = bound->first;
        impl->port = bound->second;
        return std::unique_ptr<Server>(new Server(std::move(impl)));
    }
    return std::unexpected("cannot listen on " + config.address + " (" + why + ")");
}

std::uint16_t Server::port() const {
    return impl_->port;
}

const std::string& Server::address() const {
    return impl_->config.address;
}

void Server::stop() {
    impl_->stopping.store(true);
    impl_->cv.notify_all();
}

void Server::serve(Handler handler) {
    Impl& im = *impl_;
    std::vector<std::thread> workers;
    for (std::size_t i = 0; i < std::max<std::size_t>(1, im.config.workers); ++i) {
        workers.emplace_back([&im, &handler] {
            while (true) {
                Socket s = no_socket;
                {
                    std::unique_lock lock(im.mu);
                    im.cv.wait(lock, [&] { return im.stopping.load() || !im.waiting.empty(); });
                    if (im.waiting.empty()) return;  // stopping
                    s = im.waiting.front();
                    im.waiting.pop_front();
                }
                im.handle(s, handler);
                close_socket(s);
            }
        });
    }
    while (!im.stopping.load()) {
        PollFd p{};
        p.fd = im.listener;
        p.events = POLLIN;
        if (poll_fds(&p, 1, 250) <= 0) continue;
        Socket s = ::accept(im.listener, nullptr, nullptr);
        if (s == no_socket) continue;
        set_nonblocking(s);
        std::unique_lock lock(im.mu);
        if (im.waiting.size() >= im.config.queue) {
            lock.unlock();
            close_socket(s);  // overloaded
            continue;
        }
        im.waiting.push_back(s);
        lock.unlock();
        im.cv.notify_one();
    }
    im.cv.notify_all();
    for (auto& t : workers) t.join();
    std::lock_guard lock(im.mu);
    for (Socket s : im.waiting) close_socket(s);
    im.waiting.clear();
}

// -- browser -------------------------------------------------------------------

bool open_browser(const std::string& url) {
#ifdef _WIN32
    const int size = ::MultiByteToWideChar(CP_UTF8, 0, url.c_str(), -1, nullptr, 0);
    std::wstring wide(static_cast<std::size_t>(size), L'\0');
    ::MultiByteToWideChar(CP_UTF8, 0, url.c_str(), -1, wide.data(), size);
    const auto result = reinterpret_cast<std::intptr_t>(
        ::ShellExecuteW(nullptr, L"open", wide.c_str(), nullptr, nullptr, SW_SHOWNORMAL));
    return result > 32;
#else
#ifdef __APPLE__
    const char* opener = "open";
#else
    const char* opener = "xdg-open";
#endif
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    // The opener's chatter does not belong in our terminal.
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0);
    char* argv[] = {const_cast<char*>(opener), const_cast<char*>(url.c_str()), nullptr};
    pid_t pid = 0;
    const int rc = ::posix_spawnp(&pid, opener, &actions, nullptr, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
    if (rc != 0) return false;
    // Reaped in the background: the opener ends quickly.
    std::thread([pid] {
        int status = 0;
        ::waitpid(pid, &status, 0);
    }).detach();
    return true;
#endif
}

}  // namespace paglets::web
