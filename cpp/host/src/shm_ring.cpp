// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "shm_ring.hpp"

#include "ipc.hpp"

#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <new>
#include <random>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace paglets::ipc {

namespace {

// How long a waiting side spins before it sleeps: long enough to cover the
// peer's reaction while both are busy, short against a sleeping peer.
constexpr auto spin_time = std::chrono::microseconds(30);

inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield" ::: "memory");
#endif
}

constexpr std::size_t control_size = (sizeof(RingControl) + 63) / 64 * 64;

}  // namespace

// ---------------------------------------------------------------------------
// SharedMemory

SharedMemory::SharedMemory(SharedMemory&& other) noexcept
    : data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      handle_(std::exchange(other.handle_, -1)) {}

SharedMemory& SharedMemory::operator=(SharedMemory&& other) noexcept {
    if (this != &other) {
        release();
        data_ = std::exchange(other.data_, nullptr);
        size_ = std::exchange(other.size_, 0);
        handle_ = std::exchange(other.handle_, -1);
    }
    return *this;
}

SharedMemory::~SharedMemory() {
    release();
}

void SharedMemory::release() {
#ifdef _WIN32
    if (data_ != nullptr) UnmapViewOfFile(data_);
#else
    if (data_ != nullptr) ::munmap(data_, size_);
#endif
    close_handle(handle_);
    data_ = nullptr;
    size_ = 0;
    handle_ = -1;
}

std::expected<SharedMemory, std::string> SharedMemory::create(std::size_t size) {
    SharedMemory m;
#ifdef _WIN32
    SECURITY_ATTRIBUTES sa{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};  // inheritable by the worker
    const auto size64 = static_cast<std::uint64_t>(size);
    HANDLE section = CreateFileMappingW(INVALID_HANDLE_VALUE, &sa, PAGE_READWRITE, static_cast<DWORD>(size64 >> 32),
                                        static_cast<DWORD>(size64 & 0xffffffffu), nullptr);
    if (section == nullptr) return std::unexpected("CreateFileMapping failed: " + std::to_string(GetLastError()));
    m.handle_ = reinterpret_cast<std::intptr_t>(section);
    void* view = MapViewOfFile(section, FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (view == nullptr) return std::unexpected("MapViewOfFile failed: " + std::to_string(GetLastError()));
    m.data_ = static_cast<std::uint8_t*>(view);
#else
#if defined(__linux__)
    int fd = ::memfd_create("paglets-ring", MFD_CLOEXEC);
#else
    // A uniquely named object, unlinked at once: only the descriptor remains.
    std::random_device rd;
    int fd = -1;
    for (int attempt = 0; attempt < 16 && fd < 0; ++attempt) {
        const std::string name = "/paglets-" + std::to_string(rd()) + "-" + std::to_string(rd());
        fd = ::shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd >= 0) ::shm_unlink(name.c_str());
    }
    if (fd >= 0) ::fcntl(fd, F_SETFD, FD_CLOEXEC);
#endif
    if (fd < 0) return std::unexpected(std::string("shared memory: ") + std::strerror(errno));
    m.handle_ = fd;
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        return std::unexpected(std::string("shared memory: ") + std::strerror(errno));
    }
    void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) return std::unexpected(std::string("shared memory: ") + std::strerror(errno));
    m.data_ = static_cast<std::uint8_t*>(p);
#endif
    m.size_ = size;
    return m;
}

std::expected<SharedMemory, std::string> SharedMemory::attach(std::intptr_t handle, std::size_t size) {
    SharedMemory m;
    m.handle_ = handle;
#ifdef _WIN32
    void* view = MapViewOfFile(reinterpret_cast<HANDLE>(handle), FILE_MAP_ALL_ACCESS, 0, 0, size);
    if (view == nullptr) return std::unexpected("MapViewOfFile failed: " + std::to_string(GetLastError()));
    m.data_ = static_cast<std::uint8_t*>(view);
#else
    void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, static_cast<int>(handle), 0);
    if (p == MAP_FAILED) return std::unexpected(std::string("shared memory: ") + std::strerror(errno));
    m.data_ = static_cast<std::uint8_t*>(p);
#endif
    m.size_ = size;
    return m;
}

std::size_t ring_region_size(std::size_t capacity) {
    return 2 * (control_size + capacity);
}

// ---------------------------------------------------------------------------
// RingChannel

RingChannel::RingChannel(SharedMemory memory, std::size_t capacity, Ends doorbell, bool host_side)
    : memory_(std::move(memory)), capacity_(capacity), doorbell_(doorbell) {
    std::uint8_t* base = memory_.data();
    auto* a = reinterpret_cast<RingControl*>(base);
    std::uint8_t* a_data = base + control_size;
    auto* b = reinterpret_cast<RingControl*>(base + control_size + capacity);
    std::uint8_t* b_data = base + 2 * control_size + capacity;
    if (host_side) {
        // The host creates the region and constructs the control blocks.
        a = new (a) RingControl();
        b = new (b) RingControl();
        out_ = a;
        out_data_ = a_data;
        in_ = b;
        in_data_ = b_data;
    } else {
        out_ = b;
        out_data_ = b_data;
        in_ = a;
        in_data_ = a_data;
    }
}

RingChannel::RingChannel(RingChannel&& other) noexcept
    : memory_(std::move(other.memory_)),
      capacity_(std::exchange(other.capacity_, 0)),
      out_(std::exchange(other.out_, nullptr)),
      out_data_(std::exchange(other.out_data_, nullptr)),
      in_(std::exchange(other.in_, nullptr)),
      in_data_(std::exchange(other.in_data_, nullptr)),
      doorbell_(std::exchange(other.doorbell_, Ends{})) {}

RingChannel& RingChannel::operator=(RingChannel&& other) noexcept {
    if (this != &other) {
        close();
        memory_ = std::move(other.memory_);
        capacity_ = std::exchange(other.capacity_, 0);
        out_ = std::exchange(other.out_, nullptr);
        out_data_ = std::exchange(other.out_data_, nullptr);
        in_ = std::exchange(other.in_, nullptr);
        in_data_ = std::exchange(other.in_data_, nullptr);
        doorbell_ = std::exchange(other.doorbell_, Ends{});
    }
    return *this;
}

RingChannel::~RingChannel() {
    close();
}

void RingChannel::close() {
    close_ends(doorbell_);
    memory_ = SharedMemory();
    out_ = in_ = nullptr;
    out_data_ = in_data_ = nullptr;
}

bool RingChannel::closed_by_peer() const {
    // Between calls the peer sends no wake-ups: a readable stream is closed.
    return readable_or_closed(doorbell_.read);
}

std::expected<void, std::string> RingChannel::consume_wake_up() {
    std::uint8_t byte = 0;
    if (!read_all(doorbell_.read, &byte, 1)) return std::unexpected(std::string("ipc: peer closed the connection"));
    return {};
}

// The ring positions and the waiting flags are accessed sequentially
// consistently (store own, then load the other's, on both sides): either the
// waiter sees the ring update, or the peer sees the waiting flag.
void RingChannel::wake(std::atomic<std::uint32_t>& flag) {
    if (flag.load() != 0 && flag.exchange(0) == 1) {
        const std::uint8_t byte = 1;
        (void)write_all(doorbell_.write, &byte, 1);  // a closed peer shows up at the next wait
    }
}

template <class Condition>
std::expected<void, std::string> RingChannel::wait(std::atomic<std::uint32_t>& flag, Condition&& ready) {
    const auto until = std::chrono::steady_clock::now() + spin_time;
    std::uint32_t spins = 0;
    while (!ready()) {
        if ((++spins & 63) == 0 && std::chrono::steady_clock::now() >= until) break;
        cpu_relax();
    }
    while (!ready()) {
        flag.store(1);
        if (ready()) {
            // Take the flag back; if the peer took it first, its byte is due.
            if (flag.exchange(0) == 0) return consume_wake_up();
            return {};
        }
        if (auto ok = consume_wake_up(); !ok) return ok;
    }
    return {};
}

std::expected<void, std::string> RingChannel::write_bytes(const std::uint8_t* p, std::size_t n) {
    while (n > 0) {
        const std::uint64_t head = out_->head.load(std::memory_order_relaxed);
        auto space = [&] { return capacity_ - (head - out_->tail.load()); };
        if (space() == 0) {
            if (auto ok = wait(out_->writer_waiting, [&] { return space() > 0; }); !ok) return ok;
        }
        const std::size_t chunk = std::min<std::size_t>(n, space());
        const std::size_t at = head & (capacity_ - 1);
        const std::size_t first = std::min(chunk, capacity_ - at);
        std::memcpy(out_data_ + at, p, first);
        std::memcpy(out_data_, p + first, chunk - first);
        out_->head.store(head + chunk);
        wake(out_->reader_waiting);
        p += chunk;
        n -= chunk;
    }
    return {};
}

std::expected<void, std::string> RingChannel::read_bytes(std::uint8_t* p, std::size_t n) {
    while (n > 0) {
        const std::uint64_t tail = in_->tail.load(std::memory_order_relaxed);
        auto available = [&] { return in_->head.load() - tail; };
        if (available() == 0) {
            if (auto ok = wait(in_->reader_waiting, [&] { return available() > 0; }); !ok) return ok;
        }
        const std::size_t chunk = std::min<std::size_t>(n, available());
        const std::size_t at = tail & (capacity_ - 1);
        const std::size_t first = std::min(chunk, capacity_ - at);
        std::memcpy(p, in_data_ + at, first);
        std::memcpy(p + first, in_data_, chunk - first);
        in_->tail.store(tail + chunk);
        wake(in_->writer_waiting);
        p += chunk;
        n -= chunk;
    }
    return {};
}

std::expected<void, std::string> RingChannel::send(std::span<const std::uint8_t> frame) {
    if (!valid()) return std::unexpected(std::string("ipc: channel closed"));
    if (frame.size() > 0xffffffffu) return std::unexpected(std::string("ipc: frame too large"));
    const auto n = static_cast<std::uint32_t>(frame.size());
    const std::array<std::uint8_t, 4> header{static_cast<std::uint8_t>(n), static_cast<std::uint8_t>(n >> 8),
                                             static_cast<std::uint8_t>(n >> 16), static_cast<std::uint8_t>(n >> 24)};
    if (auto ok = write_bytes(header.data(), header.size()); !ok) return ok;
    return write_bytes(frame.data(), frame.size());
}

std::expected<std::vector<std::uint8_t>, std::string> RingChannel::receive() {
    if (!valid()) return std::unexpected(std::string("ipc: channel closed"));
    std::array<std::uint8_t, 4> header{};
    if (auto ok = read_bytes(header.data(), header.size()); !ok) return std::unexpected(ok.error());
    const std::uint32_t n = header[0] | (header[1] << 8) | (header[2] << 16) | (std::uint32_t{header[3]} << 24);
    if (n > max_frame) return std::unexpected(std::string("ipc: frame too large"));
    std::vector<std::uint8_t> frame(n);
    if (auto ok = read_bytes(frame.data(), n); !ok) return std::unexpected(ok.error());
    return frame;
}

}  // namespace paglets::ipc
