// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Multicast beacons on the local network (planning/cpp-mesh.md, section 3):
// every host sends a small datagram to a multicast group now and then and
// listens for the others'. What a beacon carries (the host's signed
// announcement) is the node's business; the beacon only moves bytes.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace paglets::net {

struct BeaconConfig {
    std::string group = "239.255.80.71";  // organization-local scope
    int port = 47471;
    std::chrono::milliseconds interval{5000};
    int ttl = 1;  // the local network only
};

class Beacon {
public:
    // `payload` is asked for before every send (empty: nothing is sent);
    // `on_beacon` receives datagrams from other hosts (and this one's own,
    // which the node ignores) with the sender's IP address.
    using Payload = std::function<std::vector<std::uint8_t>()>;
    using Handler = std::function<void(std::vector<std::uint8_t> datagram, const std::string& sender_ip)>;

    Beacon(BeaconConfig config, Payload payload, Handler on_beacon);
    ~Beacon();
    Beacon(const Beacon&) = delete;
    Beacon& operator=(const Beacon&) = delete;

    // Joins the group and starts sending and listening (a thread).
    std::expected<void, std::string> start();
    void stop();
    // Sends a beacon now (for example after the address changed).
    void send_now();

    std::uint64_t sent() const;
    std::uint64_t received() const;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace paglets::net
