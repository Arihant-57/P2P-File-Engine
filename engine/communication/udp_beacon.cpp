// ============================================================================
// udp_beacon.cpp - Decentralized UDP Peer Discovery Service (Task 1)
// ============================================================================
#include "udp_beacon.hpp"

#include <chrono>
#include <cstring>

#include "../../third_party/mini_json.hpp"

namespace {

socket_t create_udp_socket(uint16_t bind_port, bool allow_reuse, bool enable_broadcast) {
    socket_t s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == INVALID_SOCK) return INVALID_SOCK;

    if (allow_reuse) {
        int reuse = 1;
        ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&reuse), sizeof(reuse));
    }
    if (enable_broadcast) {
        int bcast = 1;
        ::setsockopt(s, SOL_SOCKET, SO_BROADCAST,
                     reinterpret_cast<const char*>(&bcast), sizeof(bcast));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(bind_port);

    if (::bind(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        sock_close(s);
        return INVALID_SOCK;
    }
    return s;
}

}  // namespace

UDPBeacon::UDPBeacon(std::string self_id,
                     std::string self_address,
                     uint16_t self_tcp_port,
                     uint16_t discovery_port,
                     int interval_ms,
                     WorkloadProvider workload_provider,
                     BeaconCallback on_beacon)
    : self_id_(std::move(self_id)),
      self_address_(std::move(self_address)),
      self_tcp_port_(self_tcp_port),
      discovery_port_(discovery_port == 0 ? 5005 : discovery_port),
      interval_ms_(interval_ms < 100 ? 100 : interval_ms),
      workload_provider_(std::move(workload_provider)),
      on_beacon_(std::move(on_beacon)) {}

UDPBeacon::~UDPBeacon() {
    stop();
}

void UDPBeacon::add_seed_endpoint(const std::string& address, uint16_t port) {
    if (address.empty() || port == 0) return;
    if ((address == self_address_ || address == "127.0.0.1") && port == self_tcp_port_) return;
    std::lock_guard<std::mutex> lock(seeds_mutex_);
    for (const auto& ep : seed_endpoints_) {
        if (ep.first == address && ep.second == port) return;
    }
    seed_endpoints_.emplace_back(address, port);
}

bool UDPBeacon::start() {
    if (running_.load()) return true;
    if (!sock_init()) return false;

    bcast_sock_ = create_udp_socket(discovery_port_, true, true);
    if (self_tcp_port_ != discovery_port_) {
        unicast_sock_ = create_udp_socket(self_tcp_port_, true, true);
    }

    if (bcast_sock_ == INVALID_SOCK && unicast_sock_ == INVALID_SOCK) {
        return false;
    }

    running_.store(true);
    recv_thread_ = std::thread(&UDPBeacon::receive_loop, this);
    bcast_thread_ = std::thread(&UDPBeacon::broadcast_loop, this);
    return true;
}

void UDPBeacon::stop() {
    if (!running_.exchange(false)) return;

    if (bcast_sock_ != INVALID_SOCK) {
        sock_close(bcast_sock_);
        bcast_sock_ = INVALID_SOCK;
    }
    if (unicast_sock_ != INVALID_SOCK) {
        sock_close(unicast_sock_);
        unicast_sock_ = INVALID_SOCK;
    }

    if (bcast_thread_.joinable()) bcast_thread_.join();
    if (recv_thread_.joinable()) recv_thread_.join();
}

std::string UDPBeacon::build_beacon_json() const {
    PeerWorkload w;
    if (workload_provider_) {
        w = workload_provider_();
    }
    json::Json j = json::Json::object();
    j["type"] = "P2P_BEACON";
    j["peer_id"] = self_id_;
    j["address"] = self_address_;
    j["port"] = static_cast<long long>(self_tcp_port_);
    j["discovery_port"] = static_cast<long long>(discovery_port_);
    j["available_storage_bytes"] = static_cast<long long>(w.available_storage_bytes);
    j["active_transfers"] = static_cast<long long>(w.active_transfers);
    j["transfer_capacity"] = static_cast<long long>(w.transfer_capacity);
    return j.dump();
}

void UDPBeacon::send_beacon_to(const std::string& target_ip,
                               uint16_t target_port,
                               const std::string& payload) {
    socket_t s = (unicast_sock_ != INVALID_SOCK) ? unicast_sock_ : bcast_sock_;
    if (s == INVALID_SOCK || payload.empty() || target_port == 0) return;

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(target_port);
    if (::inet_pton(AF_INET, target_ip.c_str(), &dest.sin_addr) != 1) return;

    ::sendto(s,
             payload.data(),
             static_cast<int>(payload.size()),
             0,
             reinterpret_cast<const sockaddr*>(&dest),
             sizeof(dest));
}

void UDPBeacon::broadcast_loop() {
    while (running_.load()) {
        const std::string payload = build_beacon_json();

        // 1. LAN broadcast (limited + directed /24 subnet broadcast) & loopback broadcast on shared discovery_port_
        send_beacon_to("255.255.255.255", discovery_port_, payload);
        if (!self_address_.empty() && self_address_ != "0.0.0.0" &&
            self_address_.rfind("127.", 0) != 0) {
            const size_t last_dot = self_address_.rfind('.');
            if (last_dot != std::string::npos) {
                const std::string subnet_bcast = self_address_.substr(0, last_dot + 1) + "255";
                send_beacon_to(subnet_bcast, discovery_port_, payload);
            }
        }
        send_beacon_to("127.255.255.255", discovery_port_, payload);
        send_beacon_to("127.0.0.1", discovery_port_, payload);

        // 2. Local host port neighborhood around discovery_port_ and self_tcp_port_
        //    Ensures instant multi-process discovery on Windows even when SO_REUSEADDR
        //    routes unicast loopback datagrams to a single socket.
        const uint16_t base_a = (discovery_port_ > 10) ? static_cast<uint16_t>(discovery_port_ - 10) : 1;
        for (uint16_t p = base_a; p <= discovery_port_ + 10; ++p) {
            if (p != self_tcp_port_ && p != discovery_port_) {
                send_beacon_to("127.0.0.1", p, payload);
            }
        }
        const uint16_t base_b = (self_tcp_port_ > 10) ? static_cast<uint16_t>(self_tcp_port_ - 10) : 1;
        for (uint16_t p = base_b; p <= self_tcp_port_ + 10; ++p) {
            if (p != self_tcp_port_ && p != discovery_port_) {
                send_beacon_to("127.0.0.1", p, payload);
            }
        }

        // 3. Configured seed endpoints (send to both the seed's TCP/unicast port and shared discovery_port_)
        std::vector<std::pair<std::string, uint16_t>> seeds;
        {
            std::lock_guard<std::mutex> lock(seeds_mutex_);
            seeds = seed_endpoints_;
        }
        for (const auto& ep : seeds) {
            send_beacon_to(ep.first, ep.second, payload);
            if (ep.second != discovery_port_) {
                send_beacon_to(ep.first, discovery_port_, payload);
            }
        }

        // Sleep in small slices for responsive shutdown
        int remaining = interval_ms_;
        while (remaining > 0 && running_.load()) {
            const int step = remaining > 100 ? 100 : remaining;
            std::this_thread::sleep_for(std::chrono::milliseconds(step));
            remaining -= step;
        }
    }
}

void UDPBeacon::receive_loop() {
    char buf[4096];
    while (running_.load()) {
        fd_set readfds;
        FD_ZERO(&readfds);
        socket_t max_fd = 0;

        if (bcast_sock_ != INVALID_SOCK) {
            FD_SET(bcast_sock_, &readfds);
            if (bcast_sock_ > max_fd) max_fd = bcast_sock_;
        }
        if (unicast_sock_ != INVALID_SOCK) {
            FD_SET(unicast_sock_, &readfds);
            if (unicast_sock_ > max_fd) max_fd = unicast_sock_;
        }
        if (max_fd == 0) break;

        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 150 * 1000;  // 150ms poll

        const int ready = ::select(static_cast<int>(max_fd + 1), &readfds, nullptr, nullptr, &tv);
        if (!running_.load()) break;
        if (ready <= 0) continue;

        auto drain_socket = [&](socket_t s) {
            if (s == INVALID_SOCK || !FD_ISSET(s, &readfds)) return;
            sockaddr_in from{};
            socklen_t from_len = sizeof(from);
            const int n = ::recvfrom(s, buf, sizeof(buf) - 1, 0,
                                     reinterpret_cast<sockaddr*>(&from), &from_len);
            if (n > 0) {
                buf[n] = '\0';
                char ip_str[INET_ADDRSTRLEN] = {0};
                ::inet_ntop(AF_INET, &from.sin_addr, ip_str, sizeof(ip_str));
                handle_datagram(buf, n, ip_str);
            }
        };

        drain_socket(bcast_sock_);
        drain_socket(unicast_sock_);
    }
}

void UDPBeacon::handle_datagram(const char* buf, int len, const std::string& sender_ip) {
    if (len <= 0) return;
    try {
        const std::string raw(buf, static_cast<size_t>(len));
        const json::Json j = json::Json::parse(raw);
        if (!j.is_object() || !j.contains("type")) return;
        if (j["type"].as_string() != "P2P_BEACON") return;

        const std::string remote_id = j["peer_id"].as_string();
        if (remote_id.empty() || remote_id == self_id_) return;  // Ignore self-beacons

        const uint16_t remote_port = static_cast<uint16_t>(j["port"].as_int());
        if (remote_port == 0) return;

        std::string remote_addr = j.contains("address") ? j["address"].as_string() : "";
        if (remote_addr.empty() || remote_addr == "0.0.0.0" ||
            (remote_addr == "127.0.0.1" && !sender_ip.empty() && sender_ip != "127.0.0.1")) {
            remote_addr = sender_ip.empty() ? "127.0.0.1" : sender_ip;
        }

        PeerInfo info;
        info.id = remote_id;
        info.address = remote_addr;
        info.port = remote_port;
        info.state = PeerHealth::ONLINE;
        if (j.contains("available_storage_bytes")) {
            info.workload.available_storage_bytes =
                static_cast<uint64_t>(j["available_storage_bytes"].as_int());
        }
        if (j.contains("active_transfers")) {
            info.workload.active_transfers =
                static_cast<uint32_t>(j["active_transfers"].as_int());
        }
        if (j.contains("transfer_capacity")) {
            info.workload.transfer_capacity =
                static_cast<uint32_t>(j["transfer_capacity"].as_int());
        }

        // Remember discovered peer as a direct unicast seed so subsequent beacons reach it directly
        add_seed_endpoint(info.address, info.port);

        if (on_beacon_) {
            on_beacon_(info, "UDP");
        }
    } catch (...) {
        // Ignore malformed UDP packets
    }
}
