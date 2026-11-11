// ============================================================================
// udp_beacon.hpp - Decentralized UDP Peer Discovery Service (Task 1)
//
// Broadcasts periodic UDP discovery beacons across the local network and
// listens for beacons from other peers without any central coordinator.
// Supports:
//   - LAN broadcast (255.255.255.255:<discovery_port>)
//   - Loopback broadcast & direct peer UDP port signaling
//   - Optional manual seed peer endpoints for restricted subnets
// ============================================================================
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../../models/peer.hpp"
#include "socket_compat.hpp"

class UDPBeacon {
public:
    using WorkloadProvider = std::function<PeerWorkload()>;
    using BeaconCallback = std::function<void(const PeerInfo& peer, const std::string& discovery_source)>;

    UDPBeacon(std::string self_id,
              std::string self_address,
              uint16_t self_tcp_port,
              uint16_t discovery_port,
              int interval_ms,
              WorkloadProvider workload_provider,
              BeaconCallback on_beacon);

    ~UDPBeacon();

    UDPBeacon(const UDPBeacon&) = delete;
    UDPBeacon& operator=(const UDPBeacon&) = delete;

    // Add a manual seed peer endpoint (IP + UDP/TCP port) as fallback discovery.
    void add_seed_endpoint(const std::string& address, uint16_t port);

    bool start();
    void stop();
    bool is_running() const { return running_.load(); }
    uint16_t discovery_port() const { return discovery_port_; }

private:
    void broadcast_loop();
    void receive_loop();
    void send_beacon_to(const std::string& target_ip, uint16_t target_port, const std::string& payload);
    void handle_datagram(const char* buf, int len, const std::string& sender_ip);
    std::string build_beacon_json() const;

    std::string self_id_;
    std::string self_address_;
    uint16_t self_tcp_port_;
    uint16_t discovery_port_;
    int interval_ms_;
    WorkloadProvider workload_provider_;
    BeaconCallback on_beacon_;

    socket_t bcast_sock_ = INVALID_SOCK;
    socket_t unicast_sock_ = INVALID_SOCK;

    mutable std::mutex seeds_mutex_;
    std::vector<std::pair<std::string, uint16_t>> seed_endpoints_;

    std::atomic<bool> running_{false};
    std::thread bcast_thread_;
    std::thread recv_thread_;
};
