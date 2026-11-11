// ============================================================================
// peer_registry.hpp - Coordinator Dynamic Peer Registry (E2/E4)
//
// Thread-safe registry tracking dynamically registered peers across LAN,
// their live health progression (ONLINE -> SUSPECT -> OFFLINE), and separate
// workload metrics (available storage, active transfers, capacity).
// ============================================================================
#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "../../models/peer.hpp"
#include "../../third_party/mini_json.hpp"

class PeerRegistry {
public:
    PeerRegistry() = default;

    // Register or re-register a peer. Sets health to ONLINE and records initial workload.
    bool register_peer(const PeerInfo& info, std::string& message_out);

    // Record incoming heartbeat with latest workload metrics from peer.
    bool record_heartbeat(const std::string& peer_id, const PeerWorkload& workload);

    // Evaluate health progression based on timeout and threshold.
    // Returns list of state transition descriptions for logging.
    std::vector<std::string> evaluate_health(int timeout_ms = 1000, int failure_threshold = 2);

    // Lookups
    bool get_peer(const std::string& peer_id, PeerInfo& out) const;
    std::vector<PeerInfo> get_all_peers() const;
    std::vector<PeerInfo> get_online_peers() const;
    size_t size() const;

    // Formatting for CLI table
    std::string format_table() const;

    // JSON export
    json::Json to_json() const;

private:
    mutable std::mutex mutex_;
    std::map<std::string, PeerInfo> peers_;
};
