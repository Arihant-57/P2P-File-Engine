// ============================================================================
// coordinator_service.hpp - Coordinator TCP Listener & Health Orchestrator (E2/E4)
//
// Accepts dynamic peer registrations, processes periodic heartbeats,
// tracks peer workloads, evaluates state progressions (ONLINE -> SUSPECT -> OFFLINE),
// and serves cluster registry queries to CLI clients.
// ============================================================================
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "../catalog/catalog_manager.hpp"
#include "../communication/tcp_server.hpp"
#include "../registry/peer_registry.hpp"

class CoordinatorService {
public:
    CoordinatorService(std::string bind_address = "0.0.0.0",
                       uint16_t port = 5000,
                       int heartbeat_interval_ms = 500,
                       int heartbeat_timeout_ms = 1000,
                       int failure_threshold = 2);
    ~CoordinatorService();

    bool start();
    void stop();
    void run();

    PeerRegistry& registry() { return registry_; }
    const PeerRegistry& registry() const { return registry_; }

    CatalogManager& catalog() { return catalog_; }
    const CatalogManager& catalog() const { return catalog_; }

    uint16_t port() const { return port_; }
    std::string address() const { return bind_address_; }

    // Durability repair: orchestrates peer-to-peer shard reconstruction
    bool repair_file(const std::string& file_id, std::string& report_out);

private:
    std::string bind_address_;
    uint16_t port_;
    int heartbeat_interval_ms_;
    int heartbeat_timeout_ms_;
    int failure_threshold_;

    PeerRegistry registry_;
    CatalogManager catalog_{"metadata", "storage"};
    std::unique_ptr<TCPServer> server_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    std::thread monitor_thread_;

    bool handle_request(const json::Json& req,
                        const std::vector<uint8_t>& req_payload,
                        json::Json& resp,
                        std::vector<uint8_t>& resp_payload);

    void health_monitor_loop();
};
