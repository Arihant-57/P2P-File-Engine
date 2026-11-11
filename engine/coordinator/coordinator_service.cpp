// ============================================================================
// coordinator_service.cpp - Coordinator TCP Listener implementation
// ============================================================================
#include "coordinator_service.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include "../coding/adaptive_chunker.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"
#include "../distribution/dynamic_distributor.hpp"

CoordinatorService::CoordinatorService(std::string bind_address,
                                       uint16_t port,
                                       int heartbeat_interval_ms,
                                       int heartbeat_timeout_ms,
                                       int failure_threshold)
    : bind_address_(std::move(bind_address)),
      port_(port),
      heartbeat_interval_ms_(heartbeat_interval_ms),
      heartbeat_timeout_ms_(heartbeat_timeout_ms),
      failure_threshold_(failure_threshold) {}

CoordinatorService::~CoordinatorService() {
    stop();
}

bool CoordinatorService::start() {
    if (running_.load()) return true;

    TCPServer::Handler handler = [this](const json::Json& req,
                                        const std::vector<uint8_t>& req_payload,
                                        json::Json& resp,
                                        std::vector<uint8_t>& resp_payload) -> bool {
        return handle_request(req, req_payload, resp, resp_payload);
    };

    server_ = std::make_unique<TCPServer>(bind_address_, port_, handler);
    if (!server_->start()) {
        std::cerr << "[Coordinator] Failed to bind to " << bind_address_ << ":" << port_ << "\n";
        return false;
    }

    running_.store(true);
    server_thread_ = std::thread([this]() { server_->run(); });
    monitor_thread_ = std::thread(&CoordinatorService::health_monitor_loop, this);
    std::cout << "[Coordinator] Registration listener ACTIVE on " << bind_address_ << ":" << port_
              << " (heartbeat_interval=" << heartbeat_interval_ms_
              << "ms, timeout=" << heartbeat_timeout_ms_ << "ms)\n";
    return true;
}

void CoordinatorService::stop() {
    if (!running_.exchange(false)) return;

    if (server_) {
        server_->stop();
    }
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    if (monitor_thread_.joinable()) {
        monitor_thread_.join();
    }
    std::cout << "[Coordinator] Registration listener stopped\n";
}

void CoordinatorService::run() {
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

bool CoordinatorService::handle_request(const json::Json& req,
                                        const std::vector<uint8_t>& req_payload,
                                        json::Json& resp,
                                        std::vector<uint8_t>& resp_payload) {
    (void)resp_payload;
    const std::string type = req["type"].as_string();

    if (type == proto::PEER_REGISTER) {
        PeerInfo info;
        info.id = req["peer_id"].as_string();
        info.address = req["address"].as_string();
        info.port = static_cast<uint16_t>(req["port"].as_int());

        PeerWorkload wl;
        if (req.contains("available_storage_bytes"))
            wl.available_storage_bytes = static_cast<uint64_t>(req["available_storage_bytes"].as_int());
        if (req.contains("active_transfers"))
            wl.active_transfers = static_cast<uint32_t>(req["active_transfers"].as_int());
        if (req.contains("transfer_capacity"))
            wl.transfer_capacity = static_cast<uint32_t>(req["transfer_capacity"].as_int());
        info.workload = wl;

        std::string msg;
        registry_.register_peer(info, msg);
        std::cout << "[Coordinator] " << msg << std::endl;

        resp["type"] = proto::PEER_REGISTER_ACK;
        resp["status"] = "OK";
        resp["peer_id"] = info.id;
        resp["heartbeat_interval_ms"] = static_cast<long long>(heartbeat_interval_ms_);
        return true;
    }

    if (type == proto::PEER_HEARTBEAT) {
        const std::string peer_id = req["peer_id"].as_string();
        PeerWorkload wl;
        if (req.contains("available_storage_bytes"))
            wl.available_storage_bytes = static_cast<uint64_t>(req["available_storage_bytes"].as_int());
        if (req.contains("active_transfers"))
            wl.active_transfers = static_cast<uint32_t>(req["active_transfers"].as_int());
        if (req.contains("transfer_capacity"))
            wl.transfer_capacity = static_cast<uint32_t>(req["transfer_capacity"].as_int());

        bool ok = registry_.record_heartbeat(peer_id, wl);
        resp["type"] = proto::PEER_HEARTBEAT_ACK;
        resp["status"] = ok ? "OK" : "NOT_REGISTERED";
        resp["peer_id"] = peer_id;
        return true;
    }

    if (type == proto::QUERY_PEERS) {
        registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
        resp["type"] = proto::QUERY_PEERS_ACK;
        resp["peers"] = registry_.to_json();
        return true;
    }

    if (type == "UPLOAD_FILE") {
        const std::string filename = req.contains("filename") ? req["filename"].as_string() : "upload.bin";
        FileRecord rec;
        std::string err;
        if (!catalog_.stage_bytes(filename, req_payload, rec, err)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "failed to stage file centrally: " + err;
            return true;
        }
        resp["type"] = "UPLOAD_FILE_ACK";
        resp["status"] = "OK";
        resp["file_id"] = rec.file_id;
        resp["filename"] = rec.filename;
        resp["size"] = static_cast<long long>(rec.size);
        resp["sha256"] = rec.sha256;
        resp["status_str"] = file_status_str(rec.status);
        resp["central_path"] = rec.central_path;
        resp["upload_time"] = rec.upload_time;
        return true;
    }

    if (type == "DOWNLOAD_MANIFEST") {
        const std::string file_id = req["file_id"].as_string();
        std::lock_guard<std::mutex> lk(catalog_.get_file_mutex(file_id));

        FileRecord rec;
        if (!catalog_.get_file(file_id, rec)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "file not found: " + file_id;
            return true;
        }

        if (rec.status == FileDistributionStatus::STORED) {
            std::cout << "[Coordinator] Triggering first-demand RS(3,2) distribution for " << file_id << "...\n";
            AdaptiveChunker chunker;
            uint64_t chosen_shard_size = 0;
            const std::string staging_dir = "output/staging/" + file_id;
            rec.shards = chunker.encode_file(file_id, rec.central_path, staging_dir, chosen_shard_size);

            DynamicDistributor distributor(registry_);
            std::string dist_log;
            rec.status = distributor.distribute_file(file_id, rec.shards, staging_dir, dist_log);
            std::cout << dist_log;

            catalog_.update_shards_and_status(file_id, rec.status, rec.shards, chosen_shard_size);
            catalog_.get_file(file_id, rec);
        }

        resp["type"] = "DOWNLOAD_MANIFEST_ACK";
        resp["status"] = "OK";
        resp["manifest"] = rec.to_json();
        resp["peers"] = registry_.to_json();
        return true;
    }

    if (type == "CATALOG_LIST") {
        auto files = catalog_.list_files();
        json::Json arr = json::Json::array();
        for (const auto& f : files) arr.push_back(f.to_json());
        resp["type"] = "CATALOG_LIST_ACK";
        resp["files"] = arr;
        return true;
    }

    if (type == "CATALOG_SEARCH") {
        const std::string q = req.contains("query") ? req["query"].as_string() : "";
        auto files = catalog_.search_files(q);
        json::Json arr = json::Json::array();
        for (const auto& f : files) arr.push_back(f.to_json());
        resp["type"] = "CATALOG_SEARCH_ACK";
        resp["files"] = arr;
        return true;
    }

    if (type == "CATALOG_INFO") {
        const std::string file_id = req["file_id"].as_string();
        FileRecord rec;
        if (!catalog_.get_file(file_id, rec)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "file not found: " + file_id;
            return true;
        }
        resp["type"] = "CATALOG_INFO_ACK";
        resp["manifest"] = rec.to_json();
        return true;
    }

    if (type == "REPAIR_FILE") {
        const std::string file_id = req["file_id"].as_string();
        std::string report;
        bool ok = repair_file(file_id, report);
        resp["type"] = "REPAIR_FILE_ACK";
        resp["status"] = ok ? "OK" : "FAILED";
        resp["report"] = report;
        return true;
    }

    resp["type"] = proto::TRANSFER_ERROR;
    resp["reason"] = "unsupported coordinator request type: " + type;
    return true;
}

void CoordinatorService::health_monitor_loop() {
    const int step_ms = std::max(100, heartbeat_interval_ms_ / 2);
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        if (!running_.load()) break;

        auto transitions = registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
        for (const auto& msg : transitions) {
            std::cout << "[Coordinator] HEALTH CHANGE: " << msg << std::endl;
        }
    }
}

bool CoordinatorService::repair_file(const std::string& file_id, std::string& report_out) {
    std::lock_guard<std::mutex> lk(catalog_.get_file_mutex(file_id));
    FileRecord rec;
    if (!catalog_.get_file(file_id, rec)) {
        report_out = "file ID not found: " + file_id;
        return false;
    }

    registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
    auto online_peers = registry_.get_online_peers();
    std::map<std::string, PeerInfo> peer_map;
    for (const auto& p : registry_.get_all_peers()) peer_map[p.id] = p;

    // Group shards by stripe
    std::map<std::string, std::vector<size_t>> stripes;
    for (size_t i = 0; i < rec.shards.size(); ++i) {
        stripes[rec.shards[i].stripe_id].push_back(i);
    }

    std::ostringstream ss;
    bool any_repaired = false;
    bool all_shards_healthy = true;

    for (const auto& kv : stripes) {
        const auto& shard_indices = kv.second;

        std::vector<size_t> missing_indices;
        std::vector<size_t> surviving_indices;
        std::vector<std::string> used_peers;

        for (size_t idx : shard_indices) {
            const auto& sh = rec.shards[idx];
            bool is_online = false;
            if (!sh.peers.empty()) {
                const std::string& pid = sh.peers[0];
                used_peers.push_back(pid);
                auto it = peer_map.find(pid);
                if (it != peer_map.end() && it->second.state == PeerHealth::ONLINE) {
                    is_online = true;
                }
            }
            if (is_online) {
                surviving_indices.push_back(idx);
            } else {
                missing_indices.push_back(idx);
            }
        }

        if (missing_indices.empty()) continue;

        for (size_t m_idx : missing_indices) {
            ShardMeta& missing_shard = rec.shards[m_idx];

            if (surviving_indices.size() >= 2) {
                PeerInfo repl_peer;
                bool found_repl = false;
                for (const auto& candidate : online_peers) {
                    if (std::find(used_peers.begin(), used_peers.end(), candidate.id) == used_peers.end()) {
                        repl_peer = candidate;
                        found_repl = true;
                        break;
                    }
                }

                if (found_repl) {
                    json::Json req = json::Json::object();
                    req["type"] = "REPAIR_SHARD";
                    req["target_shard_id"] = missing_shard.chunk_id;
                    req["target_shard_size"] = static_cast<long long>(missing_shard.size);
                    req["expected_sha256"] = missing_shard.sha256;

                    json::Json sources = json::Json::array();
                    for (size_t s_idx : surviving_indices) {
                        const auto& surv_shard = rec.shards[s_idx];
                        const auto& p_info = peer_map[surv_shard.peers[0]];
                        json::Json src = json::Json::object();
                        src["peer_id"] = p_info.id;
                        src["address"] = p_info.address;
                        src["port"] = static_cast<long long>(p_info.port);
                        src["shard_id"] = surv_shard.chunk_id;
                        src["sha256"] = surv_shard.sha256;
                        sources.push_back(src);
                    }
                    req["sources"] = sources;

                    TCPClient client(5000, 5000);
                    if (client.connect_to(repl_peer.address, repl_peer.port)) {
                        json::Json resp;
                        std::vector<uint8_t> pl;
                        if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                            resp["type"].as_string() == "REPAIR_SHARD_ACK" &&
                            resp["status"].as_string() == "OK") {
                            missing_shard.peers = {repl_peer.id};
                            used_peers.push_back(repl_peer.id);
                            surviving_indices.push_back(m_idx);
                            any_repaired = true;
                            ss << "[Peer-to-Peer Repair] " << missing_shard.chunk_id
                               << " restored onto replacement Peer " << repl_peer.id
                               << " via direct RS(3,2) reconstruction\n";
                            continue;
                        }
                    }
                }
            }

            // Central fallback if surviving peer shards are insufficient
            if (std::filesystem::exists(rec.central_path)) {
                PeerInfo repl_peer;
                bool found = false;
                for (const auto& candidate : online_peers) {
                    if (std::find(used_peers.begin(), used_peers.end(), candidate.id) == used_peers.end()) {
                        repl_peer = candidate;
                        found = true;
                        break;
                    }
                }
                if (!found && !online_peers.empty()) {
                    repl_peer = online_peers[0];
                    found = true;
                }

                if (found) {
                    AdaptiveChunker chunker;
                    uint64_t dummy = 0;
                    const std::string staging_dir = "output/staging/" + file_id;
                    chunker.encode_file(file_id, rec.central_path, staging_dir, dummy);
                    const std::string sh_file = staging_dir + "/" + missing_shard.chunk_id + ".chunk";

                    std::ifstream in_sh(sh_file, std::ios::binary);
                    if (in_sh) {
                        in_sh.seekg(0, std::ios::end);
                        size_t fsz = static_cast<size_t>(in_sh.tellg());
                        in_sh.seekg(0, std::ios::beg);
                        std::vector<uint8_t> sh_bytes(fsz);
                        if (fsz > 0) in_sh.read(reinterpret_cast<char*>(sh_bytes.data()), fsz);
                        TCPClient client(3000, 3000);
                        if (client.connect_to(repl_peer.address, repl_peer.port)) {
                            json::Json store_req = json::Json::object();
                            store_req["type"] = proto::STORE_CHUNK;
                            store_req["chunk_id"] = missing_shard.chunk_id;
                            store_req["sha256"] = missing_shard.sha256;
                            json::Json store_resp;
                            std::vector<uint8_t> rpl;
                            if (client.request(store_req, sh_bytes, store_resp, rpl) == proto::RecvStatus::OK &&
                                store_resp["type"].as_string() == proto::STORE_ACK) {
                                missing_shard.peers = {repl_peer.id};
                                any_repaired = true;
                                ss << "[Central-Assisted Repair] " << missing_shard.chunk_id
                                   << " restored onto Peer " << repl_peer.id << "\n";
                                continue;
                            }
                        }
                    }
                }
            }

            all_shards_healthy = false;
            ss << "[Shortfall] Could not repair shard " << missing_shard.chunk_id << "\n";
        }
    }

    if (all_shards_healthy && !any_repaired) {
        report_out = "All shards are healthy and online across peers. File is fully protected.";
        return true;
    }

    FileDistributionStatus new_status = all_shards_healthy ? FileDistributionStatus::AVAILABLE
                                                           : FileDistributionStatus::DEGRADED;
    catalog_.update_shards_and_status(file_id, new_status, rec.shards, rec.shard_size);
    report_out = ss.str();
    return any_repaired;
}
