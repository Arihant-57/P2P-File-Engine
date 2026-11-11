// ============================================================================
// peer.cpp - Decentralized Peer Process (Task 1 & Task 2)
//
// Each peer is an autonomous OS process that acts as both TCP server and TCP
// client, maintains its own local SQLite database at:
//   storage/peer_<ID>/metadata.db
// discovers peers via UDP broadcast beacons + seed fallback, performs direct
// TCP peer handshakes, runs decentralized heartbeat monitoring (ONLINE ->
// SUSPECT -> OFFLINE), synchronizes distributed file manifests across the peer
// mesh (push + anti-entropy summary/request/response), and serves a searchable
// distributed file catalogue without any central coordinator.
//
// Usage:
//   peer <ID> <address> <port> [p2p|coordinator_address] [discovery_port|coordinator_port]
// ============================================================================
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/coding/reed_solomon.hpp"
#include "../engine/communication/http_server.hpp"
#include "../engine/communication/protocol.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/communication/tcp_server.hpp"
#include "../engine/communication/udp_beacon.hpp"
#include "../engine/db/database_manager.hpp"
#include "../engine/distribution/dynamic_distributor.hpp"
#include "../engine/process/process_manager.hpp"
#include "../engine/recovery/client_download.hpp"
#include "../engine/registry/peer_registry.hpp"
#include "../models/file_manifest.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

namespace {
static TCPServer* s_running_server = nullptr;
std::atomic<uint32_t> g_active_transfers{0};
std::atomic<uint64_t> g_max_storage_bytes{0};
const auto g_process_start_steady = std::chrono::steady_clock::now();

struct TransferGuard {
    std::atomic<uint32_t>& counter;
    explicit TransferGuard(std::atomic<uint32_t>& c) : counter(c) { ++counter; }
    ~TransferGuard() { --counter; }
};

struct TransferEntry {
    std::string transfer_id;
    std::string direction; // "UPLOAD" or "DOWNLOAD"
    std::string file_id;
    std::string filename;
    uint64_t total_bytes = 0;
    uint64_t bytes_transferred = 0;
    uint64_t speed_bps = 0;
    uint64_t elapsed_ms = 0;
    int32_t shards_count = 0;
    std::string peer_id;
    std::string status = "IN_PROGRESS"; // IN_PROGRESS, COMPLETED, FAILED
    std::string detail;
    int64_t timestamp_ms = 0;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["transfer_id"] = transfer_id;
        j["direction"] = direction;
        j["file_id"] = file_id;
        j["filename"] = filename;
        j["total_bytes"] = static_cast<long long>(total_bytes);
        j["bytes_transferred"] = static_cast<long long>(bytes_transferred);
        j["speed_bps"] = static_cast<long long>(speed_bps);
        j["elapsed_ms"] = static_cast<long long>(elapsed_ms);
        j["shards_count"] = static_cast<long long>(shards_count);
        j["peer_id"] = peer_id;
        j["status"] = status;
        j["detail"] = detail;
        j["timestamp_ms"] = static_cast<long long>(timestamp_ms);
        return j;
    }
};

struct PeerEventEntry {
    uint64_t event_id = 0;
    int64_t timestamp_ms = 0;
    std::string category;
    std::string severity;
    std::string message;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["event_id"] = static_cast<long long>(event_id);
        j["timestamp_ms"] = static_cast<long long>(timestamp_ms);
        j["category"] = category;
        j["severity"] = severity;
        j["message"] = message;
        return j;
    }
};

class PeerTelemetry {
public:
    std::string start_transfer(const std::string& direction,
                               const std::string& filename,
                               uint64_t total_bytes,
                               const std::string& peer_id,
                               const std::string& file_id = "") {
        std::lock_guard<std::mutex> lk(mu_);
        const uint64_t seq = ++transfer_seq_;
        TransferEntry e;
        e.transfer_id = (direction == "UPLOAD" ? "TX_UP_" : "TX_DL_") + std::to_string(seq);
        e.direction = direction;
        e.file_id = file_id;
        e.filename = filename;
        e.total_bytes = total_bytes;
        e.bytes_transferred = 0;
        e.peer_id = peer_id;
        e.status = "IN_PROGRESS";
        e.detail = "Transfer in progress";
        e.timestamp_ms = DatabaseManager::now_epoch_ms();
        transfers_.insert(transfers_.begin(), e);
        if (transfers_.size() > 100) transfers_.resize(100);
        return e.transfer_id;
    }

    void finish_transfer(const std::string& transfer_id,
                         bool success,
                         const std::string& file_id,
                         uint64_t bytes_transferred,
                         uint64_t elapsed_ms,
                         int32_t shards_count,
                         const std::string& detail) {
        std::lock_guard<std::mutex> lk(mu_);
        const uint64_t eff_ms = elapsed_ms > 0 ? elapsed_ms : 1;
        const uint64_t bps = (bytes_transferred * 1000ULL) / eff_ms;
        for (auto& t : transfers_) {
            if (t.transfer_id == transfer_id) {
                if (!file_id.empty()) t.file_id = file_id;
                t.bytes_transferred = bytes_transferred;
                if (t.total_bytes == 0) t.total_bytes = bytes_transferred;
                t.elapsed_ms = elapsed_ms;
                t.speed_bps = bps;
                t.shards_count = shards_count;
                t.status = success ? "COMPLETED" : "FAILED";
                t.detail = detail;
                t.timestamp_ms = DatabaseManager::now_epoch_ms();
                break;
            }
        }
    }

    void add_event(const std::string& category,
                   const std::string& severity,
                   const std::string& message) {
        std::lock_guard<std::mutex> lk(mu_);
        PeerEventEntry ev;
        ev.event_id = ++event_seq_;
        ev.timestamp_ms = DatabaseManager::now_epoch_ms();
        ev.category = category;
        ev.severity = severity;
        ev.message = message;
        events_.insert(events_.begin(), ev);
        if (events_.size() > 150) events_.resize(150);
    }

    json::Json get_transfers_json() const {
        std::lock_guard<std::mutex> lk(mu_);
        json::Json arr = json::Json::array();
        for (const auto& t : transfers_) arr.push_back(t.to_json());
        return arr;
    }

    json::Json get_transfer_json(const std::string& tid, bool& found_out) const {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto& t : transfers_) {
            if (t.transfer_id == tid) {
                found_out = true;
                return t.to_json();
            }
        }
        found_out = false;
        return json::Json::object();
    }

    json::Json get_events_json() const {
        std::lock_guard<std::mutex> lk(mu_);
        json::Json arr = json::Json::array();
        for (const auto& e : events_) arr.push_back(e.to_json());
        return arr;
    }

    void count_active(uint32_t& uploads_out, uint32_t& downloads_out) const {
        std::lock_guard<std::mutex> lk(mu_);
        uploads_out = 0;
        downloads_out = 0;
        for (const auto& t : transfers_) {
            if (t.status == "IN_PROGRESS") {
                if (t.direction == "UPLOAD") ++uploads_out;
                else ++downloads_out;
            }
        }
    }

private:
    mutable std::mutex mu_;
    uint64_t transfer_seq_ = 0;
    uint64_t event_seq_ = 0;
    std::vector<TransferEntry> transfers_;
    std::vector<PeerEventEntry> events_;
};

PeerTelemetry g_telemetry;

bool safe_chunk_id(const std::string& id) {
    if (id.empty() || id.size() > 128 || id.find("..") != std::string::npos) return false;
    for (unsigned char c : id) {
        if (c < 0x20 || c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' ||
            c == '"' || c == '<' || c == '>' || c == '|')
            return false;
    }
    return true;
}

bool safe_peer_id(const std::string& id) {
    if (id.empty() || id.size() > 64 || id.find("..") != std::string::npos) return false;
    for (unsigned char c : id) {
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.')) {
            return false;
        }
    }
    return true;
}

std::string safe_filename_only(const std::string& raw_name, const std::string& fallback = "file.bin") {
    std::string fname = fs::u8path(raw_name).filename().u8string();
    if (fname.empty() || fname == "." || fname == ".." || fname.find("..") != std::string::npos) {
        return fallback;
    }
    for (unsigned char c : fname) {
        if (c < 0x20 || c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' ||
            c == '"' || c == '<' || c == '>' || c == '|') {
            return fallback;
        }
    }
    return fname;
}

std::vector<uint8_t> read_binary(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return {};
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>());
}

std::string load_web_asset(const std::string& asset_name) {
    const std::string exe_dir = proc::self_exe_dir();
    const std::vector<std::string> candidates = {
        "web/" + asset_name,
        exe_dir + "/web/" + asset_name,
        exe_dir + "/../web/" + asset_name,
        exe_dir + "/../../web/" + asset_name
    };
    for (const auto& p : candidates) {
        std::error_code ec;
        if (fs::is_regular_file(fs::u8path(p), ec)) {
            std::ifstream in(fs::u8path(p), std::ios::binary);
            if (in) {
                std::ostringstream ss;
                ss << in.rdbuf();
                return ss.str();
            }
        }
    }
    return "";
}

struct LocalStorageStats {
    uint64_t used_bytes = 0;
    uint64_t available_bytes = 0;
    uint64_t max_storage_bytes = 0;
    uint64_t db_size_bytes = 0;
    uint32_t local_shards_count = 0;
    json::Json local_shards = json::Json::array();
};

uint64_t query_available_storage(const std::string& storage_dir) {
    std::error_code ec;
    auto space = fs::space(storage_dir, ec);
    uint64_t os_avail = ec ? 0 : space.available;
    const uint64_t quota = g_max_storage_bytes.load();
    if (quota > 0) {
        uint64_t used = 0;
        if (fs::exists(fs::u8path(storage_dir), ec)) {
            for (const auto& entry : fs::directory_iterator(fs::u8path(storage_dir), ec)) {
                if (entry.is_regular_file(ec)) {
                    used += static_cast<uint64_t>(entry.file_size(ec));
                }
            }
        }
        const uint64_t quota_rem = (quota > used) ? (quota - used) : 0;
        if (os_avail == 0 || quota_rem < os_avail) {
            return quota_rem;
        }
    }
    return os_avail;
}

LocalStorageStats compute_local_storage_stats(const std::string& storage_dir, const std::string& db_path) {
    LocalStorageStats st;
    std::error_code ec;
    if (fs::exists(fs::u8path(storage_dir), ec)) {
        for (const auto& entry : fs::directory_iterator(fs::u8path(storage_dir), ec)) {
            if (!entry.is_regular_file(ec)) continue;
            const std::string fn = entry.path().filename().u8string();
            if (fn.size() > 6 && fn.substr(fn.size() - 6) == ".chunk") {
                const uint64_t sz = static_cast<uint64_t>(entry.file_size(ec));
                st.used_bytes += sz;
                ++st.local_shards_count;
                json::Json sh = json::Json::object();
                sh["filename"] = fn;
                sh["shard_id"] = fn.substr(0, fn.size() - 6);
                sh["size_bytes"] = static_cast<long long>(sz);
                st.local_shards.push_back(sh);
            }
        }
    }
    if (fs::is_regular_file(fs::u8path(db_path), ec)) {
        st.db_size_bytes = static_cast<uint64_t>(fs::file_size(fs::u8path(db_path), ec));
    }
    st.available_bytes = query_available_storage(storage_dir);
    st.max_storage_bytes = g_max_storage_bytes.load();
    return st;
}

PeerWorkload current_workload(const std::string& storage_dir) {
    PeerWorkload w;
    w.available_storage_bytes = query_available_storage(storage_dir);
    w.active_transfers = g_active_transfers.load();
    w.transfer_capacity = 10;
    return w;
}

// Broadcast a FileManifest (and optional recovery_records) to all known online remote peers
void broadcast_manifest_to_peers(const std::string& self_id,
                                 const PeerRegistry& registry,
                                 const FileManifest& manifest,
                                 const std::vector<RecoveryRecordEntry>& recovery_records = {}) {
    const auto peers = registry.get_all_peers();
    json::Json push_req = json::Json::object();
    push_req["type"] = proto::P2P_METADATA_PUSH;
    push_req["origin_peer_id"] = self_id;
    push_req["propagate"] = false;
    push_req["manifest"] = manifest.to_json();
    if (!recovery_records.empty()) {
        json::Json rec_arr = json::Json::array();
        for (const auto& r : recovery_records) rec_arr.push_back(r.to_json());
        push_req["recovery_records"] = rec_arr;
    }

    for (const auto& p : peers) {
        if (p.id == self_id || p.state == PeerHealth::OFFLINE || p.port == 0) continue;
        TCPClient client(400, 400);
        if (client.connect_to(p.address, p.port)) {
            json::Json resp;
            std::vector<uint8_t> pl;
            client.request(push_req, {}, resp, pl);
        }
    }
}

// Reconcile local on-disk .chunk files when receiving a manifest update:
// if this peer's location_status for a shard was marked STALE or CORRUPTED (e.g.
// because this peer was OFFLINE while another peer rebuilt and replaced the shard),
// remove the stale/corrupted .chunk file from local disk so duplicate copies do not linger.
void reconcile_local_stale_shards(const std::string& self_id,
                                  const std::string& storage_dir,
                                  const FileManifest& manifest) {
    std::error_code ec;
    for (const auto& sh : manifest.shards) {
        for (const auto& loc : sh.locations) {
            if (loc.peer_id == self_id &&
                (loc.location_status == "STALE" || loc.location_status == "CORRUPTED")) {
                const std::string stale_path = storage_dir + "/" + sh.shard_id + ".chunk";
                if (fs::exists(fs::u8path(stale_path), ec)) {
                    fs::remove(fs::u8path(stale_path), ec);
                }
            }
        }
    }
}

// On-demand metadata pull from online remote peers when local catalogue is missing/stale
void pull_missing_metadata_from_peers(const std::string& self_id,
                                      const PeerRegistry& registry,
                                      DatabaseManager& db) {
    const auto peers = registry.get_all_peers();
    for (const auto& p : peers) {
        if (p.id == self_id || p.state != PeerHealth::ONLINE || p.port == 0) continue;
        const auto local_sum = db.get_metadata_summary();

        json::Json sum_req = json::Json::object();
        sum_req["type"] = proto::P2P_METADATA_SUMMARY;
        sum_req["peer_id"] = self_id;
        sum_req["summary"] = db.get_metadata_summary_json();

        json::Json sum_resp;
        std::vector<uint8_t> pl;
        TCPClient client(400, 400);
        if (!client.connect_to(p.address, p.port) ||
            client.request(sum_req, {}, sum_resp, pl) != proto::RecvStatus::OK ||
            sum_resp["type"].as_string() != proto::P2P_METADATA_SUMMARY_ACK ||
            !sum_resp.contains("summary") || !sum_resp["summary"].is_object()) {
            continue;
        }

        std::vector<std::string> to_req;
        for (const auto& kv : sum_resp["summary"].members()) {
            const int32_t rver = static_cast<int32_t>(kv.second.as_int());
            auto it = local_sum.find(kv.first);
            if (it == local_sum.end() || rver > it->second) {
                to_req.push_back(kv.first);
            }
        }
        if (to_req.empty()) continue;

        TCPClient mclient(600, 600);
        if (mclient.connect_to(p.address, p.port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_METADATA_REQUEST;
            req["peer_id"] = self_id;
            json::Json arr = json::Json::array();
            for (const auto& fid : to_req) arr.push_back(fid);
            req["file_ids"] = arr;

            json::Json resp;
            std::vector<uint8_t> rpl;
            if (mclient.request(req, {}, resp, rpl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_METADATA_RESPONSE &&
                resp.contains("manifests") && resp["manifests"].is_array()) {
                for (const auto& m_json : resp["manifests"].items()) {
                    FileManifest m = FileManifest::from_json(m_json);
                    if (!m.file_id.empty()) {
                        db.upsert_file_manifest(m, p.id);
                    }
                }
                if (resp.contains("recovery_records") && resp["recovery_records"].is_array()) {
                    for (const auto& r_json : resp["recovery_records"].items()) {
                        RecoveryRecordEntry r = RecoveryRecordEntry::from_json(r_json);
                        if (!r.recovery_id.empty()) db.upsert_recovery_record(r);
                    }
                }
            }
        }
    }
}

// ============================================================================
// DecentralizedPeerManager - Autonomous P2P Handshake, Heartbeat & Metadata Sync
// ============================================================================
class DecentralizedPeerManager {
public:
    DecentralizedPeerManager(std::string self_id,
                             std::string self_address,
                             uint16_t self_port,
                             uint16_t discovery_port,
                             std::string storage_dir,
                             PeerRegistry& registry,
                             DatabaseManager& db,
                             int hb_interval_ms = 250,
                             int hb_timeout_ms = 600,
                             int failure_threshold = 2)
        : self_id_(std::move(self_id)),
          self_address_(std::move(self_address)),
          self_port_(self_port),
          discovery_port_(discovery_port),
          storage_dir_(std::move(storage_dir)),
          registry_(registry),
          db_(db),
          hb_interval_ms_(hb_interval_ms),
          hb_timeout_ms_(hb_timeout_ms),
          failure_threshold_(failure_threshold),
          beacon_(
              self_id_,
              self_address_,
              self_port_,
              discovery_port_,
              300,
              [this]() { return current_workload(storage_dir_); },
              [this](const PeerInfo& discovered, const std::string& source) {
                  on_peer_discovered(discovered, source);
              }) {}

    ~DecentralizedPeerManager() { stop(); }

    void add_seed_peer(const std::string& address, uint16_t port) {
        beacon_.add_seed_endpoint(address, port);
    }

    void start() {
        if (running_.load()) return;

        // Register self in local registry & SQLite
        update_self_record();

        // Load previously persisted peers from SQLite so restarted peers
        // already know the cluster topology and immediately probe & sync with them.
        const auto persisted = db_.get_all_peers(false);
        for (const auto& p : persisted) {
            if (p.id.empty() || p.id == self_id_) continue;
            beacon_.add_seed_endpoint(p.address, p.port);
            std::string msg;
            registry_.register_peer(p, msg);
            if (p.state == PeerHealth::OFFLINE) {
                db_.update_peer_health(p.id, PeerHealth::OFFLINE);
            }
        }

        running_.store(true);
        beacon_.start();
        monitor_thread_ = std::thread(&DecentralizedPeerManager::run_loop, this);
    }

    void stop() {
        if (!running_.exchange(false)) return;
        beacon_.stop();
        if (monitor_thread_.joinable()) monitor_thread_.join();
    }

    // Perform direct outbound TCP handshake + metadata synchronization with a peer
    bool perform_tcp_handshake(const PeerInfo& target, const std::string& discovery_source) {
        if (target.id == self_id_ || target.port == 0) return false;

        TCPClient client(500, 500);
        if (!client.connect_to(target.address, target.port)) {
            return false;
        }

        const PeerWorkload w = current_workload(storage_dir_);
        json::Json req = json::Json::object();
        req["type"] = proto::P2P_HANDSHAKE;
        req["peer_id"] = self_id_;
        req["address"] = self_address_;
        req["port"] = static_cast<long long>(self_port_);
        req["available_storage_bytes"] = static_cast<long long>(w.available_storage_bytes);
        req["active_transfers"] = static_cast<long long>(w.active_transfers);
        req["transfer_capacity"] = static_cast<long long>(w.transfer_capacity);

        json::Json resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK) {
            return false;
        }
        if (resp["type"].as_string() != proto::P2P_HANDSHAKE_ACK) {
            return false;
        }

        PeerInfo verified = target;
        if (resp.contains("peer_id") && !resp["peer_id"].as_string().empty()) {
            verified.id = resp["peer_id"].as_string();
        }
        if (resp.contains("address") && !resp["address"].as_string().empty()) {
            verified.address = resp["address"].as_string();
        }
        if (resp.contains("port") && resp["port"].as_int() > 0) {
            verified.port = static_cast<uint16_t>(resp["port"].as_int());
        }
        if (resp.contains("available_storage_bytes")) {
            verified.workload.available_storage_bytes =
                static_cast<uint64_t>(resp["available_storage_bytes"].as_int());
        }
        if (resp.contains("active_transfers")) {
            verified.workload.active_transfers =
                static_cast<uint32_t>(resp["active_transfers"].as_int());
        }
        if (resp.contains("transfer_capacity")) {
            verified.workload.transfer_capacity =
                static_cast<uint32_t>(resp["transfer_capacity"].as_int());
        }
        verified.state = PeerHealth::ONLINE;

        std::string msg;
        registry_.register_peer(verified, msg);
        db_.upsert_peer(verified, DatabaseManager::now_epoch_ms(), discovery_source, false);
        beacon_.add_seed_endpoint(verified.address, verified.port);

        // Process gossiped peer list returned in P2P_HANDSHAKE_ACK
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                PeerInfo gp = PeerInfo::from_json(item);
                if (gp.id.empty() || gp.id == self_id_ || gp.port == 0) continue;
                beacon_.add_seed_endpoint(gp.address, gp.port);
                PeerInfo existing;
                if (!registry_.get_peer(gp.id, existing)) {
                    std::string gmsg;
                    registry_.register_peer(gp, gmsg);
                    db_.upsert_peer(gp, DatabaseManager::now_epoch_ms(), "TCP_GOSSIP", false);
                }
            }
        }

        // Immediately synchronize metadata summaries after handshake
        sync_metadata_with_peer(verified);
        return true;
    }

    // Exchange metadata summaries with a remote peer, pull missing/newer records,
    // and push local records that the remote peer is missing or has at an older version.
    void sync_metadata_with_peer(const PeerInfo& remote) {
        if (!running_.load() || remote.id == self_id_ || remote.port == 0) return;

        const auto local_summary = db_.get_metadata_summary();

        json::Json sum_req = json::Json::object();
        sum_req["type"] = proto::P2P_METADATA_SUMMARY;
        sum_req["peer_id"] = self_id_;
        sum_req["summary"] = db_.get_metadata_summary_json();

        json::Json sum_resp;
        std::vector<uint8_t> pl;
        {
            TCPClient client(400, 400);
            if (!client.connect_to(remote.address, remote.port)) return;
            if (client.request(sum_req, {}, sum_resp, pl) != proto::RecvStatus::OK) return;
            if (sum_resp["type"].as_string() != proto::P2P_METADATA_SUMMARY_ACK) return;
        }

        std::map<std::string, int32_t> remote_summary;
        if (sum_resp.contains("summary") && sum_resp["summary"].is_object()) {
            for (const auto& kv : sum_resp["summary"].members()) {
                remote_summary[kv.first] = static_cast<int32_t>(kv.second.as_int());
            }
        }

        // 1. Identify manifests that remote has which we are missing or have older version of
        std::vector<std::string> to_request;
        for (const auto& kv : remote_summary) {
            auto it = local_summary.find(kv.first);
            if (it == local_summary.end() || kv.second > it->second) {
                to_request.push_back(kv.first);
            }
        }

        if (!to_request.empty()) {
            TCPClient client(600, 600);
            if (client.connect_to(remote.address, remote.port)) {
                json::Json req = json::Json::object();
                req["type"] = proto::P2P_METADATA_REQUEST;
                req["peer_id"] = self_id_;
                json::Json ids_arr = json::Json::array();
                for (const auto& fid : to_request) ids_arr.push_back(fid);
                req["file_ids"] = ids_arr;

                json::Json resp;
                std::vector<uint8_t> rpl;
                if (client.request(req, {}, resp, rpl) == proto::RecvStatus::OK &&
                    resp["type"].as_string() == proto::P2P_METADATA_RESPONSE &&
                    resp.contains("manifests") && resp["manifests"].is_array()) {
                    for (const auto& m_json : resp["manifests"].items()) {
                        FileManifest m = FileManifest::from_json(m_json);
                        if (!m.file_id.empty()) {
                            db_.upsert_file_manifest(m, remote.id);
                            reconcile_local_stale_shards(self_id_, storage_dir_, m);
                        }
                    }
                    if (resp.contains("recovery_records") && resp["recovery_records"].is_array()) {
                        for (const auto& r_json : resp["recovery_records"].items()) {
                            RecoveryRecordEntry r = RecoveryRecordEntry::from_json(r_json);
                            if (!r.recovery_id.empty()) db_.upsert_recovery_record(r);
                        }
                    }
                }
            }
        }

        // 2. Identify manifests that we have which remote is missing or has older version of
        for (const auto& kv : local_summary) {
            auto it = remote_summary.find(kv.first);
            if (it == remote_summary.end() || kv.second > it->second) {
                FileManifest local_m;
                if (db_.get_file_manifest(kv.first, local_m)) {
                    TCPClient client(500, 500);
                    if (client.connect_to(remote.address, remote.port)) {
                        json::Json push_req = json::Json::object();
                        push_req["type"] = proto::P2P_METADATA_PUSH;
                        push_req["origin_peer_id"] = self_id_;
                        push_req["propagate"] = false;
                        push_req["manifest"] = local_m.to_json();
                        const auto recs = db_.get_recovery_records(local_m.file_id);
                        if (!recs.empty()) {
                            json::Json r_arr = json::Json::array();
                            for (const auto& r : recs) r_arr.push_back(r.to_json());
                            push_req["recovery_records"] = r_arr;
                        }
                        json::Json push_resp;
                        std::vector<uint8_t> ppl;
                        client.request(push_req, {}, push_resp, ppl);
                    }
                }
            }
        }
    }

private:
    void update_self_record() {
        PeerInfo self_info;
        self_info.id = self_id_;
        self_info.address = self_address_;
        self_info.port = self_port_;
        self_info.state = PeerHealth::ONLINE;
        self_info.workload = current_workload(storage_dir_);

        std::string msg;
        registry_.register_peer(self_info, msg);
        db_.upsert_peer(self_info, DatabaseManager::now_epoch_ms(), "SELF", true);
    }

    static std::string make_recovery_id(const std::string& file_id,
                                        int32_t stripe_index,
                                        const std::string& shard_id) {
        std::ostringstream ss;
        ss << "REC:" << file_id << ":S" << std::setfill('0') << std::setw(3)
           << stripe_index << ":" << shard_id;
        return ss.str();
    }

    // Request a distributed recovery lease from all online remote peers and record in local DB
    bool acquire_recovery_lease(const std::string& recovery_id,
                                const std::string& failed_peer_id,
                                const std::string& shard_id,
                                const std::string& file_id,
                                int32_t stripe_index,
                                int64_t lease_ttl_ms = 2500) {
        const int64_t now_ts = DatabaseManager::now_epoch_ms();
        RecoveryRecordEntry existing;
        if (db_.get_recovery_record(recovery_id, existing)) {
            if (existing.status == "COMPLETED") return false;
            if (existing.status == "STARTED" &&
                existing.rebuilder_peer_id != self_id_ &&
                (now_ts - existing.timestamp_epoch_ms) < lease_ttl_ms) {
                PeerInfo holder_info;
                const bool holder_offline =
                    registry_.get_peer(existing.rebuilder_peer_id, holder_info) &&
                    holder_info.state == PeerHealth::OFFLINE;
                if (!holder_offline) {
                    return false;
                }
            }
        }

        // Query online remote peers for lease approval
        const auto peers = registry_.get_all_peers();
        for (const auto& p : peers) {
            if (p.id == self_id_ || p.state != PeerHealth::ONLINE || p.port == 0) continue;
            TCPClient client(400, 400);
            if (!client.connect_to(p.address, p.port)) continue;

            json::Json req = json::Json::object();
            req["type"] = proto::P2P_RECOVERY_LEASE_REQ;
            req["recovery_id"] = recovery_id;
            req["failed_peer_id"] = failed_peer_id;
            req["shard_id"] = shard_id;
            req["file_id"] = file_id;
            req["stripe_index"] = static_cast<long long>(stripe_index);
            req["rebuilder_peer_id"] = self_id_;
            req["timestamp_epoch_ms"] = static_cast<long long>(now_ts);

            json::Json resp;
            std::vector<uint8_t> pl;
            if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_RECOVERY_LEASE_ACK) {
                if (!resp["granted"].as_bool()) {
                    if (resp.contains("record") && resp["record"].is_object()) {
                        db_.upsert_recovery_record(RecoveryRecordEntry::from_json(resp["record"]));
                    }
                    return false;
                }
            }
        }

        RecoveryRecordEntry started;
        started.recovery_id = recovery_id;
        started.failed_peer_id = failed_peer_id;
        started.shard_id = shard_id;
        started.file_id = file_id;
        started.stripe_index = stripe_index;
        started.rebuilder_peer_id = self_id_;
        started.target_peer_id = "";
        started.status = "STARTED";
        started.reason = "lease acquired by " + self_id_;
        started.timestamp_epoch_ms = now_ts;
        db_.upsert_recovery_record(started);
        return true;
    }

public:
    // Autonomous repair scan: inspects known manifests for shards stored on OFFLINE peers
    // (or locally corrupted shards), coordinates deterministic lease ownership, reconstructs
    // missing shards via RS(3,2), stores replacement shards on eligible online peers, and
    // synchronizes updated manifests across the peer network.
    int run_autonomous_recovery_scan(uint64_t cycle = 0,
                                     bool force_owner = false,
                                     uint64_t override_min_free_bytes = 0,
                                     bool simulate_interrupted_store = false) {
        if (!running_.load() && !force_owner) return 0;

        const auto all_peers = registry_.get_all_peers();
        std::map<std::string, PeerInfo> peer_map;
        std::vector<std::string> online_peer_ids;
        for (const auto& p : all_peers) {
            peer_map[p.id] = p;
            if (p.state == PeerHealth::ONLINE) {
                online_peer_ids.push_back(p.id);
            }
        }
        // Guarantee self_id_ is always present and marked ONLINE in peer_map & online_peer_ids
        if (peer_map.find(self_id_) == peer_map.end()) {
            PeerInfo self_p;
            self_p.id = self_id_;
            self_p.address = self_address_;
            self_p.port = self_port_;
            self_p.state = PeerHealth::ONLINE;
            self_p.workload = current_workload(storage_dir_);
            peer_map[self_id_] = self_p;
        } else {
            peer_map[self_id_].state = PeerHealth::ONLINE;
        }
        online_peer_ids.push_back(self_id_);
        std::sort(online_peer_ids.begin(), online_peer_ids.end());
        online_peer_ids.erase(std::unique(online_peer_ids.begin(), online_peer_ids.end()),
                              online_peer_ids.end());
        if (online_peer_ids.empty()) return 0;

        int repaired_count = 0;
        bool did_simulate_interrupt = false;
        const auto manifests = db_.list_file_manifests();

        for (const auto& orig_m : manifests) {
            if (orig_m.status == "UPLOADING" || orig_m.shards.empty()) continue;
            // Skip synthetic metadata-only test manifests that do not carry 64-hex SHA-256 shard digests
            if (orig_m.shards[0].sha256.size() != 64) continue;

            FileManifest m = orig_m;
            bool manifest_modified = false;
            std::vector<RecoveryRecordEntry> new_recovery_records;

            // Group shard indices in m.shards by stripe_index
            std::map<int32_t, std::vector<size_t>> stripe_to_pos;
            for (size_t idx = 0; idx < m.shards.size(); ++idx) {
                stripe_to_pos[m.shards[idx].stripe_index].push_back(idx);
            }

            int32_t unrecoverable_stripes = 0;
            int32_t degraded_stripes = 0;

            for (const auto& st_kv : stripe_to_pos) {
                const int32_t stripe_idx = st_kv.first;
                const auto& pos_list = st_kv.second;
                if (pos_list.size() < 3) continue;

                // When explicitly triggered (or repairing), verify local on-disk integrity for shards hosted on self_id_
                if (force_owner) {
                    for (size_t pos : pos_list) {
                        auto& sh = m.shards[pos];
                        for (auto& loc : sh.locations) {
                            if (loc.peer_id == self_id_ && loc.location_status == "STORED") {
                                const std::string lpath = storage_dir_ + "/" + sh.shard_id + ".chunk";
                                std::error_code ec;
                                if (!fs::exists(fs::u8path(lpath), ec) ||
                                    fs::file_size(fs::u8path(lpath), ec) != sh.shard_size ||
                                    ChunkManager::sha256_of_file(lpath) != sh.sha256) {
                                    loc.location_status = "CORRUPTED";
                                    loc.version = m.version + 1;
                                    loc.verified_at_epoch_ms = DatabaseManager::now_epoch_ms();
                                    fs::remove(fs::u8path(lpath), ec);
                                    manifest_modified = true;
                                }
                            }
                        }
                    }
                }

                // Identify which shards in this stripe have an ONLINE + STORED peer
                // Note: per Task 5.1, SUSPECT peers do NOT trigger recovery; only confirmed
                // OFFLINE peers (or CORRUPTED/STALE locations) trigger recovery when no
                // ONLINE or SUSPECT peer holds a STORED copy of that shard.
                struct ShardHealthInfo {
                    size_t manifest_pos = 0;
                    bool has_online_stored = false;
                    bool has_suspect_stored = false;
                    std::string online_peer_id;
                    std::string failed_peer_id;
                };

                std::vector<ShardHealthInfo> sh_states;
                std::set<std::string> stripe_active_peers;

                for (size_t pos : pos_list) {
                    const auto& sh = m.shards[pos];
                    ShardHealthInfo info;
                    info.manifest_pos = pos;
                    for (const auto& loc : sh.locations) {
                        if (loc.location_status == "STORED") {
                            auto pit = peer_map.find(loc.peer_id);
                            if (pit != peer_map.end()) {
                                if (pit->second.state == PeerHealth::ONLINE) {
                                    info.has_online_stored = true;
                                    info.online_peer_id = loc.peer_id;
                                    stripe_active_peers.insert(loc.peer_id);
                                } else if (pit->second.state == PeerHealth::SUSPECT) {
                                    info.has_suspect_stored = true;
                                    stripe_active_peers.insert(loc.peer_id);
                                } else if (pit->second.state == PeerHealth::OFFLINE) {
                                    info.failed_peer_id = loc.peer_id;
                                }
                            }
                        } else if (loc.location_status == "CORRUPTED" && info.failed_peer_id.empty()) {
                            info.failed_peer_id = loc.peer_id;
                        }
                    }
                    sh_states.push_back(info);
                }

                // Count how many shards need repair (neither ONLINE nor SUSPECT STORED copy)
                std::vector<ShardHealthInfo> missing_shards;
                for (const auto& st : sh_states) {
                    if (!st.has_online_stored && !st.has_suspect_stored) {
                        missing_shards.push_back(st);
                    }
                }

                if (missing_shards.empty()) {
                    continue; // All 3 shards are healthy (or only transiently SUSPECT)
                }

                // Task 5.2: If more than 1 shard is missing in an RS(3,2) stripe (< 2 surviving),
                // reconstruction is impossible -> mark UNRECOVERABLE
                if (missing_shards.size() > 1) {
                    ++unrecoverable_stripes;
                    for (const auto& ms : missing_shards) {
                        const auto& bad_sh = m.shards[ms.manifest_pos];
                        const std::string rec_id = make_recovery_id(m.file_id, stripe_idx, bad_sh.shard_id);
                        RecoveryRecordEntry unrec;
                        unrec.recovery_id = rec_id;
                        unrec.failed_peer_id = ms.failed_peer_id.empty() ? "UNKNOWN" : ms.failed_peer_id;
                        unrec.shard_id = bad_sh.shard_id;
                        unrec.file_id = m.file_id;
                        unrec.stripe_index = stripe_idx;
                        unrec.rebuilder_peer_id = self_id_;
                        unrec.target_peer_id = "";
                        unrec.status = "UNRECOVERABLE";
                        unrec.reason = "fewer than 2 surviving shards available in stripe";
                        unrec.timestamp_epoch_ms = DatabaseManager::now_epoch_ms();
                        db_.upsert_recovery_record(unrec);
                        new_recovery_records.push_back(unrec);
                    }
                    continue;
                }

                // Exactly 1 shard is missing in this stripe!
                const ShardHealthInfo& miss_info = missing_shards[0];
                ShardManifestEntry& target_sh = m.shards[miss_info.manifest_pos];
                const std::string rec_id = make_recovery_id(m.file_id, stripe_idx, target_sh.shard_id);

                // Task 5.3: Deterministic owner selection + backup takeover
                uint64_t hval = 5381;
                for (unsigned char c : m.file_id) hval = ((hval << 5) + hval) + c;
                const size_t primary_idx = static_cast<size_t>(hval % online_peer_ids.size());
                const std::string primary_owner = online_peer_ids[primary_idx];

                RecoveryRecordEntry prev_rec;
                const bool has_prev = db_.get_recovery_record(rec_id, prev_rec);
                if (has_prev && prev_rec.status == "COMPLETED") {
                    // Check if the target_peer_id from the completed record is still online
                    auto t_it = peer_map.find(prev_rec.target_peer_id);
                    if (t_it != peer_map.end() && t_it->second.state == PeerHealth::ONLINE) {
                        continue;
                    }
                }

                const bool lease_expired = has_prev && prev_rec.status == "STARTED" &&
                                           (DatabaseManager::now_epoch_ms() - prev_rec.timestamp_epoch_ms >= 2500);
                const bool is_my_turn = force_owner ||
                                        (self_id_ == primary_owner) ||
                                        lease_expired ||
                                        (cycle >= 3 && (cycle % online_peer_ids.size() == 0));
                if (!is_my_turn) {
                    ++degraded_stripes;
                    continue;
                }

                if (!acquire_recovery_lease(rec_id,
                                            miss_info.failed_peer_id.empty() ? "UNKNOWN" : miss_info.failed_peer_id,
                                            target_sh.shard_id, m.file_id, stripe_idx)) {
                    ++degraded_stripes;
                    continue;
                }

                // Task 5.2 & 5.4: Retrieve the 2 surviving shards directly from their storage peers
                // and verify their SHA-256 hashes
                const size_t shard_len = static_cast<size_t>(target_sh.shard_size > 0 ? target_sh.shard_size : m.shard_size);
                std::vector<std::vector<uint8_t>> bufs(3, std::vector<uint8_t>(shard_len, 0));
                std::vector<bool> present(3, false);
                size_t valid_surviving = 0;

                for (size_t pos : pos_list) {
                    if (pos == miss_info.manifest_pos) continue;
                    const auto& surv_sh = m.shards[pos];
                    const int32_t slot = surv_sh.shard_index;
                    if (slot < 0 || slot >= 3) continue;

                    for (const auto& loc : surv_sh.locations) {
                        if (loc.location_status != "STORED") continue;
                        if (loc.peer_id == self_id_) {
                            const std::string lpath = storage_dir_ + "/" + surv_sh.shard_id + ".chunk";
                            std::error_code ec;
                            if (fs::exists(fs::u8path(lpath), ec) &&
                                fs::file_size(fs::u8path(lpath), ec) == shard_len) {
                                std::vector<uint8_t> data = read_binary(lpath);
                                if (data.size() == shard_len &&
                                    ChunkManager::sha256_of_bytes(data) == surv_sh.sha256) {
                                    bufs[slot] = std::move(data);
                                    present[slot] = true;
                                    ++valid_surviving;
                                    break;
                                } else {
                                    // Local surviving shard is corrupted!
                                    for (auto& mut_loc : m.shards[pos].locations) {
                                        if (mut_loc.peer_id == self_id_) {
                                            mut_loc.location_status = "CORRUPTED";
                                            mut_loc.version = m.version + 1;
                                        }
                                    }
                                    manifest_modified = true;
                                }
                            }
                            continue;
                        }

                        auto pit = peer_map.find(loc.peer_id);
                        if (pit == peer_map.end() || pit->second.state == PeerHealth::OFFLINE || pit->second.port == 0) {
                            continue;
                        }

                        TCPClient client(2000, 2000);
                        if (!client.connect_to(pit->second.address, pit->second.port)) continue;

                        json::Json get_req = json::Json::object();
                        get_req["type"] = proto::GET_CHUNK;
                        get_req["chunk_id"] = surv_sh.shard_id;
                        json::Json get_resp;
                        std::vector<uint8_t> pl;
                        if (client.request(get_req, {}, get_resp, pl) == proto::RecvStatus::OK &&
                            get_resp["type"].as_string() == proto::CHUNK_RESPONSE) {
                            if (pl.size() == shard_len &&
                                ChunkManager::sha256_of_bytes(pl) == surv_sh.sha256) {
                                bufs[slot] = std::move(pl);
                                present[slot] = true;
                                ++valid_surviving;
                                break;
                            } else {
                                // Surviving shard on remote peer failed SHA-256 verification!
                                for (auto& mut_loc : m.shards[pos].locations) {
                                    if (mut_loc.peer_id == loc.peer_id) {
                                        mut_loc.location_status = "CORRUPTED";
                                        mut_loc.version = m.version + 1;
                                    }
                                }
                                manifest_modified = true;
                            }
                        }
                    }
                }

                if (valid_surviving < 2) {
                    ++unrecoverable_stripes;
                    RecoveryRecordEntry fail_rec;
                    fail_rec.recovery_id = rec_id;
                    fail_rec.failed_peer_id = miss_info.failed_peer_id.empty() ? "UNKNOWN" : miss_info.failed_peer_id;
                    fail_rec.shard_id = target_sh.shard_id;
                    fail_rec.file_id = m.file_id;
                    fail_rec.stripe_index = stripe_idx;
                    fail_rec.rebuilder_peer_id = self_id_;
                    fail_rec.target_peer_id = "";
                    fail_rec.status = "UNRECOVERABLE";
                    fail_rec.reason = "surviving shards failed retrieval or SHA-256 verification (<2 valid)";
                    fail_rec.timestamp_epoch_ms = DatabaseManager::now_epoch_ms();
                    db_.upsert_recovery_record(fail_rec);
                    new_recovery_records.push_back(fail_rec);
                    continue;
                }

                // Task 5.4: Reconstruct the missing shard via RS(3,2) and verify its expected SHA-256
                ReedSolomon rs(2, 1);
                std::vector<uint8_t*> ptrs = {bufs[0].data(), bufs[1].data(), bufs[2].data()};
                const int32_t target_slot = target_sh.shard_index;
                if (target_slot < 0 || target_slot >= 3 || !rs.decode(ptrs, present, shard_len)) {
                    ++degraded_stripes;
                    continue;
                }

                const std::string rebuilt_sha = ChunkManager::sha256_of_bytes(bufs[target_slot]);
                if (rebuilt_sha != target_sh.sha256) {
                    ++unrecoverable_stripes;
                    RecoveryRecordEntry bad_rec;
                    bad_rec.recovery_id = rec_id;
                    bad_rec.failed_peer_id = miss_info.failed_peer_id.empty() ? "UNKNOWN" : miss_info.failed_peer_id;
                    bad_rec.shard_id = target_sh.shard_id;
                    bad_rec.file_id = m.file_id;
                    bad_rec.stripe_index = stripe_idx;
                    bad_rec.rebuilder_peer_id = self_id_;
                    bad_rec.target_peer_id = "";
                    bad_rec.status = "FAILED";
                    bad_rec.reason = "reconstructed shard SHA-256 mismatch";
                    bad_rec.timestamp_epoch_ms = DatabaseManager::now_epoch_ms();
                    db_.upsert_recovery_record(bad_rec);
                    new_recovery_records.push_back(bad_rec);
                    continue;
                }

                // Task 5.5: Select an eligible replacement peer (ONLINE, sufficient free storage,
                // does NOT already hold another shard of the same stripe)
                std::vector<PeerInfo> replacement_candidates;
                for (const auto& p : all_peers) {
                    if (p.state != PeerHealth::ONLINE) continue;
                    if (stripe_active_peers.find(p.id) != stripe_active_peers.end()) continue;
                    if (p.workload.available_storage_bytes > 0 &&
                        p.workload.available_storage_bytes < shard_len) {
                        continue;
                    }
                    if (override_min_free_bytes > 0 &&
                        p.workload.available_storage_bytes < override_min_free_bytes) {
                        continue;
                    }
                    replacement_candidates.push_back(p);
                }

                std::sort(replacement_candidates.begin(), replacement_candidates.end(),
                          [](const PeerInfo& a, const PeerInfo& b) {
                              if (a.workload.active_transfers != b.workload.active_transfers)
                                  return a.workload.active_transfers < b.workload.active_transfers;
                              if (a.workload.available_storage_bytes != b.workload.available_storage_bytes)
                                  return a.workload.available_storage_bytes > b.workload.available_storage_bytes;
                              return a.id < b.id;
                          });

                if (replacement_candidates.empty()) {
                    ++degraded_stripes;
                    RecoveryRecordEntry nospace_rec;
                    nospace_rec.recovery_id = rec_id;
                    nospace_rec.failed_peer_id = miss_info.failed_peer_id.empty() ? "UNKNOWN" : miss_info.failed_peer_id;
                    nospace_rec.shard_id = target_sh.shard_id;
                    nospace_rec.file_id = m.file_id;
                    nospace_rec.stripe_index = stripe_idx;
                    nospace_rec.rebuilder_peer_id = self_id_;
                    nospace_rec.target_peer_id = "";
                    nospace_rec.status = "FAILED";
                    nospace_rec.reason = "no eligible replacement peer with sufficient free storage";
                    nospace_rec.timestamp_epoch_ms = DatabaseManager::now_epoch_ms();
                    db_.upsert_recovery_record(nospace_rec);
                    new_recovery_records.push_back(nospace_rec);
                    continue;
                }

                // Re-verify from local DB before storing that another peer hasn't already completed
                // this recovery while GET_CHUNK / RS(3,2) decode was in flight (prevents duplicate repairs)
                {
                    RecoveryRecordEntry mid_rec;
                    if (db_.get_recovery_record(rec_id, mid_rec) && mid_rec.status == "COMPLETED") {
                        auto t_it = peer_map.find(mid_rec.target_peer_id);
                        if (t_it != peer_map.end() && t_it->second.state == PeerHealth::ONLINE) {
                            continue;
                        }
                    }
                }

                // Task 5.6: Send reconstructed shard to replacement peer via STORE_CHUNK (with retry)
                std::string chosen_target_peer;
                uint64_t stripe_data_b = 0;
                for (const auto& st_entry : m.stripes) {
                    if (st_entry.stripe_index == stripe_idx) {
                        stripe_data_b = st_entry.stripe_data_bytes;
                        break;
                    }
                }

                for (const auto& cand : replacement_candidates) {
                    bool stored_ok = false;
                    for (int attempt = 1; attempt <= 2; ++attempt) {
                        TCPClient sclient(2500, 5000);
                        if (!sclient.connect_to(cand.address, cand.port)) continue;

                        std::vector<uint8_t> send_payload = bufs[target_slot];
                        if (simulate_interrupted_store && !did_simulate_interrupt && attempt == 1) {
                            did_simulate_interrupt = true;
                            send_payload.resize(send_payload.size() / 2);
                        }

                        json::Json store_req = json::Json::object();
                        store_req["type"] = proto::STORE_CHUNK;
                        store_req["chunk_id"] = target_sh.shard_id;
                        store_req["sha256"] = target_sh.sha256;
                        store_req["expected_size"] = static_cast<long long>(shard_len);
                        if (override_min_free_bytes > 0) {
                            store_req["require_min_free_bytes"] = static_cast<long long>(override_min_free_bytes);
                        }
                        store_req["file_id"] = m.file_id;
                        store_req["original_name"] = m.original_name;
                        store_req["file_size"] = static_cast<long long>(m.file_size);
                        store_req["file_sha256"] = m.sha256;
                        store_req["total_stripes"] = static_cast<long long>(m.total_stripes);
                        store_req["total_shards"] = static_cast<long long>(m.total_shards);
                        store_req["uploader_peer_id"] = m.uploader_peer_id;
                        store_req["stripe_index"] = static_cast<long long>(stripe_idx);
                        store_req["stripe_data_bytes"] = static_cast<long long>(stripe_data_b);
                        store_req["shard_index"] = static_cast<long long>(target_sh.shard_index);
                        store_req["shard_type"] = target_sh.shard_type;
                        store_req["original_bytes"] = static_cast<long long>(target_sh.original_bytes);

                        json::Json store_resp;
                        std::vector<uint8_t> spl;
                        if (sclient.request(store_req, send_payload, store_resp, spl) == proto::RecvStatus::OK &&
                            store_resp["type"].as_string() == proto::STORE_ACK &&
                            store_resp["status"].as_string() == "OK" &&
                            store_resp["sha256"].as_string() == target_sh.sha256) {
                            stored_ok = true;
                            break;
                        }
                    }
                    if (stored_ok) {
                        chosen_target_peer = cand.id;
                        break;
                    }
                }

                if (chosen_target_peer.empty()) {
                    ++degraded_stripes;
                    RecoveryRecordEntry store_fail;
                    store_fail.recovery_id = rec_id;
                    store_fail.failed_peer_id = miss_info.failed_peer_id.empty() ? "UNKNOWN" : miss_info.failed_peer_id;
                    store_fail.shard_id = target_sh.shard_id;
                    store_fail.file_id = m.file_id;
                    store_fail.stripe_index = stripe_idx;
                    store_fail.rebuilder_peer_id = self_id_;
                    store_fail.target_peer_id = "";
                    store_fail.status = "FAILED";
                    store_fail.reason = "failed to store reconstructed shard on replacement peer";
                    store_fail.timestamp_epoch_ms = DatabaseManager::now_epoch_ms();
                    db_.upsert_recovery_record(store_fail);
                    new_recovery_records.push_back(store_fail);
                    continue;
                }

                // Mark old failed peer location as STALE and add new STORED location on chosen_target_peer
                const int64_t commit_ts = DatabaseManager::now_epoch_ms();
                const int32_t next_ver = m.version + 1;
                for (auto& loc : target_sh.locations) {
                    if (loc.location_status == "STORED") {
                        loc.location_status = "STALE";
                        loc.verified_at_epoch_ms = commit_ts;
                        loc.version = next_ver;
                    }
                }
                bool found_target_loc = false;
                for (auto& loc : target_sh.locations) {
                    if (loc.peer_id == chosen_target_peer) {
                        loc.location_status = "STORED";
                        loc.verified_at_epoch_ms = commit_ts;
                        loc.version = next_ver;
                        found_target_loc = true;
                        break;
                    }
                }
                if (!found_target_loc) {
                    ShardLocationEntry new_loc;
                    new_loc.shard_id = target_sh.shard_id;
                    new_loc.peer_id = chosen_target_peer;
                    new_loc.location_status = "STORED";
                    new_loc.verified_at_epoch_ms = commit_ts;
                    new_loc.version = next_ver;
                    target_sh.locations.push_back(new_loc);
                }

                RecoveryRecordEntry comp_rec;
                comp_rec.recovery_id = rec_id;
                comp_rec.failed_peer_id = miss_info.failed_peer_id.empty() ? "UNKNOWN" : miss_info.failed_peer_id;
                comp_rec.shard_id = target_sh.shard_id;
                comp_rec.file_id = m.file_id;
                comp_rec.stripe_index = stripe_idx;
                comp_rec.rebuilder_peer_id = self_id_;
                comp_rec.target_peer_id = chosen_target_peer;
                comp_rec.status = "COMPLETED";
                comp_rec.reason = "reconstructed via RS(3,2) and stored on " + chosen_target_peer;
                comp_rec.timestamp_epoch_ms = commit_ts;
                db_.upsert_recovery_record(comp_rec);
                new_recovery_records.push_back(comp_rec);

                std::cout << "[peer " << self_id_ << "][P2P-Recovery] Rebuilt "
                          << target_sh.shard_id << " (from failed peer " << comp_rec.failed_peer_id
                          << ") -> stored on Peer " << chosen_target_peer << std::endl;
                g_telemetry.add_event("RECOVERY", "COMPLETED",
                                      "Rebuilt shard " + target_sh.shard_id + " (from failed peer " +
                                      comp_rec.failed_peer_id + ") -> stored on Peer " + chosen_target_peer);

                manifest_modified = true;
                ++repaired_count;
                if (repaired_count % 2 == 0) {
                    const PeerWorkload keep_w = current_workload(storage_dir_);
                    for (const auto& hp : registry_.get_all_peers()) {
                        if (hp.id == self_id_ || hp.state != PeerHealth::ONLINE || hp.port == 0) continue;
                        TCPClient hb_c(120, 200);
                        if (hb_c.connect_to(hp.address, hp.port)) {
                            json::Json hb_req = json::Json::object();
                            hb_req["type"] = proto::P2P_HEARTBEAT;
                            hb_req["peer_id"] = self_id_;
                            hb_req["address"] = self_address_;
                            hb_req["port"] = static_cast<long long>(self_port_);
                            hb_req["available_storage_bytes"] = static_cast<long long>(keep_w.available_storage_bytes);
                            hb_req["active_transfers"] = static_cast<long long>(keep_w.active_transfers);
                            hb_req["transfer_capacity"] = static_cast<long long>(keep_w.transfer_capacity);
                            json::Json hb_resp;
                            std::vector<uint8_t> hb_pl;
                            if (hb_c.request(hb_req, {}, hb_resp, hb_pl) == proto::RecvStatus::OK) {
                                registry_.record_heartbeat(hp.id, hp.workload);
                            }
                        }
                    }
                }
            }

            if (unrecoverable_stripes > 0 && m.status != "UNRECOVERABLE") {
                m.status = "UNRECOVERABLE";
                manifest_modified = true;
                g_telemetry.add_event("RECOVERY", "UNRECOVERABLE",
                                      "File " + m.file_id + " marked UNRECOVERABLE (<2 surviving shards in stripe)");
            } else if (unrecoverable_stripes == 0 && degraded_stripes == 0 && manifest_modified) {
                m.status = "AVAILABLE";
            }

            if (manifest_modified) {
                m.version += 1;
                db_.upsert_file_manifest(m, self_id_);
                broadcast_manifest_to_peers(self_id_, registry_, m, new_recovery_records);
            } else if (!new_recovery_records.empty()) {
                broadcast_manifest_to_peers(self_id_, registry_, m, new_recovery_records);
            }
        }

        return repaired_count;
    }

private:

    void on_peer_discovered(const PeerInfo& discovered, const std::string& source) {
        if (!running_.load() || discovered.id.empty() || discovered.id == self_id_) return;

        PeerInfo existing;
        const bool known = registry_.get_peer(discovered.id, existing);
        if (!known || existing.state != PeerHealth::ONLINE ||
            existing.address != discovered.address || existing.port != discovered.port) {
            if (perform_tcp_handshake(discovered, source)) {
                g_telemetry.add_event("NETWORK", "ONLINE",
                                      "Discovered & verified Peer " + discovered.id + " at " +
                                      discovered.address + ":" + std::to_string(discovered.port) +
                                      " via " + source);
            }
        }
    }

    void run_loop() {
        uint64_t cycle = 0;
        while (running_.load()) {
            int remaining = hb_interval_ms_;
            while (remaining > 0 && running_.load()) {
                const int step = remaining > 50 ? 50 : remaining;
                std::this_thread::sleep_for(std::chrono::milliseconds(step));
                remaining -= step;
            }
            if (!running_.load()) break;
            ++cycle;

            update_self_record();

            const auto all_peers = registry_.get_all_peers();
            const PeerWorkload self_w = current_workload(storage_dir_);

            for (const auto& p : all_peers) {
                if (!running_.load()) break;
                if (p.id == self_id_) continue;

                const bool was_online = (p.state == PeerHealth::ONLINE);

                TCPClient client(150, 250);
                if (client.connect_to(p.address, p.port)) {
                    json::Json req = json::Json::object();
                    req["type"] = proto::P2P_HEARTBEAT;
                    req["peer_id"] = self_id_;
                    req["address"] = self_address_;
                    req["port"] = static_cast<long long>(self_port_);
                    req["available_storage_bytes"] = static_cast<long long>(self_w.available_storage_bytes);
                    req["active_transfers"] = static_cast<long long>(self_w.active_transfers);
                    req["transfer_capacity"] = static_cast<long long>(self_w.transfer_capacity);

                    json::Json resp;
                    std::vector<uint8_t> pl;
                    if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                        resp["type"].as_string() == proto::P2P_HEARTBEAT_ACK) {
                        PeerWorkload rw = p.workload;
                        if (resp.contains("available_storage_bytes")) {
                            rw.available_storage_bytes =
                                static_cast<uint64_t>(resp["available_storage_bytes"].as_int());
                        }
                        if (resp.contains("active_transfers")) {
                            rw.active_transfers =
                                static_cast<uint32_t>(resp["active_transfers"].as_int());
                        }
                        if (resp.contains("transfer_capacity")) {
                            rw.transfer_capacity =
                                static_cast<uint32_t>(resp["transfer_capacity"].as_int());
                        }
                        registry_.record_heartbeat(p.id, rw);

                        PeerInfo updated = p;
                        updated.state = PeerHealth::ONLINE;
                        updated.workload = rw;
                        db_.upsert_peer(updated, DatabaseManager::now_epoch_ms(), "P2P_HEARTBEAT", false);

                        // Synchronize metadata immediately when a peer transitions back to ONLINE
                        // or periodically every 2 heartbeat cycles (~500ms)
                        if (!was_online || (cycle % 2 == 0)) {
                            if (!was_online) {
                                g_telemetry.add_event("NETWORK", "ONLINE",
                                                      "Peer " + p.id + " rejoined ONLINE at " +
                                                      p.address + ":" + std::to_string(p.port));
                            }
                            sync_metadata_with_peer(updated);
                        }
                    }
                }
            }

            // Evaluate health progression (ONLINE -> SUSPECT -> OFFLINE)
            const auto transitions = registry_.evaluate_health(hb_timeout_ms_, failure_threshold_);
            for (const auto& t : transitions) {
                std::cout << "[peer " << self_id_ << "][P2P] " << t << std::endl;
                const std::string sev = (t.find("OFFLINE") != std::string::npos) ? "OFFLINE" : "SUSPECT";
                g_telemetry.add_event("NETWORK", sev, t);
            }

            // Synchronize any health state changes to SQLite
            bool any_peer_offline = false;
            for (const auto& p : registry_.get_all_peers()) {
                if (p.id == self_id_) continue;
                db_.update_peer_health(p.id, p.state);
                if (p.state == PeerHealth::OFFLINE) any_peer_offline = true;
            }

            // Task 5.1 & 5.7: Autonomous background repair monitor (triggered when any peer is OFFLINE)
            if (any_peer_offline) {
                run_autonomous_recovery_scan(cycle, false, 0, false);
            }
        }
    }

    std::string self_id_;
    std::string self_address_;
    uint16_t self_port_;
    uint16_t discovery_port_;
    std::string storage_dir_;
    PeerRegistry& registry_;
    DatabaseManager& db_;
    int hb_interval_ms_;
    int hb_timeout_ms_;
    int failure_threshold_;

    UDPBeacon beacon_;
    std::atomic<bool> running_{false};
    std::thread monitor_thread_;
};

// ============================================================================
// PeerRegistrar - Gate-2 Coordinator Registration (backward compatibility)
// ============================================================================
class PeerRegistrar {
public:
    PeerRegistrar(std::string id, std::string peer_address, uint16_t peer_port,
                  std::string coord_address, uint16_t coord_port,
                  std::string storage_dir)
        : id_(std::move(id)),
          peer_address_(std::move(peer_address)),
          peer_port_(peer_port),
          coord_address_(std::move(coord_address)),
          coord_port_(coord_port),
          storage_dir_(std::move(storage_dir)) {}

    ~PeerRegistrar() { stop(); }

    void start() {
        if (coord_address_.empty() || coord_port_ == 0) return;
        running_.store(true);
        thread_ = std::thread(&PeerRegistrar::run, this);
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (thread_.joinable()) thread_.join();
    }

private:
    std::string id_;
    std::string peer_address_;
    uint16_t peer_port_;
    std::string coord_address_;
    uint16_t coord_port_;
    std::string storage_dir_;

    std::atomic<bool> running_{false};
    std::thread thread_;

    uint64_t get_available_storage() const {
        return query_available_storage(storage_dir_);
    }

    void run() {
        bool registered = false;
        int heartbeat_interval_ms = 500;

        while (running_.load() && !registered) {
            TCPClient client(1500, 1500);
            if (client.connect_to(coord_address_, coord_port_)) {
                json::Json req = json::Json::object();
                req["type"] = proto::PEER_REGISTER;
                req["peer_id"] = id_;
                req["address"] = peer_address_;
                req["port"] = static_cast<long long>(peer_port_);
                req["available_storage_bytes"] = static_cast<long long>(get_available_storage());
                req["active_transfers"] = static_cast<long long>(g_active_transfers.load());
                req["transfer_capacity"] = 10LL;

                json::Json resp;
                std::vector<uint8_t> pl;
                if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                    resp["type"].as_string() == proto::PEER_REGISTER_ACK) {
                    registered = true;
                    if (resp.contains("heartbeat_interval_ms"))
                        heartbeat_interval_ms = static_cast<int>(resp["heartbeat_interval_ms"].as_int());
                    std::cout << "[peer " << id_ << "] Dynamically registered with Coordinator at "
                              << coord_address_ << ":" << coord_port_ << std::endl;
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        while (running_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_interval_ms));
            if (!running_.load()) break;

            TCPClient client(1000, 1000);
            if (client.connect_to(coord_address_, coord_port_)) {
                json::Json req = json::Json::object();
                req["type"] = proto::PEER_HEARTBEAT;
                req["peer_id"] = id_;
                req["available_storage_bytes"] = static_cast<long long>(get_available_storage());
                req["active_transfers"] = static_cast<long long>(g_active_transfers.load());
                req["transfer_capacity"] = 10LL;

                json::Json resp;
                std::vector<uint8_t> pl;
                client.request(req, {}, resp, pl);
            }
        }
    }
};

}  // namespace

int main(int argc, char** argv) {
    std::vector<std::string> args = proc::command_line_args(argc, argv);

    if (args.size() >= 2 && (args[1] == "--help" || args[1] == "-h" || args[1] == "help")) {
        std::cout << "usage: peer <ID> <address> <port> [p2p|coordinator_address] [discovery_port|coordinator_port]\n"
                  << "            [--seed <ip:port>] [--max-storage-mb <MB>] [--storage-dir <path>]\n"
                  << "            [--ui-port <port>] [--no-ui] [--ui-lan] [--ui-token <token>]\n"
                  << "   or: peer --config <peer_config.json>\n";
        return 0;
    }

    std::string config_file_path;
    std::string custom_storage_root;
    std::vector<std::pair<std::string, uint16_t>> cli_seeds;
    uint64_t cli_max_storage_bytes = 0;
    uint16_t cli_ui_port = 0;
    bool cli_ui_port_explicit = false;
    bool ui_enabled = true;
    bool ui_lan_access = false;
    std::string ui_auth_token;

    // Extract optional flags (--config <file>, --seed <ip:port>, --max-storage-mb <MB>, --storage-dir <path>, --ui-port <port>, --no-ui, --ui-lan, --ui-token <token>)
    for (size_t i = 1; i < args.size();) {
        if (args[i] == "--config" && i + 1 < args.size()) {
            config_file_path = args[i + 1];
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
        } else if (args[i] == "--storage-dir" && i + 1 < args.size()) {
            custom_storage_root = args[i + 1];
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
        } else if (args[i] == "--seed" && i + 1 < args.size()) {
            const std::string ep = args[i + 1];
            const size_t colon = ep.rfind(':');
            if (colon != std::string::npos && colon > 0 && colon + 1 < ep.size()) {
                try {
                    const int sp = std::stoi(ep.substr(colon + 1));
                    if (sp >= 1 && sp <= 65535) {
                        cli_seeds.emplace_back(ep.substr(0, colon), static_cast<uint16_t>(sp));
                    }
                } catch (...) {}
            }
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
        } else if (args[i] == "--max-storage-mb" && i + 1 < args.size()) {
            try {
                cli_max_storage_bytes = static_cast<uint64_t>(std::stoull(args[i + 1])) * 1024ULL * 1024ULL;
            } catch (...) {}
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
        } else if (args[i] == "--ui-port" && i + 1 < args.size()) {
            try {
                const int up = std::stoi(args[i + 1]);
                if (up >= 1 && up <= 65535) {
                    cli_ui_port = static_cast<uint16_t>(up);
                    cli_ui_port_explicit = true;
                }
            } catch (...) {}
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
        } else if (args[i] == "--no-ui") {
            ui_enabled = false;
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i));
        } else if (args[i] == "--ui-lan") {
            ui_lan_access = true;
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i));
        } else if (args[i] == "--ui-token" && i + 1 < args.size()) {
            ui_auth_token = args[i + 1];
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
        } else {
            ++i;
        }
    }

    std::string id;
    std::string address;
    int port_i = 0;
    std::string coord_address;
    uint16_t coord_port = 0;

    if (!config_file_path.empty()) {
        std::ifstream cfg(config_file_path);
        if (!cfg) {
            std::cerr << "cannot open peer config file: " << config_file_path << "\n";
            return 1;
        }
        try {
            std::stringstream ss;
            ss << cfg.rdbuf();
            auto j = json::Json::parse(ss.str());
            if (j.contains("peer_id")) id = j["peer_id"].as_string();
            else if (j.contains("id")) id = j["id"].as_string();
            if (j.contains("address")) address = j["address"].as_string();
            if (j.contains("port")) port_i = static_cast<int>(j["port"].as_int());
            coord_address = j.contains("mode") ? j["mode"].as_string() : "p2p";
            if (j.contains("discovery_port")) {
                coord_port = static_cast<uint16_t>(j["discovery_port"].as_int());
            }
            if (custom_storage_root.empty() && j.contains("storage_dir") && !j["storage_dir"].as_string().empty()) {
                custom_storage_root = j["storage_dir"].as_string();
            }
            if (j.contains("max_storage_bytes")) {
                cli_max_storage_bytes = static_cast<uint64_t>(j["max_storage_bytes"].as_int());
            } else if (j.contains("max_storage_mb")) {
                cli_max_storage_bytes = static_cast<uint64_t>(j["max_storage_mb"].as_int()) * 1024ULL * 1024ULL;
            }
            if (!cli_ui_port_explicit && j.contains("ui_port")) {
                const int up = static_cast<int>(j["ui_port"].as_int());
                if (up >= 1 && up <= 65535) {
                    cli_ui_port = static_cast<uint16_t>(up);
                }
            }
            if (j.contains("ui_enabled") && !j["ui_enabled"].as_bool()) {
                ui_enabled = false;
            }
            if (j.contains("ui_lan") && j["ui_lan"].as_bool()) {
                ui_lan_access = true;
            }
            if (ui_auth_token.empty() && j.contains("ui_token")) {
                ui_auth_token = j["ui_token"].as_string();
            }
            if (j.contains("seed_peers") && j["seed_peers"].is_array()) {
                for (const auto& sp : j["seed_peers"].items()) {
                    if (sp.is_object() && sp.contains("address") && sp.contains("port")) {
                        cli_seeds.emplace_back(sp["address"].as_string(),
                                               static_cast<uint16_t>(sp["port"].as_int()));
                    } else if (sp.is_string()) {
                        const std::string ep = sp.as_string();
                        const size_t colon = ep.rfind(':');
                        if (colon != std::string::npos) {
                            cli_seeds.emplace_back(ep.substr(0, colon),
                                                   static_cast<uint16_t>(std::stoi(ep.substr(colon + 1))));
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "invalid JSON in peer config file " << config_file_path << ": " << e.what() << "\n";
            return 1;
        }
    }

    if (args.size() >= 4) {
        id = args[1];
        address = args[2];
        try {
            port_i = std::stoi(args[3]);
        } catch (...) {
            port_i = 0;
        }
    }

    if (id.empty() || !safe_peer_id(id) || address.empty() || port_i < 1 || port_i > 65535) {
        std::cerr << "usage: peer <ID> <address> <port> [p2p|coordinator_address] [discovery_port|coordinator_port]\n"
                  << "   or: peer --config <peer_config.json>\n";
        return 1;
    }
    const auto port = static_cast<uint16_t>(port_i);

    if (args.size() >= 5) {
        coord_address = args[4];
        if (args.size() >= 6) {
            try {
                coord_port = static_cast<uint16_t>(std::stoi(args[5]));
            } catch (...) {
                coord_port = 0;
            }
        }
    } else if (coord_address.empty()) {
        std::ifstream cfg("config.json");
        if (cfg) {
            try {
                std::stringstream ss;
                ss << cfg.rdbuf();
                auto j = json::Json::parse(ss.str());
                if (j.contains("coordinator")) {
                    coord_address = j["coordinator"]["address"].as_string();
                    coord_port = static_cast<uint16_t>(j["coordinator"]["port"].as_int());
                }
                if (j.contains("peers") && j["peers"].is_array()) {
                    for (const auto& pe : j["peers"].items()) {
                        if (pe.is_object() && pe.contains("address") && pe.contains("port")) {
                            cli_seeds.emplace_back(pe["address"].as_string(),
                                                   static_cast<uint16_t>(pe["port"].as_int()));
                        }
                    }
                }
            } catch (...) {}
        }
    }

    if (cli_max_storage_bytes > 0) {
        g_max_storage_bytes.store(cli_max_storage_bytes);
    }

    // Derive default UI port if not specified (8080 for 5001/5701, 8081 for 5002/5702, etc.)
    uint16_t ui_port = cli_ui_port;
    if (ui_port == 0) {
        if (port >= 5001 && port <= 5020) {
            ui_port = static_cast<uint16_t>(8080 + (port - 5001));
        } else if (port >= 5701 && port <= 5720) {
            ui_port = static_cast<uint16_t>(8080 + (port - 5701));
        } else {
            ui_port = static_cast<uint16_t>(8080 + (port % 100));
        }
    }
    const std::string ui_bind_address = ui_lan_access ? "0.0.0.0" : "127.0.0.1";

    const bool is_p2p_mode =
        (coord_address == "p2p" || coord_address == "P2P" ||
         coord_address == "none" || coord_address == "decentralized" ||
         coord_address == "--p2p");
    const uint16_t discovery_port = (is_p2p_mode && coord_port > 0) ? coord_port : 5005;

    if (!sock_init()) {
        std::cerr << "[peer " << id << "] socket subsystem initialisation failed\n";
        return 1;
    }

    // Isolated per-peer storage and embedded SQLite database (Task 1 & Task 2).
    const std::string peer_root_dir = !custom_storage_root.empty()
                                          ? custom_storage_root
                                          : ("storage/peer_" + id);
    const std::string storage_dir = peer_root_dir + "/chunks";
    const std::string staging_dir = peer_root_dir + "/staging";
    const std::string db_path = peer_root_dir + "/metadata.db";
    fs::create_directories(storage_dir);
    fs::create_directories(staging_dir);

    // Clean up any leftover temporary (.tmp) or interrupted upload staging files from prior runs
    {
        std::error_code clean_ec;
        fs::remove_all(fs::u8path(staging_dir), clean_ec);
        fs::create_directories(staging_dir, clean_ec);
        for (const auto& entry : fs::directory_iterator(fs::u8path(storage_dir), clean_ec)) {
            if (entry.is_regular_file(clean_ec)) {
                const std::string fn = entry.path().filename().u8string();
                if (fn.size() >= 4 && fn.substr(fn.size() - 4) == ".tmp") {
                    fs::remove(entry.path(), clean_ec);
                }
            }
        }
    }

    DatabaseManager db;
    std::string db_err;
    if (!db.open(db_path, &db_err)) {
        std::cerr << "[peer " << id << "] failed to open SQLite database at "
                  << db_path << ": " << db_err << "\n";
        return 1;
    }

    // Persist local peer configuration in SQLite.
    db.set_config("peer_id", id);
    db.set_config("address", address);
    db.set_config("port", std::to_string(port));
    db.set_config("storage_dir", storage_dir);
    db.set_config("db_path", db_path);
    db.set_config("discovery_port", std::to_string(discovery_port));
    db.set_config("network_mode", is_p2p_mode ? "DECENTRALIZED_P2P" : "HYBRID");
    db.set_config("ui_enabled", ui_enabled ? "1" : "0");
    db.set_config("ui_port", std::to_string(ui_port));
    if (cli_max_storage_bytes > 0) {
        db.set_config("max_storage_bytes", std::to_string(cli_max_storage_bytes));
    }

    g_telemetry.add_event("STARTUP", "INFO",
                          "Peer " + id + " started at " + address + ":" + std::to_string(port) +
                              " (UDP discovery port " + std::to_string(discovery_port) + ")");

    PeerRegistry local_registry;
    DecentralizedPeerManager p2p_manager(
        id, address, port, discovery_port, storage_dir, local_registry, db);
    for (const auto& seed : cli_seeds) {
        p2p_manager.add_seed_peer(seed.first, seed.second);
    }

    TCPServer* server_ptr = nullptr;
    HttpServer* http_server_ptr = nullptr;
    uint16_t bound_ui_port = ui_port;

    TCPServer::Handler handler =
        [&id, &address, port, &storage_dir, &staging_dir, &db_path, &local_registry, &db, &p2p_manager,
         &ui_enabled, &bound_ui_port, &server_ptr, &http_server_ptr](
            const json::Json& req,
            const std::vector<uint8_t>& req_payload,
            json::Json& resp,
            std::vector<uint8_t>& resp_payload) -> bool {
        const std::string type = req["type"].as_string();

        if (type == proto::REGISTER) {
            resp["type"] = proto::REGISTER_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            return true;
        }

        if (type == proto::HEARTBEAT) {
            resp["type"] = proto::HEARTBEAT_ACK;
            resp["peer_id"] = id;
            resp["status"] = "ALIVE";
            return true;
        }

        // ---- Phase-2 Task-1: Direct P2P Handshake --------------------------------
        if (type == proto::P2P_HANDSHAKE) {
            PeerInfo remote;
            remote.id = req.contains("peer_id") ? req["peer_id"].as_string() : "";
            remote.address = req.contains("address") ? req["address"].as_string() : "127.0.0.1";
            remote.port = req.contains("port") ? static_cast<uint16_t>(req["port"].as_int()) : 0;
            remote.state = PeerHealth::ONLINE;
            if (req.contains("available_storage_bytes")) {
                remote.workload.available_storage_bytes =
                    static_cast<uint64_t>(req["available_storage_bytes"].as_int());
            }
            if (req.contains("active_transfers")) {
                remote.workload.active_transfers =
                    static_cast<uint32_t>(req["active_transfers"].as_int());
            }
            if (req.contains("transfer_capacity")) {
                remote.workload.transfer_capacity =
                    static_cast<uint32_t>(req["transfer_capacity"].as_int());
            }

            if (safe_peer_id(remote.id) && remote.id != id && remote.port > 0) {
                std::string msg;
                local_registry.register_peer(remote, msg);
                db.upsert_peer(remote, DatabaseManager::now_epoch_ms(), "TCP_HANDSHAKE", false);
            }

            const PeerWorkload w = current_workload(storage_dir);
            resp["type"] = proto::P2P_HANDSHAKE_ACK;
            resp["peer_id"] = id;
            resp["address"] = address;
            resp["port"] = static_cast<long long>(port);
            resp["status"] = "OK";
            resp["available_storage_bytes"] = static_cast<long long>(w.available_storage_bytes);
            resp["active_transfers"] = static_cast<long long>(w.active_transfers);
            resp["transfer_capacity"] = static_cast<long long>(w.transfer_capacity);
            resp["peers"] = local_registry.to_json();
            return true;
        }

        // ---- Phase-2 Task-1: Direct P2P Heartbeat --------------------------------
        if (type == proto::P2P_HEARTBEAT) {
            const std::string remote_id = req.contains("peer_id") ? req["peer_id"].as_string() : "";
            const std::string remote_addr = req.contains("address") ? req["address"].as_string() : "127.0.0.1";
            const uint16_t remote_port = req.contains("port") ? static_cast<uint16_t>(req["port"].as_int()) : 0;

            PeerWorkload rw;
            if (req.contains("available_storage_bytes")) {
                rw.available_storage_bytes =
                    static_cast<uint64_t>(req["available_storage_bytes"].as_int());
            }
            if (req.contains("active_transfers")) {
                rw.active_transfers =
                    static_cast<uint32_t>(req["active_transfers"].as_int());
            }
            if (req.contains("transfer_capacity")) {
                rw.transfer_capacity =
                    static_cast<uint32_t>(req["transfer_capacity"].as_int());
            }

            if (safe_peer_id(remote_id) && remote_id != id && remote_port > 0) {
                if (!local_registry.record_heartbeat(remote_id, rw)) {
                    PeerInfo p;
                    p.id = remote_id;
                    p.address = remote_addr;
                    p.port = remote_port;
                    p.state = PeerHealth::ONLINE;
                    p.workload = rw;
                    std::string msg;
                    local_registry.register_peer(p, msg);
                }
                PeerInfo updated;
                updated.id = remote_id;
                updated.address = remote_addr;
                updated.port = remote_port;
                updated.state = PeerHealth::ONLINE;
                updated.workload = rw;
                db.upsert_peer(updated, DatabaseManager::now_epoch_ms(), "P2P_HEARTBEAT", false);
            }

            const PeerWorkload self_w = current_workload(storage_dir);
            resp["type"] = proto::P2P_HEARTBEAT_ACK;
            resp["peer_id"] = id;
            resp["address"] = address;
            resp["port"] = static_cast<long long>(port);
            resp["status"] = "ONLINE";
            resp["available_storage_bytes"] = static_cast<long long>(self_w.available_storage_bytes);
            resp["active_transfers"] = static_cast<long long>(self_w.active_transfers);
            resp["transfer_capacity"] = static_cast<long long>(self_w.transfer_capacity);
            return true;
        }

        // ---- Phase-2 Task-1: Query Local Peer Membership & SQLite Table ----------
        if (type == proto::P2P_QUERY_PEERS || type == proto::QUERY_PEERS) {
            const LocalStorageStats st = compute_local_storage_stats(storage_dir, db_path);
            const auto manifests = db.list_file_manifests();
            const auto rec_records = db.get_recovery_records();
            int completed_rec = 0;
            for (const auto& r : rec_records) {
                if (r.status == "COMPLETED") ++completed_rec;
            }
            resp["type"] = (type == proto::P2P_QUERY_PEERS)
                               ? proto::P2P_QUERY_PEERS_ACK
                               : proto::QUERY_PEERS_ACK;
            resp["peer_id"] = id;
            resp["peers"] = local_registry.to_json();
            resp["db_peers"] = db.get_all_peers_json(true);
            resp["config"] = db.get_all_config_json();
            resp["table"] = local_registry.format_table();
            resp["ui_enabled"] = ui_enabled;
            resp["ui_port"] = static_cast<long long>(bound_ui_port);
            resp["ui_url"] = ui_enabled ? ("http://127.0.0.1:" + std::to_string(bound_ui_port)) : "DISABLED";
            resp["used_storage_bytes"] = static_cast<long long>(st.used_bytes);
            resp["available_storage_bytes"] = static_cast<long long>(st.available_bytes);
            resp["local_shards_count"] = static_cast<long long>(st.local_shards_count);
            resp["catalogue_files_count"] = static_cast<long long>(manifests.size());
            resp["active_transfers"] = static_cast<long long>(g_active_transfers.load());
            resp["completed_recoveries"] = static_cast<long long>(completed_rec);
            return true;
        }

        // ---- Storage Inspection via CLI / P2P ------------------------------------
        if (type == proto::P2P_STORAGE_INFO) {
            const LocalStorageStats st = compute_local_storage_stats(storage_dir, db_path);
            const auto manifests = db.list_file_manifests();
            resp["type"] = proto::P2P_STORAGE_INFO_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["storage_dir"] = storage_dir;
            resp["db_path"] = db_path;
            resp["used_bytes"] = static_cast<long long>(st.used_bytes);
            resp["available_bytes"] = static_cast<long long>(st.available_bytes);
            resp["max_storage_bytes"] = static_cast<long long>(st.max_storage_bytes);
            resp["db_size_bytes"] = static_cast<long long>(st.db_size_bytes);
            resp["local_shards_count"] = static_cast<long long>(st.local_shards_count);
            resp["catalogue_files_count"] = static_cast<long long>(manifests.size());
            resp["local_shards"] = st.local_shards;
            return true;
        }

        // ---- Active & Recent Transfers Query via CLI / P2P -----------------------
        if (type == proto::P2P_TRANSFERS_QUERY) {
            uint32_t act_up = 0, act_dl = 0;
            g_telemetry.count_active(act_up, act_dl);
            resp["type"] = proto::P2P_TRANSFERS_QUERY_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["active_transfers"] = static_cast<long long>(g_active_transfers.load());
            resp["active_uploads"] = static_cast<long long>(act_up);
            resp["active_downloads"] = static_cast<long long>(act_dl);
            resp["transfers"] = g_telemetry.get_transfers_json();
            resp["peers"] = local_registry.to_json();
            return true;
        }

        // ---- Recovery Status & Records Query via CLI / P2P -----------------------
        if (type == proto::P2P_RECOVERY_STATUS) {
            if (req.contains("trigger_scan") && req["trigger_scan"].as_bool()) {
                p2p_manager.run_autonomous_recovery_scan(999, true, 0, false);
            }
            json::Json recs_arr = json::Json::array();
            int started_cnt = 0, completed_cnt = 0, failed_cnt = 0;
            for (const auto& r : db.get_recovery_records()) {
                recs_arr.push_back(r.to_json());
                if (r.status == "STARTED") ++started_cnt;
                else if (r.status == "COMPLETED") ++completed_cnt;
                else if (r.status == "FAILED" || r.status == "ABORTED") ++failed_cnt;
            }
            resp["type"] = proto::P2P_RECOVERY_STATUS_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["started_count"] = static_cast<long long>(started_cnt);
            resp["completed_count"] = static_cast<long long>(completed_cnt);
            resp["failed_count"] = static_cast<long long>(failed_cnt);
            resp["total_records"] = static_cast<long long>(recs_arr.size());
            resp["recovery_records"] = recs_arr;
            resp["records"] = recs_arr;
            return true;
        }

        // ---- Graceful Shutdown Command via CLI / P2P -----------------------------
        if (type == proto::P2P_SHUTDOWN) {
            resp["type"] = proto::P2P_SHUTDOWN_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["message"] = "Peer " + id + " initiating graceful shutdown";
            g_telemetry.add_event("SHUTDOWN", "WARN", "Graceful shutdown requested via CLI");
            std::thread([&p2p_manager, &server_ptr, &http_server_ptr]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(120));
                p2p_manager.stop();
                if (http_server_ptr) http_server_ptr->stop();
                if (server_ptr) server_ptr->stop();
            }).detach();
            return true;
        }

        // ---- Phase-2 Task-2: P2P Metadata Push -----------------------------------
        if (type == proto::P2P_METADATA_PUSH) {
            const std::string origin = req.contains("origin_peer_id")
                                           ? req["origin_peer_id"].as_string()
                                           : id;
            const bool propagate = req.contains("propagate") && req["propagate"].as_bool();

            int applied = 0;
            std::string last_file_id;
            int32_t last_version = 0;

            if (req.contains("manifest") && req["manifest"].is_object()) {
                FileManifest m = FileManifest::from_json(req["manifest"]);
                if (!m.file_id.empty() && db.upsert_file_manifest(m, origin)) {
                    ++applied;
                    last_file_id = m.file_id;
                    last_version = m.version;
                    reconcile_local_stale_shards(id, storage_dir, m);
                    if (propagate) {
                        broadcast_manifest_to_peers(id, local_registry, m);
                    }
                }
            }
            if (req.contains("manifests") && req["manifests"].is_array()) {
                for (const auto& item : req["manifests"].items()) {
                    FileManifest m = FileManifest::from_json(item);
                    if (!m.file_id.empty() && db.upsert_file_manifest(m, origin)) {
                        ++applied;
                        last_file_id = m.file_id;
                        last_version = m.version;
                        reconcile_local_stale_shards(id, storage_dir, m);
                        if (propagate) {
                            broadcast_manifest_to_peers(id, local_registry, m);
                        }
                    }
                }
            }
            if (req.contains("recovery_records") && req["recovery_records"].is_array()) {
                for (const auto& item : req["recovery_records"].items()) {
                    RecoveryRecordEntry r = RecoveryRecordEntry::from_json(item);
                    if (!r.recovery_id.empty()) {
                        db.upsert_recovery_record(r);
                        ++applied;
                    }
                }
            }

            resp["type"] = proto::P2P_METADATA_PUSH_ACK;
            resp["peer_id"] = id;
            resp["status"] = applied > 0 ? "OK" : "ERROR";
            resp["applied_count"] = static_cast<long long>(applied);
            resp["file_id"] = last_file_id;
            resp["version"] = static_cast<long long>(last_version);
            return true;
        }

        // ---- Phase-2 Task-2: P2P Metadata Summary Exchange -----------------------
        if (type == proto::P2P_METADATA_SUMMARY) {
            resp["type"] = proto::P2P_METADATA_SUMMARY_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["summary"] = db.get_metadata_summary_json();
            return true;
        }

        // ---- Phase-2 Task-2: P2P Metadata Request / Response ---------------------
        if (type == proto::P2P_METADATA_REQUEST) {
            json::Json manifests_arr = json::Json::array();
            json::Json recs_arr = json::Json::array();
            if (req.contains("file_ids") && req["file_ids"].is_array()) {
                for (const auto& item : req["file_ids"].items()) {
                    const std::string fid = item.as_string();
                    FileManifest m;
                    if (db.get_file_manifest(fid, m)) {
                        manifests_arr.push_back(m.to_json());
                    }
                    for (const auto& r : db.get_recovery_records(fid)) {
                        recs_arr.push_back(r.to_json());
                    }
                }
            } else {
                for (const auto& m : db.list_file_manifests()) {
                    manifests_arr.push_back(m.to_json());
                }
                for (const auto& r : db.get_recovery_records()) {
                    recs_arr.push_back(r.to_json());
                }
            }
            resp["type"] = proto::P2P_METADATA_RESPONSE;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["manifests"] = manifests_arr;
            resp["recovery_records"] = recs_arr;
            return true;
        }

        // ---- Phase-2 Task-2 & Task-4: Distributed File Catalogue Query -----------
        if (type == proto::P2P_CATALOGUE_QUERY) {
            const std::string q = req.contains("query") ? req["query"].as_string() : "";
            const std::string exact_fid = req.contains("file_id") ? req["file_id"].as_string() : "";
            const bool force_sync = req.contains("sync_if_missing") && req["sync_if_missing"].as_bool();

            resp["type"] = proto::P2P_CATALOGUE_QUERY_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";

            if (!exact_fid.empty()) {
                FileManifest m;
                if (!db.get_file_manifest(exact_fid, m) || force_sync) {
                    pull_missing_metadata_from_peers(id, local_registry, db);
                }
                json::Json arr = json::Json::array();
                if (db.get_file_manifest(exact_fid, m)) {
                    arr.push_back(db.evaluate_catalogue_entry(m).to_json());
                }
                resp["files"] = arr;
            } else {
                auto entries = db.query_catalogue_json(q);
                if (entries.size() == 0 || force_sync) {
                    pull_missing_metadata_from_peers(id, local_registry, db);
                    entries = db.query_catalogue_json(q);
                }
                resp["files"] = entries;
            }
            return true;
        }

        // ---- Phase-2 Task-4: Decentralized File Download & RS(3,2) Reconstruction -
        if (type == proto::P2P_DOWNLOAD_FILE) {
            TransferGuard guard(g_active_transfers);
            const auto t_start = std::chrono::steady_clock::now();
            const std::string target_q = req.contains("file_id") ? req["file_id"].as_string() : "";
            const std::string out_path = req.contains("output_path") ? req["output_path"].as_string() : "";

            resp["type"] = proto::P2P_DOWNLOAD_FILE_ACK;
            resp["peer_id"] = id;

            if (target_q.empty()) {
                resp["status"] = "FAILED";
                resp["reason"] = "missing file_id or filename in P2P_DOWNLOAD_FILE";
                return true;
            }

            // Ensure local catalogue is synchronized with online peers if file is missing/stale
            FileManifest chosen;
            if (!db.get_file_manifest(target_q, chosen)) {
                pull_missing_metadata_from_peers(id, local_registry, db);
            }

            if (!db.get_file_manifest(target_q, chosen)) {
                const auto matches = db.search_file_manifests(target_q);
                if (matches.empty()) {
                    resp["status"] = "FAILED";
                    resp["reason"] = "file '" + target_q + "' not found in distributed catalogue";
                    return true;
                }
                std::vector<FileManifest> exact_names;
                for (const auto& m : matches) {
                    if (m.original_name == target_q) exact_names.push_back(m);
                }
                if (exact_names.size() == 1) {
                    chosen = exact_names[0];
                } else if (exact_names.size() > 1 || matches.size() > 1) {
                    std::ostringstream amb;
                    amb << "ambiguous filename '" << target_q << "' matches multiple File IDs: ";
                    const auto& list = !exact_names.empty() ? exact_names : matches;
                    for (size_t i = 0; i < list.size(); ++i) {
                        if (i > 0) amb << ", ";
                        amb << list[i].file_id;
                    }
                    resp["status"] = "FAILED";
                    resp["reason"] = amb.str();
                    return true;
                } else {
                    chosen = matches[0];
                }
            }

            const std::string tx_id = g_telemetry.start_transfer(
                "DOWNLOAD", chosen.original_name, chosen.file_size, id, chosen.file_id);

            const std::string eff_out = !out_path.empty()
                                            ? out_path
                                            : ("output/" + safe_filename_only(chosen.original_name, chosen.file_id + ".bin"));

            const DownloadResult dl_res = ClientDownload::reconstruct_from_manifest_p2p(
                chosen, local_registry.get_all_peers(), eff_out, id, storage_dir);

            const auto elapsed_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - t_start)
                    .count());
            const uint64_t eff_ms = elapsed_ms > 0 ? elapsed_ms : 1;
            const uint64_t speed_bps = (dl_res.downloaded_bytes * 1000ULL) / eff_ms;

            g_telemetry.finish_transfer(tx_id, dl_res.success, dl_res.file_id,
                                        dl_res.downloaded_bytes, elapsed_ms,
                                        static_cast<int32_t>(dl_res.shards_retrieved),
                                        dl_res.message);
            g_telemetry.add_event("DOWNLOAD", dl_res.success ? "INFO" : "ERROR",
                                  (dl_res.success ? "Downloaded & verified " : "Download failed for ") +
                                      chosen.original_name + " (" + chosen.file_id + ")");

            resp["status"] = dl_res.success ? "OK" : "FAILED";
            resp["transfer_id"] = tx_id;
            resp["file_id"] = dl_res.file_id;
            resp["filename"] = dl_res.filename;
            resp["file_size"] = static_cast<long long>(dl_res.file_size);
            resp["downloaded_bytes"] = static_cast<long long>(dl_res.downloaded_bytes);
            resp["elapsed_ms"] = static_cast<long long>(elapsed_ms);
            resp["speed_bps"] = static_cast<long long>(speed_bps);
            resp["expected_sha256"] = dl_res.expected_sha256;
            resp["actual_sha256"] = dl_res.actual_sha256;
            resp["output_path"] = dl_res.output_path;
            resp["stripes_total"] = static_cast<long long>(dl_res.stripes_total);
            resp["stripes_reconstructed"] = static_cast<long long>(dl_res.stripes_reconstructed);
            resp["shards_retrieved"] = static_cast<long long>(dl_res.shards_retrieved);
            resp["failed_peer_connections"] = static_cast<long long>(dl_res.failed_peer_connections);
            resp["corrupted_shards_rejected"] = static_cast<long long>(dl_res.corrupted_shards_rejected);
            resp["retrieval_log"] = dl_res.retrieval_log;
            resp["reason"] = dl_res.message;
            return true;
        }

        // ---- Phase-2 Task-3: Decentralized File Upload & RS(3,2) Shard Placement -
        if (type == proto::P2P_UPLOAD_FILE) {
            TransferGuard guard(g_active_transfers);
            const auto t_start = std::chrono::steady_clock::now();
            const std::string filename = safe_filename_only(
                req.contains("filename") ? req["filename"].as_string() : "uploaded_file.bin",
                "uploaded_file.bin");
            std::string source_path;
            bool cleanup_source = false;

            const std::string upload_staging_root = staging_dir;
            std::error_code ec;
            fs::create_directories(upload_staging_root, ec);

            if (req.contains("file_path") && !req["file_path"].as_string().empty() &&
                req["file_path"].as_string().find("..") == std::string::npos &&
                req_payload.empty() && fs::is_regular_file(fs::u8path(req["file_path"].as_string()), ec)) {
                source_path = req["file_path"].as_string();
            } else {
                source_path = upload_staging_root + "/upload_in_" +
                              std::to_string(DatabaseManager::now_epoch_ms()) + ".bin";
                cleanup_source = true;
                std::ofstream out(fs::u8path(source_path), std::ios::binary | std::ios::trunc);
                if (!out) {
                    resp["type"] = proto::P2P_UPLOAD_FILE_ACK;
                    resp["peer_id"] = id;
                    resp["status"] = "FAILED";
                    resp["reason"] = "cannot write temporary upload file on peer " + id;
                    return true;
                }
                if (!req_payload.empty()) {
                    out.write(reinterpret_cast<const char*>(req_payload.data()),
                              static_cast<std::streamsize>(req_payload.size()));
                }
            }

            const uint64_t source_size = fs::is_regular_file(fs::u8path(source_path), ec)
                                             ? static_cast<uint64_t>(fs::file_size(fs::u8path(source_path), ec))
                                             : 0;
            const std::string tx_id = g_telemetry.start_transfer("UPLOAD", filename, source_size, id);

            if (req.contains("expected_sha256") && !req["expected_sha256"].as_string().empty()) {
                const std::string actual_sha = ChunkManager::sha256_of_file(source_path);
                if (actual_sha != req["expected_sha256"].as_string()) {
                    if (cleanup_source) fs::remove(fs::u8path(source_path), ec);
                    g_telemetry.finish_transfer(tx_id, false, "", 0, 1, 0,
                                                "uploaded file SHA-256 mismatch at entry peer " + id);
                    resp["type"] = proto::P2P_UPLOAD_FILE_ACK;
                    resp["peer_id"] = id;
                    resp["status"] = "FAILED";
                    resp["reason"] = "uploaded file SHA-256 mismatch at entry peer " + id;
                    return true;
                }
            }

            P2PUploadOptions opts;
            if (req.contains("file_id")) opts.custom_file_id = req["file_id"].as_string();
            if (req.contains("fail_first_store_on_peer"))
                opts.fail_first_store_on_peer = req["fail_first_store_on_peer"].as_string();
            if (req.contains("corrupt_first_hash_on_peer"))
                opts.corrupt_first_hash_on_peer = req["corrupt_first_hash_on_peer"].as_bool();
            if (req.contains("simulate_interrupted_transfer"))
                opts.simulate_interrupted_transfer = req["simulate_interrupted_transfer"].as_bool();
            if (req.contains("duplicate_store_count"))
                opts.duplicate_store_count = static_cast<int>(req["duplicate_store_count"].as_int());
            if (req.contains("override_min_free_bytes"))
                opts.override_min_free_bytes = static_cast<uint64_t>(req["override_min_free_bytes"].as_int());

            const std::string shards_staging_dir = upload_staging_root + "/shards_" +
                                                   std::to_string(DatabaseManager::now_epoch_ms());
            DynamicDistributor distributor(local_registry);
            const P2PUploadResult up_res = distributor.upload_and_distribute_p2p(
                id, source_path, filename, shards_staging_dir, db, opts);

            if (cleanup_source) {
                fs::remove(fs::u8path(source_path), ec);
            }
            fs::remove_all(fs::u8path(shards_staging_dir), ec);

            const auto elapsed_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - t_start)
                    .count());
            const uint64_t eff_ms = elapsed_ms > 0 ? elapsed_ms : 1;
            const uint64_t speed_bps = (up_res.manifest.file_size * 1000ULL) / eff_ms;

            g_telemetry.finish_transfer(
                tx_id, up_res.success, up_res.file_id, up_res.manifest.file_size, elapsed_ms,
                up_res.manifest.total_shards,
                up_res.success ? ("Distributed " + std::to_string(up_res.manifest.total_shards) + " shards")
                               : up_res.error_message);
            g_telemetry.add_event(
                "UPLOAD", up_res.success ? "INFO" : "ERROR",
                (up_res.success ? "Uploaded & distributed " : "Upload failed for ") + filename +
                    (up_res.file_id.empty() ? "" : (" (" + up_res.file_id + ")")));

            resp["type"] = proto::P2P_UPLOAD_FILE_ACK;
            resp["peer_id"] = id;
            resp["transfer_id"] = tx_id;
            resp["file_id"] = up_res.file_id;
            resp["status"] = up_res.success ? "OK" : "FAILED";
            resp["file_status"] = up_res.status;
            resp["reason"] = up_res.error_message;
            resp["elapsed_ms"] = static_cast<long long>(elapsed_ms);
            resp["speed_bps"] = static_cast<long long>(speed_bps);
            resp["placement_log"] = up_res.placement_log;
            resp["retries_performed"] = static_cast<long long>(up_res.retries_performed);
            resp["duplicate_acks_verified"] = static_cast<long long>(up_res.duplicate_acks_verified);
            resp["manifest"] = up_res.manifest.to_json();

            if (up_res.success) {
                // Synchronize the completed AVAILABLE manifest across all peers
                broadcast_manifest_to_peers(id, local_registry, up_res.manifest);
            }
            return true;
        }

        if (type == proto::STORE_CHUNK) {
            TransferGuard guard(g_active_transfers);
            const std::string chunk_id = req["chunk_id"].as_string();
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "bad chunk_id";
                return true;
            }
            if (req.contains("expected_size")) {
                const size_t exp_sz = static_cast<size_t>(req["expected_size"].as_int());
                if (req_payload.size() != exp_sz) {
                    resp["type"] = proto::TRANSFER_ERROR;
                    resp["reason"] = "shard size mismatch / interrupted transfer on store";
                    return true;
                }
            }

            const uint64_t avail_bytes = query_available_storage(storage_dir);
            const uint64_t req_free = req.contains("require_min_free_bytes")
                                          ? static_cast<uint64_t>(req["require_min_free_bytes"].as_int())
                                          : static_cast<uint64_t>(req_payload.size());
            if (avail_bytes > 0 && avail_bytes < req_free) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "insufficient storage on peer " + id;
                return true;
            }

            const std::string got_sha256 = ChunkManager::sha256_of_bytes(req_payload);
            if (req.contains("sha256")) {
                if (got_sha256 != req["sha256"].as_string()) {
                    resp["type"] = proto::TRANSFER_ERROR;
                    resp["reason"] = "sha256 mismatch on store";
                    return true;
                }
            }

            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            static std::atomic<uint64_t> s_tmp_seq{0};
            const std::string tmp = path + "." + std::to_string(++s_tmp_seq) + ".tmp";
            {
                std::ofstream out(fs::u8path(tmp), std::ios::binary | std::ios::trunc);
                if (!out) {
                    resp["type"] = proto::TRANSFER_ERROR;
                    resp["reason"] = "disk write error on peer " + id;
                    return true;
                }
                if (!req_payload.empty()) {
                    out.write(reinterpret_cast<const char*>(req_payload.data()),
                              static_cast<std::streamsize>(req_payload.size()));
                }
                out.flush();
            }

            std::error_code ec;
            fs::rename(fs::u8path(tmp), fs::u8path(path), ec);
            if (ec) {
                // On Windows, rename fails if target already exists (e.g. duplicate STORE_CHUNK).
                fs::remove(fs::u8path(path), ec);
                ec.clear();
                fs::rename(fs::u8path(tmp), fs::u8path(path), ec);
                if (ec) {
                    fs::remove(fs::u8path(tmp), ec);
                    resp["type"] = proto::TRANSFER_ERROR;
                    resp["reason"] = "atomic commit failed on peer " + id;
                    return true;
                }
            }

            // Record the verified shard in the destination peer's local SQLite database (Task 3.5)
            if (req.contains("file_id") && !req["file_id"].as_string().empty()) {
                const std::string fid = req["file_id"].as_string();
                const std::string orig_name = req.contains("original_name") ? req["original_name"].as_string() : fid;
                const uint64_t fsize = req.contains("file_size") ? static_cast<uint64_t>(req["file_size"].as_int()) : 0;
                const std::string fsha = req.contains("file_sha256") ? req["file_sha256"].as_string() : got_sha256;
                const int32_t tot_st = req.contains("total_stripes") ? static_cast<int32_t>(req["total_stripes"].as_int()) : 1;
                const int32_t tot_sh = req.contains("total_shards") ? static_cast<int32_t>(req["total_shards"].as_int()) : 3;
                const std::string up_peer = req.contains("uploader_peer_id") ? req["uploader_peer_id"].as_string() : id;
                const int32_t st_idx = req.contains("stripe_index") ? static_cast<int32_t>(req["stripe_index"].as_int()) : 0;
                const uint64_t st_bytes = req.contains("stripe_data_bytes") ? static_cast<uint64_t>(req["stripe_data_bytes"].as_int()) : 0;
                const int32_t sh_idx = req.contains("shard_index") ? static_cast<int32_t>(req["shard_index"].as_int()) : 0;
                const std::string sh_type = req.contains("shard_type") ? req["shard_type"].as_string() : "DATA";
                const uint64_t orig_b = req.contains("original_bytes") ? static_cast<uint64_t>(req["original_bytes"].as_int()) : req_payload.size();

                db.record_received_shard(id, fid, orig_name, fsize, fsha, tot_st, tot_sh,
                                         up_peer, st_idx, st_bytes, chunk_id, sh_idx,
                                         sh_type, req_payload.size(), orig_b, got_sha256);
            }

            resp["type"] = proto::STORE_ACK;
            resp["chunk_id"] = chunk_id;
            resp["peer_id"] = id;
            resp["sha256"] = got_sha256;
            resp["status"] = "OK";
            resp["size"] = static_cast<long long>(req_payload.size());
            return true;
        }

        if (type == proto::GET_CHUNK) {
            TransferGuard guard(g_active_transfers);
            const std::string chunk_id = req["chunk_id"].as_string();
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "bad chunk_id";
                return true;
            }
            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            if (!fs::exists(path)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "chunk not found on peer " + id;
                return true;
            }
            std::vector<uint8_t> data = read_binary(path);
            resp["type"] = proto::CHUNK_RESPONSE;
            resp["chunk_id"] = chunk_id;
            resp["peer_id"] = id;

            if (req.contains("simulate") &&
                req["simulate"].as_string() == "interrupt") {
                resp["truncate_payload_to"] =
                    static_cast<long long>(data.size() / 2);
            }
            resp_payload = std::move(data);
            return true;
        }

        if (type == "REPAIR_SHARD") {
            TransferGuard guard(g_active_transfers);
            const std::string target_shard_id = req["target_shard_id"].as_string();
            const size_t shard_size = static_cast<size_t>(req["target_shard_size"].as_int());
            const std::string expected_sha256 = req["expected_sha256"].as_string();

            std::vector<std::vector<uint8_t>> bufs(3, std::vector<uint8_t>(shard_size, 0));
            std::vector<bool> present(3, false);

            if (req.contains("sources") && req["sources"].is_array()) {
                for (const auto& src : req["sources"].items()) {
                    const std::string src_addr = src["address"].as_string();
                    const uint16_t s_port = static_cast<uint16_t>(src["port"].as_int());
                    const std::string s_cid = src["shard_id"].as_string();
                    const std::string s_sha = src["sha256"].as_string();

                    size_t slot = 0;
                    if (s_cid.find("-D0") != std::string::npos) slot = 0;
                    else if (s_cid.find("-D1") != std::string::npos) slot = 1;
                    else if (s_cid.find("-P0") != std::string::npos) slot = 2;

                    TCPClient c(3000, 3000);
                    if (c.connect_to(src_addr, s_port)) {
                        json::Json get_req = json::Json::object();
                        get_req["type"] = proto::GET_CHUNK;
                        get_req["chunk_id"] = s_cid;
                        json::Json get_resp;
                        std::vector<uint8_t> pl;
                        if (c.request(get_req, {}, get_resp, pl) == proto::RecvStatus::OK &&
                            get_resp["type"].as_string() == proto::CHUNK_RESPONSE &&
                            pl.size() == shard_size &&
                            ChunkManager::sha256_of_bytes(pl) == s_sha) {
                            bufs[slot] = std::move(pl);
                            present[slot] = true;
                        }
                    }
                }
            }

            size_t count_avail = 0;
            for (bool b : present) if (b) ++count_avail;

            if (count_avail < 2) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "insufficient surviving peer sources reached";
                return true;
            }

            ReedSolomon rs(2, 1);
            std::vector<uint8_t*> ptrs = {bufs[0].data(), bufs[1].data(), bufs[2].data()};
            if (!rs.decode(ptrs, present, shard_size)) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "RS decoding failed";
                return true;
            }

            size_t target_slot = 0;
            if (target_shard_id.find("-D0") != std::string::npos) target_slot = 0;
            else if (target_shard_id.find("-D1") != std::string::npos) target_slot = 1;
            else if (target_shard_id.find("-P0") != std::string::npos) target_slot = 2;

            const std::string actual_sha = ChunkManager::sha256_of_bytes(bufs[target_slot]);
            if (!expected_sha256.empty() && actual_sha != expected_sha256) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "reconstructed shard SHA-256 mismatch";
                return true;
            }

            const std::string path = storage_dir + "/" + target_shard_id + ".chunk";
            const std::string tmp = path + ".tmp";
            {
                std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(bufs[target_slot].data()), static_cast<std::streamsize>(shard_size));
            }
            fs::rename(tmp, path);

            resp["type"] = "REPAIR_SHARD_ACK";
            resp["status"] = "OK";
            resp["target_shard_id"] = target_shard_id;
            resp["peer_id"] = id;
            return true;
        }

        // ---- Phase-2 Task-5: Distributed Recovery Lease Request / Response -------
        if (type == proto::P2P_RECOVERY_LEASE_REQ) {
            const std::string rec_id = req.contains("recovery_id") ? req["recovery_id"].as_string() : "";
            const std::string failed_pid = req.contains("failed_peer_id") ? req["failed_peer_id"].as_string() : "UNKNOWN";
            const std::string sh_id = req.contains("shard_id") ? req["shard_id"].as_string() : "";
            const std::string f_id = req.contains("file_id") ? req["file_id"].as_string() : "";
            const int32_t st_idx = req.contains("stripe_index") ? static_cast<int32_t>(req["stripe_index"].as_int()) : 0;
            const std::string requester = req.contains("rebuilder_peer_id") ? req["rebuilder_peer_id"].as_string() : "";
            const int64_t now_ts = DatabaseManager::now_epoch_ms();
            const int64_t lease_ttl_ms = 2500;

            resp["type"] = proto::P2P_RECOVERY_LEASE_ACK;
            resp["peer_id"] = id;
            resp["recovery_id"] = rec_id;

            RecoveryRecordEntry existing;
            if (db.get_recovery_record(rec_id, existing)) {
                if (existing.status == "COMPLETED") {
                    resp["granted"] = false;
                    resp["reason"] = "already completed on " + existing.target_peer_id;
                    resp["record"] = existing.to_json();
                    return true;
                }
                if (existing.status == "STARTED" &&
                    existing.rebuilder_peer_id != requester &&
                    (now_ts - existing.timestamp_epoch_ms) < lease_ttl_ms) {
                    PeerInfo holder_info;
                    const bool holder_offline =
                        local_registry.get_peer(existing.rebuilder_peer_id, holder_info) &&
                        holder_info.state == PeerHealth::OFFLINE;
                    if (!holder_offline) {
                        resp["granted"] = false;
                        resp["reason"] = "active lease held by " + existing.rebuilder_peer_id;
                        resp["record"] = existing.to_json();
                        return true;
                    }
                }
            }

            RecoveryRecordEntry grant_rec;
            grant_rec.recovery_id = rec_id;
            grant_rec.failed_peer_id = failed_pid;
            grant_rec.shard_id = sh_id;
            grant_rec.file_id = f_id;
            grant_rec.stripe_index = st_idx;
            grant_rec.rebuilder_peer_id = requester;
            grant_rec.target_peer_id = "";
            grant_rec.status = "STARTED";
            grant_rec.reason = "lease granted to " + requester;
            grant_rec.timestamp_epoch_ms = now_ts;
            db.upsert_recovery_record(grant_rec);

            resp["granted"] = true;
            resp["record"] = grant_rec.to_json();
            return true;
        }

        // ---- Phase-2 Task-5: Explicit / Diagnostic Repair Scan Trigger -----------
        if (type == proto::P2P_REPAIR_TRIGGER) {
            const bool force_owner = !req.contains("force_owner") || req["force_owner"].as_bool();
            const uint64_t min_free = req.contains("override_min_free_bytes")
                                          ? static_cast<uint64_t>(req["override_min_free_bytes"].as_int())
                                          : 0;
            const bool sim_int = req.contains("simulate_interrupted_store") &&
                                 req["simulate_interrupted_store"].as_bool();

            const int repaired = p2p_manager.run_autonomous_recovery_scan(999, force_owner, min_free, sim_int);

            resp["type"] = proto::P2P_REPAIR_TRIGGER_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["repaired_count"] = static_cast<long long>(repaired);
            json::Json recs_arr = json::Json::array();
            for (const auto& r : db.get_recovery_records()) {
                recs_arr.push_back(r.to_json());
            }
            resp["recovery_records"] = recs_arr;
            return true;
        }

        // ---- Phase-3: Per-Peer Storage Telemetry ---------------------------------
        if (type == proto::P2P_STORAGE_INFO) {
            const LocalStorageStats st = compute_local_storage_stats(storage_dir, db_path);
            const auto manifests = db.list_file_manifests();
            resp["type"] = proto::P2P_STORAGE_INFO_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            resp["storage_dir"] = storage_dir;
            resp["db_path"] = db_path;
            resp["used_bytes"] = static_cast<long long>(st.used_bytes);
            resp["available_bytes"] = static_cast<long long>(st.available_bytes);
            resp["max_storage_bytes"] = static_cast<long long>(st.max_storage_bytes);
            resp["db_size_bytes"] = static_cast<long long>(st.db_size_bytes);
            resp["local_shards_count"] = static_cast<long long>(st.local_shards_count);
            resp["catalogue_files_count"] = static_cast<long long>(manifests.size());
            resp["local_shards"] = st.local_shards;
            return true;
        }

        resp["type"] = proto::TRANSFER_ERROR;
        resp["reason"] = "unknown message type: " + type;
        return true;
    };

    // ---- Interface B: Per-Peer Local HTTP Server & Web Dashboard ----------------
    HttpServer::Handler http_handler =
        [&id, &address, port, discovery_port, &storage_dir, &staging_dir, &db_path,
         &local_registry, &db, &p2p_manager, &ui_lan_access, &ui_auth_token, &bound_ui_port](
            const HttpRequest& req, HttpResponse& resp) {
        // Security checks for API routes
        if (req.path.rfind("/api/", 0) == 0) {
            // Optional token authentication when LAN access or token is configured
            if (!ui_auth_token.empty() && req.path != "/api/v1/health") {
                const std::string hdr_tok = req.get_header("x-rfe-token");
                const std::string q_tok = req.get_query("token");
                if (hdr_tok != ui_auth_token && q_tok != ui_auth_token) {
                    json::Json err = json::Json::object();
                    err["status"] = "ERROR";
                    err["error"] = "Unauthorized: valid X-RFE-Token required";
                    resp.set_json(401, err.dump());
                    return;
                }
            }
            // CSRF / Origin validation on state-changing POST requests
            if (req.method == "POST") {
                const std::string csrf_hdr = req.header("x-rfe-request");
                const std::string origin = req.header("origin");
                if (csrf_hdr != "1") {
                    json::Json err = json::Json::object();
                    err["status"] = "ERROR";
                    err["error"] = "Forbidden: missing X-RFE-Request header (CSRF protection)";
                    resp.set_json(403, err.dump());
                    return;
                }
                if (!origin.empty() && !ui_lan_access) {
                    if (origin.find("127.0.0.1") == std::string::npos &&
                        origin.find("localhost") == std::string::npos) {
                        json::Json err = json::Json::object();
                        err["status"] = "ERROR";
                        err["error"] = "Forbidden: cross-origin request rejected";
                        resp.set_json(403, err.dump());
                        return;
                    }
                }
            }
        }

        // Static Dashboard UI assets
        if (req.method == "GET" && (req.path == "/" || req.path == "/index.html")) {
            const std::string html = load_web_asset("index.html");
            if (html.empty()) {
                resp.status = 404;
                resp.content_type = "text/plain";
                resp.body = "Dashboard asset web/index.html not found";
                return;
            }
            resp.status = 200;
            resp.content_type = "text/html; charset=utf-8";
            resp.body = html;
            return;
        }
        if (req.method == "GET" && req.path == "/styles.css") {
            const std::string css = load_web_asset("styles.css");
            resp.status = css.empty() ? 404 : 200;
            resp.content_type = "text/css; charset=utf-8";
            resp.body = css;
            return;
        }
        if (req.method == "GET" && req.path == "/app.js") {
            const std::string js = load_web_asset("app.js");
            resp.status = js.empty() ? 404 : 200;
            resp.content_type = "application/javascript; charset=utf-8";
            resp.body = js;
            return;
        }
        if (req.method == "GET" && req.path == "/favicon.ico") {
            resp.status = 204;
            return;
        }

        // ---- GET /api/v1/health ------------------------------------------------
        if (req.method == "GET" && req.path == "/api/v1/health") {
            const auto uptime_s = std::chrono::duration_cast<std::chrono::seconds>(
                                      std::chrono::steady_clock::now() - g_process_start_steady)
                                      .count();
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["tcp_port"] = static_cast<long long>(port);
            j["ui_port"] = static_cast<long long>(bound_ui_port);
            j["uptime_seconds"] = static_cast<long long>(uptime_s);
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET /api/v1/node --------------------------------------------------
        if (req.method == "GET" && req.path == "/api/v1/node") {
            const auto all_peers = local_registry.get_all_peers();
            int online_cnt = 0, suspect_cnt = 0, offline_cnt = 0;
            for (const auto& p : all_peers) {
                if (p.state == PeerHealth::ONLINE) ++online_cnt;
                else if (p.state == PeerHealth::SUSPECT) ++suspect_cnt;
                else ++offline_cnt;
            }
            const LocalStorageStats st = compute_local_storage_stats(storage_dir, db_path);
            const auto files = db.query_catalogue_json("");
            const auto rec_records = db.get_recovery_records();
            int active_rec = 0, completed_rec = 0, failed_rec = 0;
            for (const auto& r : rec_records) {
                if (r.status == "STARTED") ++active_rec;
                else if (r.status == "COMPLETED") ++completed_rec;
                else if (r.status == "FAILED") ++failed_rec;
            }
            uint32_t act_up = 0, act_dl = 0;
            g_telemetry.count_active(act_up, act_dl);

            std::string net_health = "HEALTHY";
            if (offline_cnt > 0 || suspect_cnt > 0) net_health = "DEGRADED";
            if (online_cnt == 0 && !all_peers.empty()) net_health = "CRITICAL";

            const auto uptime_s = std::chrono::duration_cast<std::chrono::seconds>(
                                      std::chrono::steady_clock::now() - g_process_start_steady)
                                      .count();

            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["address"] = address;
            j["port"] = static_cast<long long>(port);
            j["discovery_port"] = static_cast<long long>(discovery_port);
            j["ui_port"] = static_cast<long long>(bound_ui_port);
            j["ui_lan_enabled"] = ui_lan_access;
            j["discovery_status"] = "ACTIVE (UDP + Seed + Peer Exchange)";
            j["network_health"] = net_health;
            j["online_peers"] = static_cast<long long>(online_cnt);
            j["suspect_peers"] = static_cast<long long>(suspect_cnt);
            j["offline_peers"] = static_cast<long long>(offline_cnt);
            j["total_discovered_peers"] = static_cast<long long>(all_peers.size());
            j["available_storage_bytes"] = static_cast<long long>(st.available_bytes);
            j["used_storage_bytes"] = static_cast<long long>(st.used_bytes);
            j["max_storage_bytes"] = static_cast<long long>(st.max_storage_bytes);
            j["local_shards_count"] = static_cast<long long>(st.local_shards_count);
            j["catalogue_files_count"] = static_cast<long long>(files.size());
            j["active_transfers"] = static_cast<long long>(g_active_transfers.load());
            j["active_uploads"] = static_cast<long long>(act_up);
            j["active_downloads"] = static_cast<long long>(act_dl);
            j["active_recovery_tasks"] = static_cast<long long>(active_rec);
            j["completed_recovery_tasks"] = static_cast<long long>(completed_rec);
            j["failed_recovery_tasks"] = static_cast<long long>(failed_rec);
            j["storage_dir"] = storage_dir;
            j["db_path"] = db_path;
            j["uptime_seconds"] = static_cast<long long>(uptime_s);
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET /api/v1/peers -------------------------------------------------
        if (req.method == "GET" && req.path == "/api/v1/peers") {
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["peers"] = local_registry.to_json();
            j["db_peers"] = db.get_all_peers_json(true);
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET /api/v1/files & GET /api/v1/search ----------------------------
        if (req.method == "GET" && (req.path == "/api/v1/files" || req.path == "/api/v1/search")) {
            const std::string q = req.get_query("q");
            const bool sync = (req.get_query("sync") == "1" || req.get_query("sync") == "true");
            auto files = db.query_catalogue_json(q);
            if (files.size() == 0 || sync) {
                pull_missing_metadata_from_peers(id, local_registry, db);
                files = db.query_catalogue_json(q);
            }
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["query"] = q;
            j["files"] = files;
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET /api/v1/files/{file_id} ---------------------------------------
        if (req.method == "GET" && req.path.rfind("/api/v1/files/", 0) == 0) {
            const std::string fid = req.path.substr(std::string("/api/v1/files/").size());
            if (!safe_chunk_id(fid)) {
                json::Json err = json::Json::object();
                err["status"] = "ERROR";
                err["error"] = "Invalid file_id";
                resp.set_json(400, err.dump());
                return;
            }
            FileManifest m;
            if (!db.get_file_manifest(fid, m)) {
                pull_missing_metadata_from_peers(id, local_registry, db);
            }
            if (!db.get_file_manifest(fid, m)) {
                json::Json err = json::Json::object();
                err["status"] = "ERROR";
                err["error"] = "File not found in distributed catalogue: " + fid;
                resp.set_json(404, err.dump());
                return;
            }
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["file"] = db.evaluate_catalogue_entry(m).to_json();
            resp.set_json(200, j.dump());
            return;
        }

        // ---- POST /api/v1/uploads ----------------------------------------------
        if (req.method == "POST" && req.path == "/api/v1/uploads") {
            TransferGuard guard(g_active_transfers);
            const auto t_start = std::chrono::steady_clock::now();

            std::string raw_name = !req.uploaded_filename.empty()
                                       ? req.uploaded_filename
                                       : req.get_header("x-file-name");
            if (raw_name.empty()) raw_name = req.get_query("filename");
            const std::string filename = safe_filename_only(raw_name, "uploaded_file.bin");

            std::error_code ec;
            if (req.temp_file_path.empty() || !fs::is_regular_file(fs::u8path(req.temp_file_path), ec)) {
                json::Json err = json::Json::object();
                err["status"] = "FAILED";
                err["error"] = "Empty or missing file upload payload";
                resp.set_json(400, err.dump());
                return;
            }
            const uint64_t fsize = static_cast<uint64_t>(fs::file_size(fs::u8path(req.temp_file_path), ec));
            if (fsize == 0) {
                fs::remove(fs::u8path(req.temp_file_path), ec);
                json::Json err = json::Json::object();
                err["status"] = "FAILED";
                err["error"] = "Cannot upload an empty (0-byte) file";
                resp.set_json(400, err.dump());
                return;
            }

            const std::string tx_id = g_telemetry.start_transfer("UPLOAD", filename, fsize, id);
            const std::string shards_staging_dir =
                staging_dir + "/web_shards_" + std::to_string(DatabaseManager::now_epoch_ms());

            DynamicDistributor distributor(local_registry);
            P2PUploadOptions opts;
            const P2PUploadResult up_res = distributor.upload_and_distribute_p2p(
                id, req.temp_file_path, filename, shards_staging_dir, db, opts);

            fs::remove(fs::u8path(req.temp_file_path), ec);
            fs::remove_all(fs::u8path(shards_staging_dir), ec);

            const auto elapsed_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - t_start)
                    .count());
            const uint64_t eff_ms = elapsed_ms > 0 ? elapsed_ms : 1;
            const uint64_t speed_bps = (fsize * 1000ULL) / eff_ms;

            g_telemetry.finish_transfer(
                tx_id, up_res.success, up_res.file_id, fsize, elapsed_ms,
                up_res.manifest.total_shards,
                up_res.success ? ("Distributed " + std::to_string(up_res.manifest.total_shards) + " shards")
                               : up_res.error_message);
            g_telemetry.add_event(
                "UPLOAD", up_res.success ? "INFO" : "ERROR",
                (up_res.success ? "Web upload completed: " : "Web upload failed: ") + filename +
                    (up_res.file_id.empty() ? "" : (" (" + up_res.file_id + ")")));

            if (up_res.success) {
                broadcast_manifest_to_peers(id, local_registry, up_res.manifest);
            }

            json::Json j = json::Json::object();
            j["status"] = up_res.success ? "OK" : "FAILED";
            j["peer_id"] = id;
            j["transfer_id"] = tx_id;
            j["file_id"] = up_res.file_id;
            j["filename"] = filename;
            j["file_size"] = static_cast<long long>(fsize);
            j["file_status"] = up_res.status;
            j["elapsed_ms"] = static_cast<long long>(elapsed_ms);
            j["speed_bps"] = static_cast<long long>(speed_bps);
            j["placement_log"] = up_res.placement_log;
            j["error"] = up_res.error_message;
            j["manifest"] = up_res.manifest.to_json();
            resp.set_json(up_res.success ? 200 : 422, j.dump());
            return;
        }

        // ---- GET /api/v1/downloads/{file_id} -----------------------------------
        if (req.method == "GET" && req.path.rfind("/api/v1/downloads/", 0) == 0) {
            TransferGuard guard(g_active_transfers);
            const auto t_start = std::chrono::steady_clock::now();
            const std::string fid = req.path.substr(std::string("/api/v1/downloads/").size());
            if (!safe_chunk_id(fid)) {
                json::Json err = json::Json::object();
                err["status"] = "FAILED";
                err["error"] = "Invalid file_id";
                resp.set_json(400, err.dump());
                return;
            }

            FileManifest chosen;
            if (!db.get_file_manifest(fid, chosen)) {
                pull_missing_metadata_from_peers(id, local_registry, db);
            }
            if (!db.get_file_manifest(fid, chosen)) {
                json::Json err = json::Json::object();
                err["status"] = "FAILED";
                err["error"] = "File not found in distributed catalogue: " + fid;
                resp.set_json(404, err.dump());
                return;
            }

            const std::string tx_id = g_telemetry.start_transfer(
                "DOWNLOAD", chosen.original_name, chosen.file_size, id, chosen.file_id);

            static std::atomic<uint64_t> s_dl_seq{0};
            const std::string temp_out =
                staging_dir + "/web_dl_" +
                std::to_string(DatabaseManager::now_epoch_ms()) + "_" +
                std::to_string(++s_dl_seq) + ".bin";

            const DownloadResult dl_res = ClientDownload::reconstruct_from_manifest_p2p(
                chosen, local_registry.get_all_peers(), temp_out, id, storage_dir);

            const auto elapsed_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - t_start)
                    .count());
            const uint64_t eff_ms = elapsed_ms > 0 ? elapsed_ms : 1;
            const uint64_t speed_bps = (dl_res.downloaded_bytes * 1000ULL) / eff_ms;

            g_telemetry.finish_transfer(tx_id, dl_res.success, dl_res.file_id,
                                        dl_res.downloaded_bytes, elapsed_ms,
                                        static_cast<int32_t>(dl_res.shards_retrieved),
                                        dl_res.message);
            g_telemetry.add_event("DOWNLOAD", dl_res.success ? "INFO" : "ERROR",
                                  (dl_res.success ? "Web download reconstructed: " : "Web download failed: ") +
                                      chosen.original_name + " (" + chosen.file_id + ")");

            if (!dl_res.success) {
                std::error_code ec;
                fs::remove(fs::u8path(temp_out), ec);
                json::Json err = json::Json::object();
                err["status"] = "FAILED";
                err["file_id"] = chosen.file_id;
                err["error"] = dl_res.message;
                resp.set_json(422, err.dump());
                return;
            }

            const std::string clean_name = safe_filename_only(chosen.original_name, chosen.file_id + ".bin");
            resp.status = 200;
            resp.content_type = "application/octet-stream";
            resp.headers["Content-Disposition"] = "attachment; filename=\"" + clean_name + "\"";
            resp.headers["X-RFE-File-Id"] = dl_res.file_id;
            resp.headers["X-RFE-SHA256"] = dl_res.actual_sha256;
            resp.headers["X-RFE-Shards-Retrieved"] = std::to_string(dl_res.shards_retrieved);
            resp.headers["X-RFE-Stripes-Reconstructed"] = std::to_string(dl_res.stripes_reconstructed);
            resp.headers["X-RFE-Elapsed-Ms"] = std::to_string(elapsed_ms);
            resp.headers["X-RFE-Speed-Bps"] = std::to_string(speed_bps);
            resp.stream_file_path = temp_out;
            resp.delete_stream_file_after = true;
            return;
        }

        // ---- GET /api/v1/transfers & /api/v1/transfers/{id} --------------------
        if (req.method == "GET" && req.path == "/api/v1/transfers") {
            uint32_t act_up = 0, act_dl = 0;
            g_telemetry.count_active(act_up, act_dl);
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["active_transfers"] = static_cast<long long>(g_active_transfers.load());
            j["active_uploads"] = static_cast<long long>(act_up);
            j["active_downloads"] = static_cast<long long>(act_dl);
            j["transfers"] = g_telemetry.get_transfers_json();
            j["peer_workloads"] = local_registry.to_json();
            resp.set_json(200, j.dump());
            return;
        }
        if (req.method == "GET" && req.path.rfind("/api/v1/transfers/", 0) == 0) {
            const std::string tid = req.path.substr(std::string("/api/v1/transfers/").size());
            bool found = false;
            const auto tx = g_telemetry.get_transfer_json(tid, found);
            if (!found) {
                json::Json err = json::Json::object();
                err["status"] = "ERROR";
                err["error"] = "Transfer not found: " + tid;
                resp.set_json(404, err.dump());
                return;
            }
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["transfer"] = tx;
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET & POST /api/v1/recovery ---------------------------------------
        if (req.method == "GET" && req.path == "/api/v1/recovery") {
            json::Json recs_arr = json::Json::array();
            int started_cnt = 0, completed_cnt = 0, failed_cnt = 0;
            for (const auto& r : db.get_recovery_records()) {
                recs_arr.push_back(r.to_json());
                if (r.status == "STARTED") ++started_cnt;
                else if (r.status == "COMPLETED") ++completed_cnt;
                else if (r.status == "FAILED") ++failed_cnt;
            }
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["started_count"] = static_cast<long long>(started_cnt);
            j["completed_count"] = static_cast<long long>(completed_cnt);
            j["failed_count"] = static_cast<long long>(failed_cnt);
            j["recovery_records"] = recs_arr;
            resp.set_json(200, j.dump());
            return;
        }
        if (req.method == "POST" && req.path == "/api/v1/recovery") {
            const int repaired = p2p_manager.run_autonomous_recovery_scan(999, true, 0, false);
            g_telemetry.add_event("RECOVERY", "INFO",
                                  "Manual recovery scan triggered from Web UI (repaired=" +
                                      std::to_string(repaired) + ")");
            json::Json recs_arr = json::Json::array();
            for (const auto& r : db.get_recovery_records()) {
                recs_arr.push_back(r.to_json());
            }
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["repaired_count"] = static_cast<long long>(repaired);
            j["recovery_records"] = recs_arr;
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET /api/v1/storage -----------------------------------------------
        if (req.method == "GET" && req.path == "/api/v1/storage") {
            const LocalStorageStats st = compute_local_storage_stats(storage_dir, db_path);
            const auto manifests = db.list_file_manifests();
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["storage_dir"] = storage_dir;
            j["db_path"] = db_path;
            j["used_bytes"] = static_cast<long long>(st.used_bytes);
            j["available_bytes"] = static_cast<long long>(st.available_bytes);
            j["max_storage_bytes"] = static_cast<long long>(st.max_storage_bytes);
            j["db_size_bytes"] = static_cast<long long>(st.db_size_bytes);
            j["local_shards_count"] = static_cast<long long>(st.local_shards_count);
            j["catalogue_files_count"] = static_cast<long long>(manifests.size());
            j["local_shards"] = st.local_shards;
            j["config"] = db.get_all_config_json();
            resp.set_json(200, j.dump());
            return;
        }

        // ---- GET /api/v1/events ------------------------------------------------
        if (req.method == "GET" && req.path == "/api/v1/events") {
            json::Json j = json::Json::object();
            j["status"] = "OK";
            j["peer_id"] = id;
            j["events"] = g_telemetry.get_events_json();
            resp.set_json(200, j.dump());
            return;
        }

        // ---- POST /api/v1/settings ---------------------------------------------
        if (req.method == "POST" && req.path == "/api/v1/settings") {
            try {
                auto body_j = json::Json::parse(req.body.empty() ? "{}" : req.body);
                if (body_j.contains("max_storage_mb")) {
                    const int64_t mb = body_j["max_storage_mb"].as_int();
                    const uint64_t bytes = (mb > 0) ? (static_cast<uint64_t>(mb) * 1024ULL * 1024ULL) : 0ULL;
                    g_max_storage_bytes.store(bytes);
                    db.set_config("max_storage_bytes", std::to_string(bytes));
                    g_telemetry.add_event("SETTINGS", "INFO",
                                          "Updated storage quota to " +
                                              (mb > 0 ? (std::to_string(mb) + " MB") : std::string("UNLIMITED")));
                }
                if (body_j.contains("seed_peer") && !body_j["seed_peer"].as_string().empty()) {
                    const std::string ep = body_j["seed_peer"].as_string();
                    const size_t colon = ep.rfind(':');
                    if (colon != std::string::npos && colon > 0 && colon + 1 < ep.size()) {
                        const std::string s_ip = ep.substr(0, colon);
                        const int s_port = std::stoi(ep.substr(colon + 1));
                        if (s_port >= 1 && s_port <= 65535) {
                            p2p_manager.add_seed_peer(s_ip, static_cast<uint16_t>(s_port));
                            g_telemetry.add_event("SETTINGS", "INFO", "Added seed peer " + ep);
                        }
                    }
                }
                json::Json j = json::Json::object();
                j["status"] = "OK";
                j["peer_id"] = id;
                j["max_storage_bytes"] = static_cast<long long>(g_max_storage_bytes.load());
                resp.set_json(200, j.dump());
                return;
            } catch (const std::exception& e) {
                json::Json err = json::Json::object();
                err["status"] = "ERROR";
                err["error"] = std::string("Invalid settings payload: ") + e.what();
                resp.set_json(400, err.dump());
                return;
            }
        }

        json::Json err = json::Json::object();
        err["status"] = "ERROR";
        err["error"] = "Unknown endpoint: " + req.method + " " + req.path;
        resp.set_json(404, err.dump());
    };

    TCPServer server(address, port, handler);
    server_ptr = &server;
    if (!server.start()) {
        std::cerr << "[peer " << id << "] failed to bind " << address << ":"
                  << port << "\n";
        return 1;
    }

    HttpServer http_server(ui_bind_address, ui_port, staging_dir, http_handler);
    http_server_ptr = &http_server;
    if (ui_enabled) {
        if (http_server.start(!cli_ui_port_explicit)) {
            bound_ui_port = http_server.bound_port();
            db.set_config("ui_port", std::to_string(bound_ui_port));
        } else {
            std::cerr << "[peer " << id << "] warning: could not bind Web Dashboard HTTP server on "
                      << ui_bind_address << ":" << ui_port << "\n";
            ui_enabled = false;
            db.set_config("ui_enabled", "0");
        }
    }

    std::cout << "[peer " << id << "] ONLINE at " << address << ":" << port
              << " storage=" << storage_dir << " db=" << db_path;
    if (ui_enabled) {
        std::cout << " ui=http://127.0.0.1:" << bound_ui_port;
    }
    std::cout << std::endl;

    // Start decentralized P2P discovery, heartbeat monitor & metadata sync
    p2p_manager.start();

    // Start legacy coordinator registration only if non-P2P coordinator is configured
    PeerRegistrar registrar(id, address, port, coord_address, coord_port, storage_dir);
    if (!is_p2p_mode && !coord_address.empty() && coord_port != 0) {
        registrar.start();
    }

    proc::install_shutdown_handler([&server, &http_server, &registrar, &p2p_manager]() {
        p2p_manager.stop();
        registrar.stop();
        http_server.stop();
        server.stop();
    });

    s_running_server = &server;
    server.run();
    s_running_server = nullptr;
    http_server.stop();
    p2p_manager.stop();
    registrar.stop();
    db.close();
    proc::remove_shutdown_handler();
    std::cout << "[peer " << id << "] stopped" << std::endl;
    return 0;
}
