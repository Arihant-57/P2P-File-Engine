// ============================================================================
// database_manager.hpp - Per-Peer Embedded SQLite Database Manager (Task 1 & 2)
//
// Each peer maintains its own local SQLite database at:
//   storage/peer_<PEER_ID>/metadata.db
//
// Features:
//   - C++17 RAII wrapper around sqlite3*
//   - WAL journal mode + 5000ms busy timeout + FULLMUTEX + C++ std::mutex
//   - Parameterized prepared statements & atomic transactions
//   - Task 1: Persists peer identity, IP, TCP port, last-seen epoch ms, health
//     state, storage capacity, discovery source, and local configuration
//   - Task 2: Persists distributed file manifests (files, stripes, shards,
//     shard_locations), idempotent metadata_sync_log, recovery_records, and
//     provides searchable distributed catalogue queries with live RS(3,2)
//     reconstructability evaluation
// ============================================================================
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "../../models/file_manifest.hpp"
#include "../../models/peer.hpp"
#include "../../third_party/mini_json.hpp"

struct sqlite3;

class DatabaseManager {
public:
    DatabaseManager() = default;
    explicit DatabaseManager(const std::string& db_path);
    ~DatabaseManager();

    DatabaseManager(const DatabaseManager&) = delete;
    DatabaseManager& operator=(const DatabaseManager&) = delete;

    // Open database, apply WAL / busy_timeout pragmas, and initialize schema.
    bool open(const std::string& db_path, std::string* err_out = nullptr);
    void close();
    bool is_open() const;
    const std::string& db_path() const { return db_path_; }

    // Current UTC epoch milliseconds helper.
    static int64_t now_epoch_ms();

    // Local configuration key-value persistence.
    bool set_config(const std::string& key, const std::string& value);
    std::string get_config(const std::string& key, const std::string& default_val = "") const;
    json::Json get_all_config_json() const;

    // ---- Task 1: Peer table persistence ------------------------------------
    bool upsert_peer(const PeerInfo& peer,
                     int64_t last_seen_epoch_ms,
                     const std::string& discovery_source = "UDP",
                     bool is_self = false);

    bool update_peer_health(const std::string& peer_id, PeerHealth health);

    bool get_peer(const std::string& peer_id,
                  PeerInfo& out,
                  int64_t* last_seen_epoch_ms_out = nullptr,
                  std::string* discovery_source_out = nullptr) const;

    std::vector<PeerInfo> get_all_peers(bool include_self = true) const;
    json::Json get_all_peers_json(bool include_self = true) const;

    // ---- Task 2: Distributed File Manifest & Catalogue Persistence ---------
    // Atomically insert or update a complete FileManifest (files, stripes,
    // shards, shard_locations, and metadata_sync_log). Idempotent across
    // duplicate or out-of-order deliveries.
    bool upsert_file_manifest(const FileManifest& manifest,
                              const std::string& origin_peer_id = "",
                              int64_t origin_seq = 0);

    // Update or insert a single shard location and bump file version if needed.
    bool upsert_shard_location(const ShardLocationEntry& loc,
                               const std::string& file_id = "",
                               int32_t new_file_version = 0,
                               const std::string& origin_peer_id = "");

    // Atomically record a locally stored shard upon receiving STORE_CHUNK (Task 3.5).
    // Ensures parent files/stripes rows exist (with status 'UPLOADING' if not yet present)
    // and upserts shards + shard_locations ('STORED' on local_peer_id).
    bool record_received_shard(const std::string& local_peer_id,
                               const std::string& file_id,
                               const std::string& original_name,
                               uint64_t file_size,
                               const std::string& file_sha256,
                               int32_t total_stripes,
                               int32_t total_shards,
                               const std::string& uploader_peer_id,
                               int32_t stripe_index,
                               uint64_t stripe_data_bytes,
                               const std::string& shard_id,
                               int32_t shard_index,
                               const std::string& shard_type,
                               uint64_t shard_size,
                               uint64_t original_bytes,
                               const std::string& shard_sha256);

    // Load a complete FileManifest (with ordered stripes, shards, and locations).
    bool get_file_manifest(const std::string& file_id, FileManifest& out) const;

    // List all FileManifests in deterministic file_id order.
    std::vector<FileManifest> list_file_manifests() const;

    // Search FileManifests by exact file_id or case-insensitive filename / file_id substring.
    std::vector<FileManifest> search_file_manifests(const std::string& query) const;

    // Return compact map of (file_id -> version) for anti-entropy metadata sync.
    std::map<std::string, int32_t> get_metadata_summary() const;
    json::Json get_metadata_summary_json() const;

    // Evaluate live RS(3,2) reconstructability and shard hosting status for a manifest
    // by cross-referencing shard_locations with current peer health states in SQLite.
    CatalogueEntry evaluate_catalogue_entry(const FileManifest& manifest) const;

    // Query the local distributed file catalogue (empty query = list all files).
    std::vector<CatalogueEntry> query_catalogue(const std::string& query = "") const;
    json::Json query_catalogue_json(const std::string& query = "") const;

    // Metadata sync log & recovery records.
    std::vector<MetadataSyncEntry> get_sync_log_entries(int64_t limit = 100) const;
    bool upsert_recovery_record(const RecoveryRecordEntry& rec);
    bool get_recovery_record(const std::string& recovery_id, RecoveryRecordEntry& out) const;
    std::vector<RecoveryRecordEntry> get_recovery_records(const std::string& file_id = "") const;

    // Diagnostic helper to count rows in a table (for verification tests).
    int64_t count_rows(const std::string& table_name) const;

private:
    bool exec_sql_unlocked(const char* sql, std::string* err_out = nullptr);
    bool init_schema_unlocked(std::string* err_out = nullptr);
    bool load_file_manifest_unlocked(const std::string& file_id, FileManifest& out) const;
    CatalogueEntry evaluate_catalogue_entry_unlocked(
        const FileManifest& manifest,
        const std::map<std::string, PeerHealth>& peer_health_map) const;

    sqlite3* db_ = nullptr;
    std::string db_path_;
    mutable std::mutex mutex_;
};
