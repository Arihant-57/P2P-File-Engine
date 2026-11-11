// ============================================================================
// database_manager.cpp - Per-Peer Embedded SQLite Database Manager (Task 1 & 2)
// ============================================================================
#include "database_manager.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <set>

#include "../../third_party/sqlite/sqlite3.h"

namespace fs = std::filesystem;

namespace {

struct StmtGuard {
    sqlite3_stmt* stmt = nullptr;
    explicit StmtGuard(sqlite3_stmt* s = nullptr) : stmt(s) {}
    ~StmtGuard() {
        if (stmt) sqlite3_finalize(stmt);
    }
    StmtGuard(const StmtGuard&) = delete;
    StmtGuard& operator=(const StmtGuard&) = delete;
};

constexpr const char* kSchemaDDL = R"SQL(
CREATE TABLE IF NOT EXISTS local_config (
    key              TEXT PRIMARY KEY,
    value            TEXT NOT NULL,
    updated_epoch_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS peers (
    peer_id                 TEXT PRIMARY KEY,
    address                 TEXT NOT NULL,
    port                    INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    health_state            TEXT NOT NULL DEFAULT 'ONLINE'
                            CHECK (health_state IN ('ONLINE', 'SUSPECT', 'OFFLINE', 'RECOVERING', 'UNKNOWN')),
    available_storage_bytes INTEGER NOT NULL DEFAULT 0,
    active_transfers        INTEGER NOT NULL DEFAULT 0,
    transfer_capacity       INTEGER NOT NULL DEFAULT 10,
    last_seen_epoch_ms      INTEGER NOT NULL,
    discovery_source        TEXT NOT NULL DEFAULT 'UDP',
    is_self                 INTEGER NOT NULL DEFAULT 0 CHECK (is_self IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_peers_health ON peers(health_state);

CREATE TABLE IF NOT EXISTS files (
    file_id               TEXT PRIMARY KEY,
    original_name         TEXT NOT NULL,
    file_size             INTEGER NOT NULL CHECK (file_size >= 0),
    sha256                TEXT NOT NULL,
    shard_size            INTEGER NOT NULL CHECK (shard_size > 0),
    data_shards_k         INTEGER NOT NULL DEFAULT 2,
    parity_shards_m       INTEGER NOT NULL DEFAULT 1,
    total_stripes         INTEGER NOT NULL CHECK (total_stripes >= 0),
    total_shards          INTEGER NOT NULL CHECK (total_shards >= 0),
    uploader_peer_id      TEXT NOT NULL,
    uploaded_at_epoch_ms  INTEGER NOT NULL,
    status                TEXT NOT NULL DEFAULT 'AVAILABLE'
                          CHECK (status IN ('UPLOADING', 'AVAILABLE', 'DEGRADED', 'UNRECOVERABLE')),
    version               INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_files_name ON files(original_name);

CREATE TABLE IF NOT EXISTS stripes (
    file_id             TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    stripe_index        INTEGER NOT NULL CHECK (stripe_index >= 0),
    stripe_data_bytes   INTEGER NOT NULL CHECK (stripe_data_bytes >= 0),
    shard_size          INTEGER NOT NULL CHECK (shard_size > 0),
    PRIMARY KEY (file_id, stripe_index)
);

CREATE TABLE IF NOT EXISTS shards (
    shard_id         TEXT PRIMARY KEY,
    file_id          TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    stripe_index     INTEGER NOT NULL,
    shard_index      INTEGER NOT NULL CHECK (shard_index IN (0, 1, 2)),
    shard_type       TEXT NOT NULL CHECK (shard_type IN ('DATA', 'PARITY')),
    shard_size       INTEGER NOT NULL CHECK (shard_size > 0),
    original_bytes   INTEGER NOT NULL CHECK (original_bytes >= 0),
    sha256           TEXT NOT NULL,
    UNIQUE (file_id, stripe_index, shard_index)
);

CREATE INDEX IF NOT EXISTS idx_shards_file ON shards(file_id, stripe_index);

CREATE TABLE IF NOT EXISTS shard_locations (
    shard_id              TEXT NOT NULL REFERENCES shards(shard_id) ON DELETE CASCADE,
    peer_id               TEXT NOT NULL,
    location_status       TEXT NOT NULL DEFAULT 'STORED'
                          CHECK (location_status IN ('STORED', 'STALE', 'CORRUPTED', 'RECOVERING')),
    verified_at_epoch_ms  INTEGER NOT NULL,
    version               INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (shard_id, peer_id)
);

CREATE INDEX IF NOT EXISTS idx_shard_locations_peer ON shard_locations(peer_id, location_status);

CREATE TABLE IF NOT EXISTS metadata_sync_log (
    update_id            TEXT PRIMARY KEY,
    origin_peer_id       TEXT NOT NULL,
    origin_seq           INTEGER NOT NULL,
    entity_type          TEXT NOT NULL CHECK (entity_type IN ('FILE', 'SHARD_LOCATION', 'PEER', 'RECOVERY')),
    entity_key           TEXT NOT NULL,
    payload_json         TEXT NOT NULL,
    created_at_epoch_ms  INTEGER NOT NULL,
    UNIQUE (origin_peer_id, origin_seq)
);

CREATE TABLE IF NOT EXISTS recovery_records (
    recovery_id          TEXT PRIMARY KEY,
    failed_peer_id       TEXT NOT NULL,
    shard_id             TEXT NOT NULL,
    file_id              TEXT NOT NULL,
    stripe_index         INTEGER NOT NULL,
    rebuilder_peer_id    TEXT NOT NULL,
    target_peer_id       TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('STARTED', 'COMPLETED', 'FAILED', 'UNRECOVERABLE')),
    reason               TEXT NOT NULL DEFAULT '',
    timestamp_epoch_ms   INTEGER NOT NULL
);
)SQL";

}  // namespace

DatabaseManager::DatabaseManager(const std::string& db_path) {
    open(db_path, nullptr);
}

DatabaseManager::~DatabaseManager() {
    close();
}

int64_t DatabaseManager::now_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool DatabaseManager::open(const std::string& db_path, std::string* err_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
    db_path_ = db_path;

    std::error_code ec;
    const fs::path p = fs::u8path(db_path);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path(), ec);
    }

    const int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
    if (sqlite3_open_v2(db_path.c_str(), &db_, flags, nullptr) != SQLITE_OK) {
        if (err_out) *err_out = db_ ? sqlite3_errmsg(db_) : "sqlite3_open_v2 failed";
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
        return false;
    }

    exec_sql_unlocked("PRAGMA busy_timeout = 5000;");
    exec_sql_unlocked("PRAGMA journal_mode = WAL;");
    exec_sql_unlocked("PRAGMA synchronous = NORMAL;");
    exec_sql_unlocked("PRAGMA foreign_keys = ON;");

    if (!init_schema_unlocked(err_out)) {
        sqlite3_close(db_);
        db_ = nullptr;
        return false;
    }
    return true;
}

void DatabaseManager::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

bool DatabaseManager::is_open() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return db_ != nullptr;
}

bool DatabaseManager::exec_sql_unlocked(const char* sql, std::string* err_out) {
    if (!db_) return false;
    char* errmsg = nullptr;
    const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &errmsg);
    if (rc != SQLITE_OK) {
        if (err_out && errmsg) *err_out = errmsg;
        if (errmsg) sqlite3_free(errmsg);
        return false;
    }
    return true;
}

bool DatabaseManager::init_schema_unlocked(std::string* err_out) {
    if (!exec_sql_unlocked("BEGIN IMMEDIATE TRANSACTION;", err_out)) return false;
    if (!exec_sql_unlocked(kSchemaDDL, err_out)) {
        exec_sql_unlocked("ROLLBACK;");
        return false;
    }
    return exec_sql_unlocked("COMMIT;", err_out);
}

bool DatabaseManager::set_config(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) return false;

    const char* sql =
        "INSERT INTO local_config (key, value, updated_epoch_ms) VALUES (?1, ?2, ?3) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_epoch_ms = excluded.updated_epoch_ms;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;

    const int64_t ts = now_epoch_ms();
    sqlite3_bind_text(sg.stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(sg.stmt, 3, ts);

    return sqlite3_step(sg.stmt) == SQLITE_DONE;
}

std::string DatabaseManager::get_config(const std::string& key, const std::string& default_val) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) return default_val;

    const char* sql = "SELECT value FROM local_config WHERE key = ?1;";
    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return default_val;

    sqlite3_bind_text(sg.stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        const unsigned char* txt = sqlite3_column_text(sg.stmt, 0);
        if (txt) return reinterpret_cast<const char*>(txt);
    }
    return default_val;
}

json::Json DatabaseManager::get_all_config_json() const {
    std::lock_guard<std::mutex> lock(mutex_);
    json::Json obj = json::Json::object();
    if (!db_) return obj;

    const char* sql = "SELECT key, value FROM local_config ORDER BY key ASC;";
    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return obj;

    while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        const unsigned char* k = sqlite3_column_text(sg.stmt, 0);
        const unsigned char* v = sqlite3_column_text(sg.stmt, 1);
        if (k && v) {
            obj[reinterpret_cast<const char*>(k)] = std::string(reinterpret_cast<const char*>(v));
        }
    }
    return obj;
}

// ============================================================================
// Task 1: Peer Table Persistence
// ============================================================================

bool DatabaseManager::upsert_peer(const PeerInfo& peer,
                                  int64_t last_seen_epoch_ms,
                                  const std::string& discovery_source,
                                  bool is_self) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || peer.id.empty() || peer.port == 0) return false;

    const char* sql =
        "INSERT INTO peers ("
        "  peer_id, address, port, health_state, available_storage_bytes, "
        "  active_transfers, transfer_capacity, last_seen_epoch_ms, discovery_source, is_self"
        ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) "
        "ON CONFLICT(peer_id) DO UPDATE SET "
        "  address = excluded.address, "
        "  port = excluded.port, "
        "  health_state = excluded.health_state, "
        "  available_storage_bytes = excluded.available_storage_bytes, "
        "  active_transfers = excluded.active_transfers, "
        "  transfer_capacity = excluded.transfer_capacity, "
        "  last_seen_epoch_ms = excluded.last_seen_epoch_ms, "
        "  discovery_source = excluded.discovery_source, "
        "  is_self = CASE WHEN peers.is_self = 1 THEN 1 ELSE excluded.is_self END;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;

    const char* health_str = peer_health_str(peer.state == PeerHealth::UNKNOWN ? PeerHealth::ONLINE : peer.state);
    sqlite3_bind_text(sg.stmt, 1, peer.id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 2, peer.address.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(sg.stmt, 3, static_cast<int>(peer.port));
    sqlite3_bind_text(sg.stmt, 4, health_str, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(sg.stmt, 5, static_cast<sqlite3_int64>(peer.workload.available_storage_bytes));
    sqlite3_bind_int(sg.stmt, 6, static_cast<int>(peer.workload.active_transfers));
    sqlite3_bind_int(sg.stmt, 7, static_cast<int>(peer.workload.transfer_capacity));
    sqlite3_bind_int64(sg.stmt, 8, static_cast<sqlite3_int64>(last_seen_epoch_ms));
    sqlite3_bind_text(sg.stmt, 9, discovery_source.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(sg.stmt, 10, is_self ? 1 : 0);

    return sqlite3_step(sg.stmt) == SQLITE_DONE;
}

bool DatabaseManager::update_peer_health(const std::string& peer_id, PeerHealth health) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || peer_id.empty()) return false;

    const char* sql = "UPDATE peers SET health_state = ?1 WHERE peer_id = ?2;";
    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;

    sqlite3_bind_text(sg.stmt, 1, peer_health_str(health), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 2, peer_id.c_str(), -1, SQLITE_TRANSIENT);

    return sqlite3_step(sg.stmt) == SQLITE_DONE;
}

bool DatabaseManager::get_peer(const std::string& peer_id,
                               PeerInfo& out,
                               int64_t* last_seen_epoch_ms_out,
                               std::string* discovery_source_out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || peer_id.empty()) return false;

    const char* sql =
        "SELECT peer_id, address, port, health_state, available_storage_bytes, "
        "       active_transfers, transfer_capacity, last_seen_epoch_ms, discovery_source "
        "FROM peers WHERE peer_id = ?1;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;

    sqlite3_bind_text(sg.stmt, 1, peer_id.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(sg.stmt) != SQLITE_ROW) return false;

    const unsigned char* id_txt = sqlite3_column_text(sg.stmt, 0);
    const unsigned char* addr_txt = sqlite3_column_text(sg.stmt, 1);
    const int port = sqlite3_column_int(sg.stmt, 2);
    const unsigned char* health_txt = sqlite3_column_text(sg.stmt, 3);
    const sqlite3_int64 avail_bytes = sqlite3_column_int64(sg.stmt, 4);
    const int active_tx = sqlite3_column_int(sg.stmt, 5);
    const int tx_cap = sqlite3_column_int(sg.stmt, 6);
    const sqlite3_int64 last_seen = sqlite3_column_int64(sg.stmt, 7);
    const unsigned char* disc_txt = sqlite3_column_text(sg.stmt, 8);

    out.id = id_txt ? reinterpret_cast<const char*>(id_txt) : "";
    out.address = addr_txt ? reinterpret_cast<const char*>(addr_txt) : "";
    out.port = static_cast<uint16_t>(port);
    out.state = health_txt ? peer_health_from_str(reinterpret_cast<const char*>(health_txt)) : PeerHealth::UNKNOWN;
    out.workload.available_storage_bytes = static_cast<uint64_t>(avail_bytes);
    out.workload.active_transfers = static_cast<uint32_t>(active_tx);
    out.workload.transfer_capacity = static_cast<uint32_t>(tx_cap);

    if (last_seen_epoch_ms_out) *last_seen_epoch_ms_out = static_cast<int64_t>(last_seen);
    if (discovery_source_out) {
        *discovery_source_out = disc_txt ? reinterpret_cast<const char*>(disc_txt) : "";
    }
    return true;
}

std::vector<PeerInfo> DatabaseManager::get_all_peers(bool include_self) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PeerInfo> list;
    if (!db_) return list;

    const char* sql = include_self
        ? "SELECT peer_id, address, port, health_state, available_storage_bytes, "
          "       active_transfers, transfer_capacity "
          "FROM peers ORDER BY peer_id ASC;"
        : "SELECT peer_id, address, port, health_state, available_storage_bytes, "
          "       active_transfers, transfer_capacity "
          "FROM peers WHERE is_self = 0 ORDER BY peer_id ASC;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return list;

    while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        PeerInfo p;
        const unsigned char* id_txt = sqlite3_column_text(sg.stmt, 0);
        const unsigned char* addr_txt = sqlite3_column_text(sg.stmt, 1);
        p.id = id_txt ? reinterpret_cast<const char*>(id_txt) : "";
        p.address = addr_txt ? reinterpret_cast<const char*>(addr_txt) : "";
        p.port = static_cast<uint16_t>(sqlite3_column_int(sg.stmt, 2));
        const unsigned char* health_txt = sqlite3_column_text(sg.stmt, 3);
        p.state = health_txt ? peer_health_from_str(reinterpret_cast<const char*>(health_txt)) : PeerHealth::UNKNOWN;
        p.workload.available_storage_bytes = static_cast<uint64_t>(sqlite3_column_int64(sg.stmt, 4));
        p.workload.active_transfers = static_cast<uint32_t>(sqlite3_column_int(sg.stmt, 5));
        p.workload.transfer_capacity = static_cast<uint32_t>(sqlite3_column_int(sg.stmt, 6));
        list.push_back(std::move(p));
    }
    return list;
}

json::Json DatabaseManager::get_all_peers_json(bool include_self) const {
    std::lock_guard<std::mutex> lock(mutex_);
    json::Json arr = json::Json::array();
    if (!db_) return arr;

    const char* sql = include_self
        ? "SELECT peer_id, address, port, health_state, available_storage_bytes, "
          "       active_transfers, transfer_capacity, last_seen_epoch_ms, discovery_source, is_self "
          "FROM peers ORDER BY peer_id ASC;"
        : "SELECT peer_id, address, port, health_state, available_storage_bytes, "
          "       active_transfers, transfer_capacity, last_seen_epoch_ms, discovery_source, is_self "
          "FROM peers WHERE is_self = 0 ORDER BY peer_id ASC;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return arr;

    while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        json::Json j = json::Json::object();
        const unsigned char* id_txt = sqlite3_column_text(sg.stmt, 0);
        const unsigned char* addr_txt = sqlite3_column_text(sg.stmt, 1);
        const unsigned char* health_txt = sqlite3_column_text(sg.stmt, 3);
        const unsigned char* disc_txt = sqlite3_column_text(sg.stmt, 8);

        j["id"] = id_txt ? std::string(reinterpret_cast<const char*>(id_txt)) : std::string();
        j["address"] = addr_txt ? std::string(reinterpret_cast<const char*>(addr_txt)) : std::string();
        j["port"] = static_cast<long long>(sqlite3_column_int(sg.stmt, 2));
        j["health"] = health_txt ? std::string(reinterpret_cast<const char*>(health_txt)) : std::string("UNKNOWN");
        j["available_storage_bytes"] = static_cast<long long>(sqlite3_column_int64(sg.stmt, 4));
        j["active_transfers"] = static_cast<long long>(sqlite3_column_int(sg.stmt, 5));
        j["transfer_capacity"] = static_cast<long long>(sqlite3_column_int(sg.stmt, 6));
        j["last_seen_epoch_ms"] = static_cast<long long>(sqlite3_column_int64(sg.stmt, 7));
        j["discovery_source"] = disc_txt ? std::string(reinterpret_cast<const char*>(disc_txt)) : std::string();
        j["is_self"] = sqlite3_column_int(sg.stmt, 9) != 0;
        arr.push_back(std::move(j));
    }
    return arr;
}

// ============================================================================
// Task 2: Distributed File Manifest & Catalogue Persistence
// ============================================================================

bool DatabaseManager::upsert_file_manifest(const FileManifest& manifest,
                                           const std::string& origin_peer_id,
                                           int64_t origin_seq) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || manifest.file_id.empty() || manifest.original_name.empty()) return false;
    if (manifest.shard_size == 0) return false;

    if (!exec_sql_unlocked("BEGIN IMMEDIATE TRANSACTION;")) return false;

    auto rollback_and_fail = [this]() -> bool {
        exec_sql_unlocked("ROLLBACK;");
        return false;
    };

    const int32_t incoming_ver = manifest.version > 0 ? manifest.version : 1;

    // 0. Check if a newer version of this file_id is already persisted (stale delivery protection)
    {
        const char* sql_chk = "SELECT version FROM files WHERE file_id = ?1;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_chk, -1, &sg.stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(sg.stmt, 1, manifest.file_id.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(sg.stmt) == SQLITE_ROW) {
                const int32_t existing_ver = sqlite3_column_int(sg.stmt, 0);
                if (existing_ver > incoming_ver && (incoming_ver <= 1 || manifest.status == "UPLOADING")) {
                    // Stale initial/uploading manifest; idempotent no-op
                    return exec_sql_unlocked("COMMIT;");
                }
            }
        }
    }

    // 1. Upsert into files (only advance or keep equal version; never overwrite newer version with older)
    {
        const char* sql_file =
            "INSERT INTO files ("
            "  file_id, original_name, file_size, sha256, shard_size, "
            "  data_shards_k, parity_shards_m, total_stripes, total_shards, "
            "  uploader_peer_id, uploaded_at_epoch_ms, status, version"
            ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "  original_name = CASE WHEN excluded.version >= files.version THEN excluded.original_name ELSE files.original_name END, "
            "  file_size = CASE WHEN excluded.version >= files.version THEN excluded.file_size ELSE files.file_size END, "
            "  sha256 = CASE WHEN excluded.version >= files.version THEN excluded.sha256 ELSE files.sha256 END, "
            "  shard_size = CASE WHEN excluded.version >= files.version THEN excluded.shard_size ELSE files.shard_size END, "
            "  data_shards_k = CASE WHEN excluded.version >= files.version THEN excluded.data_shards_k ELSE files.data_shards_k END, "
            "  parity_shards_m = CASE WHEN excluded.version >= files.version THEN excluded.parity_shards_m ELSE files.parity_shards_m END, "
            "  total_stripes = CASE WHEN excluded.version >= files.version THEN excluded.total_stripes ELSE files.total_stripes END, "
            "  total_shards = CASE WHEN excluded.version >= files.version THEN excluded.total_shards ELSE files.total_shards END, "
            "  uploader_peer_id = CASE WHEN excluded.version >= files.version THEN excluded.uploader_peer_id ELSE files.uploader_peer_id END, "
            "  uploaded_at_epoch_ms = CASE WHEN excluded.version >= files.version THEN excluded.uploaded_at_epoch_ms ELSE files.uploaded_at_epoch_ms END, "
            "  status = CASE WHEN excluded.version >= files.version THEN excluded.status ELSE files.status END, "
            "  version = CASE WHEN excluded.version > files.version THEN excluded.version ELSE files.version END;";

        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_file, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }
        const int64_t uploaded_ts = manifest.uploaded_at_epoch_ms > 0
                                        ? manifest.uploaded_at_epoch_ms
                                        : now_epoch_ms();
        const std::string status_str = manifest.status.empty() ? "AVAILABLE" : manifest.status;
        const int32_t ver = manifest.version > 0 ? manifest.version : 1;

        sqlite3_bind_text(sg.stmt, 1, manifest.file_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, manifest.original_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(manifest.file_size));
        sqlite3_bind_text(sg.stmt, 4, manifest.sha256.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 5, static_cast<sqlite3_int64>(manifest.shard_size));
        sqlite3_bind_int(sg.stmt, 6, manifest.data_shards_k);
        sqlite3_bind_int(sg.stmt, 7, manifest.parity_shards_m);
        sqlite3_bind_int(sg.stmt, 8, manifest.total_stripes);
        sqlite3_bind_int(sg.stmt, 9, manifest.total_shards);
        sqlite3_bind_text(sg.stmt, 10, manifest.uploader_peer_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 11, static_cast<sqlite3_int64>(uploaded_ts));
        sqlite3_bind_text(sg.stmt, 12, status_str.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(sg.stmt, 13, ver);

        if (sqlite3_step(sg.stmt) != SQLITE_DONE) {
            return rollback_and_fail();
        }
    }

    // 2. Upsert stripes
    {
        const char* sql_stripe =
            "INSERT INTO stripes (file_id, stripe_index, stripe_data_bytes, shard_size) "
            "VALUES (?1, ?2, ?3, ?4) "
            "ON CONFLICT(file_id, stripe_index) DO UPDATE SET "
            "  stripe_data_bytes = excluded.stripe_data_bytes, "
            "  shard_size = excluded.shard_size;";

        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_stripe, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }

        for (const auto& st : manifest.stripes) {
            sqlite3_reset(sg.stmt);
            sqlite3_clear_bindings(sg.stmt);
            const uint64_t st_shard_size = st.shard_size > 0 ? st.shard_size : manifest.shard_size;
            sqlite3_bind_text(sg.stmt, 1, manifest.file_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sg.stmt, 2, st.stripe_index);
            sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(st.stripe_data_bytes));
            sqlite3_bind_int64(sg.stmt, 4, static_cast<sqlite3_int64>(st_shard_size));

            if (sqlite3_step(sg.stmt) != SQLITE_DONE) {
                return rollback_and_fail();
            }
        }
    }

    // 3. Upsert shards & shard_locations
    {
        const char* sql_shard =
            "INSERT INTO shards ("
            "  shard_id, file_id, stripe_index, shard_index, shard_type, "
            "  shard_size, original_bytes, sha256"
            ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) "
            "ON CONFLICT(shard_id) DO UPDATE SET "
            "  shard_size = excluded.shard_size, "
            "  original_bytes = excluded.original_bytes, "
            "  sha256 = excluded.sha256;";

        const char* sql_loc =
            "INSERT INTO shard_locations ("
            "  shard_id, peer_id, location_status, verified_at_epoch_ms, version"
            ") VALUES (?1, ?2, ?3, ?4, ?5) "
            "ON CONFLICT(shard_id, peer_id) DO UPDATE SET "
            "  location_status = CASE "
            "    WHEN excluded.version > shard_locations.version THEN excluded.location_status "
            "    WHEN excluded.version = shard_locations.version AND (shard_locations.location_status = 'STORED' OR excluded.location_status IN ('STALE', 'CORRUPTED')) THEN excluded.location_status "
            "    ELSE shard_locations.location_status END, "
            "  verified_at_epoch_ms = CASE WHEN excluded.verified_at_epoch_ms > shard_locations.verified_at_epoch_ms THEN excluded.verified_at_epoch_ms ELSE shard_locations.verified_at_epoch_ms END, "
            "  version = CASE WHEN excluded.version > shard_locations.version THEN excluded.version ELSE shard_locations.version END;";

        StmtGuard sg_sh;
        StmtGuard sg_loc;
        if (sqlite3_prepare_v2(db_, sql_shard, -1, &sg_sh.stmt, nullptr) != SQLITE_OK ||
            sqlite3_prepare_v2(db_, sql_loc, -1, &sg_loc.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }

        const int64_t now_ts = now_epoch_ms();
        for (const auto& sh : manifest.shards) {
            sqlite3_reset(sg_sh.stmt);
            sqlite3_clear_bindings(sg_sh.stmt);
            const uint64_t sh_size = sh.shard_size > 0 ? sh.shard_size : manifest.shard_size;
            sqlite3_bind_text(sg_sh.stmt, 1, sh.shard_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(sg_sh.stmt, 2, manifest.file_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(sg_sh.stmt, 3, sh.stripe_index);
            sqlite3_bind_int(sg_sh.stmt, 4, sh.shard_index);
            sqlite3_bind_text(sg_sh.stmt, 5, sh.shard_type.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(sg_sh.stmt, 6, static_cast<sqlite3_int64>(sh_size));
            sqlite3_bind_int64(sg_sh.stmt, 7, static_cast<sqlite3_int64>(sh.original_bytes));
            sqlite3_bind_text(sg_sh.stmt, 8, sh.sha256.c_str(), -1, SQLITE_TRANSIENT);

            if (sqlite3_step(sg_sh.stmt) != SQLITE_DONE) {
                return rollback_and_fail();
            }

            for (const auto& loc : sh.locations) {
                if (loc.peer_id.empty()) continue;
                sqlite3_reset(sg_loc.stmt);
                sqlite3_clear_bindings(sg_loc.stmt);
                const int64_t v_ts = loc.verified_at_epoch_ms > 0 ? loc.verified_at_epoch_ms : now_ts;
                const int32_t loc_ver = loc.version > 0 ? loc.version : manifest.version;
                const std::string st = loc.location_status.empty() ? "STORED" : loc.location_status;

                sqlite3_bind_text(sg_loc.stmt, 1, sh.shard_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(sg_loc.stmt, 2, loc.peer_id.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(sg_loc.stmt, 3, st.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(sg_loc.stmt, 4, static_cast<sqlite3_int64>(v_ts));
                sqlite3_bind_int(sg_loc.stmt, 5, loc_ver > 0 ? loc_ver : 1);

                if (sqlite3_step(sg_loc.stmt) != SQLITE_DONE) {
                    return rollback_and_fail();
                }
            }
        }
    }

    // 4. Record idempotent entry in metadata_sync_log
    {
        const std::string eff_origin = !manifest.uploader_peer_id.empty()
                                           ? manifest.uploader_peer_id
                                           : (origin_peer_id.empty() ? "LOCAL" : origin_peer_id);
        const int64_t eff_seq = origin_seq > 0
                                    ? origin_seq
                                    : static_cast<int64_t>(incoming_ver);
        const std::string update_id = "FILE:" + manifest.file_id + ":v" + std::to_string(incoming_ver);
        // Deterministic sequence key per (file_id, version)
        int64_t unique_seq = eff_seq;
        for (unsigned char c : update_id) {
            unique_seq = (unique_seq * 131) + c;
        }
        unique_seq = (unique_seq & 0x7FFFFFFFFFFFFFFFLL);

        const char* sql_log =
            "INSERT OR IGNORE INTO metadata_sync_log ("
            "  update_id, origin_peer_id, origin_seq, entity_type, entity_key, payload_json, created_at_epoch_ms"
            ") VALUES (?1, ?2, ?3, 'FILE', ?4, ?5, ?6);";

        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_log, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }
        const std::string payload_str = manifest.to_json().dump();
        sqlite3_bind_text(sg.stmt, 1, update_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, eff_origin.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(unique_seq));
        sqlite3_bind_text(sg.stmt, 4, manifest.file_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 5, payload_str.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 6, static_cast<sqlite3_int64>(now_epoch_ms()));

        if (sqlite3_step(sg.stmt) != SQLITE_DONE) {
            return rollback_and_fail();
        }
    }

    return exec_sql_unlocked("COMMIT;");
}

bool DatabaseManager::upsert_shard_location(const ShardLocationEntry& loc,
                                            const std::string& file_id,
                                            int32_t new_file_version,
                                            const std::string& origin_peer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || loc.shard_id.empty() || loc.peer_id.empty()) return false;

    if (!exec_sql_unlocked("BEGIN IMMEDIATE TRANSACTION;")) return false;

    const char* sql_loc =
        "INSERT INTO shard_locations ("
        "  shard_id, peer_id, location_status, verified_at_epoch_ms, version"
        ") VALUES (?1, ?2, ?3, ?4, ?5) "
        "ON CONFLICT(shard_id, peer_id) DO UPDATE SET "
        "  location_status = CASE WHEN excluded.version >= shard_locations.version THEN excluded.location_status ELSE shard_locations.location_status END, "
        "  verified_at_epoch_ms = CASE WHEN excluded.verified_at_epoch_ms > shard_locations.verified_at_epoch_ms THEN excluded.verified_at_epoch_ms ELSE shard_locations.verified_at_epoch_ms END, "
        "  version = CASE WHEN excluded.version > shard_locations.version THEN excluded.version ELSE shard_locations.version END;";

    {
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_loc, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            exec_sql_unlocked("ROLLBACK;");
            return false;
        }
        const int64_t ts = loc.verified_at_epoch_ms > 0 ? loc.verified_at_epoch_ms : now_epoch_ms();
        const int32_t ver = loc.version > 0 ? loc.version : 1;
        const std::string st = loc.location_status.empty() ? "STORED" : loc.location_status;

        sqlite3_bind_text(sg.stmt, 1, loc.shard_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, loc.peer_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 3, st.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 4, static_cast<sqlite3_int64>(ts));
        sqlite3_bind_int(sg.stmt, 5, ver);

        if (sqlite3_step(sg.stmt) != SQLITE_DONE) {
            exec_sql_unlocked("ROLLBACK;");
            return false;
        }
    }

    if (!file_id.empty() && new_file_version > 0) {
        const char* sql_ver =
            "UPDATE files SET version = CASE WHEN ?1 > version THEN ?1 ELSE version END "
            "WHERE file_id = ?2;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_ver, -1, &sg.stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_int(sg.stmt, 1, new_file_version);
            sqlite3_bind_text(sg.stmt, 2, file_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(sg.stmt);
        }
    }

    if (!origin_peer_id.empty()) {
        const std::string update_id = origin_peer_id + ":LOC:" + loc.shard_id + ":" +
                                      loc.peer_id + ":v" + std::to_string(loc.version);
        int64_t unique_seq = loc.version;
        for (unsigned char c : update_id) unique_seq = (unique_seq * 131) + c;
        unique_seq = (unique_seq & 0x7FFFFFFFFFFFFFFFLL);

        const char* sql_log =
            "INSERT OR IGNORE INTO metadata_sync_log ("
            "  update_id, origin_peer_id, origin_seq, entity_type, entity_key, payload_json, created_at_epoch_ms"
            ") VALUES (?1, ?2, ?3, 'SHARD_LOCATION', ?4, ?5, ?6);";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_log, -1, &sg.stmt, nullptr) == SQLITE_OK) {
            const std::string payload = loc.to_json().dump();
            sqlite3_bind_text(sg.stmt, 1, update_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(sg.stmt, 2, origin_peer_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(unique_seq));
            sqlite3_bind_text(sg.stmt, 4, loc.shard_id.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(sg.stmt, 5, payload.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(sg.stmt, 6, static_cast<sqlite3_int64>(now_epoch_ms()));
            sqlite3_step(sg.stmt);
        }
    }

    return exec_sql_unlocked("COMMIT;");
}

bool DatabaseManager::record_received_shard(const std::string& local_peer_id,
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
                                            const std::string& shard_sha256) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || local_peer_id.empty() || file_id.empty() || shard_id.empty() || shard_size == 0) {
        return false;
    }

    if (!exec_sql_unlocked("BEGIN IMMEDIATE TRANSACTION;")) return false;
    auto rollback_and_fail = [this]() -> bool {
        exec_sql_unlocked("ROLLBACK;");
        return false;
    };

    const int64_t now_ts = now_epoch_ms();

    // 1. Ensure parent file record exists (as UPLOADING, version 1 if not yet present)
    {
        const char* sql_f =
            "INSERT OR IGNORE INTO files ("
            "  file_id, original_name, file_size, sha256, shard_size, "
            "  data_shards_k, parity_shards_m, total_stripes, total_shards, "
            "  uploader_peer_id, uploaded_at_epoch_ms, status, version"
            ") VALUES (?1, ?2, ?3, ?4, ?5, 2, 1, ?6, ?7, ?8, ?9, 'UPLOADING', 1);";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_f, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }
        const std::string eff_name = original_name.empty() ? file_id : original_name;
        const std::string eff_up = uploader_peer_id.empty() ? local_peer_id : uploader_peer_id;
        sqlite3_bind_text(sg.stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, eff_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(file_size));
        sqlite3_bind_text(sg.stmt, 4, file_sha256.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 5, static_cast<sqlite3_int64>(shard_size));
        sqlite3_bind_int(sg.stmt, 6, total_stripes);
        sqlite3_bind_int(sg.stmt, 7, total_shards);
        sqlite3_bind_text(sg.stmt, 8, eff_up.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 9, static_cast<sqlite3_int64>(now_ts));
        if (sqlite3_step(sg.stmt) != SQLITE_DONE) return rollback_and_fail();
    }

    // 2. Ensure parent stripe record exists
    {
        const char* sql_st =
            "INSERT OR IGNORE INTO stripes (file_id, stripe_index, stripe_data_bytes, shard_size) "
            "VALUES (?1, ?2, ?3, ?4);";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_st, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }
        sqlite3_bind_text(sg.stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(sg.stmt, 2, stripe_index);
        sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(stripe_data_bytes));
        sqlite3_bind_int64(sg.stmt, 4, static_cast<sqlite3_int64>(shard_size));
        if (sqlite3_step(sg.stmt) != SQLITE_DONE) return rollback_and_fail();
    }

    // 3. Upsert shard record
    {
        const char* sql_sh =
            "INSERT INTO shards ("
            "  shard_id, file_id, stripe_index, shard_index, shard_type, "
            "  shard_size, original_bytes, sha256"
            ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) "
            "ON CONFLICT(shard_id) DO UPDATE SET "
            "  shard_size = excluded.shard_size, "
            "  original_bytes = excluded.original_bytes, "
            "  sha256 = excluded.sha256;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_sh, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }
        const std::string eff_type = (shard_type == "PARITY") ? "PARITY" : "DATA";
        sqlite3_bind_text(sg.stmt, 1, shard_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, file_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(sg.stmt, 3, stripe_index);
        sqlite3_bind_int(sg.stmt, 4, shard_index);
        sqlite3_bind_text(sg.stmt, 5, eff_type.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 6, static_cast<sqlite3_int64>(shard_size));
        sqlite3_bind_int64(sg.stmt, 7, static_cast<sqlite3_int64>(original_bytes));
        sqlite3_bind_text(sg.stmt, 8, shard_sha256.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(sg.stmt) != SQLITE_DONE) return rollback_and_fail();
    }

    // 4. Upsert shard_locations entry for local_peer_id
    {
        const char* sql_loc =
            "INSERT INTO shard_locations (shard_id, peer_id, location_status, verified_at_epoch_ms, version) "
            "VALUES (?1, ?2, 'STORED', ?3, 1) "
            "ON CONFLICT(shard_id, peer_id) DO UPDATE SET "
            "  location_status = 'STORED', "
            "  verified_at_epoch_ms = excluded.verified_at_epoch_ms;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_loc, -1, &sg.stmt, nullptr) != SQLITE_OK) {
            return rollback_and_fail();
        }
        sqlite3_bind_text(sg.stmt, 1, shard_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, local_peer_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(sg.stmt, 3, static_cast<sqlite3_int64>(now_ts));
        if (sqlite3_step(sg.stmt) != SQLITE_DONE) return rollback_and_fail();
    }

    return exec_sql_unlocked("COMMIT;");
}

bool DatabaseManager::load_file_manifest_unlocked(const std::string& file_id, FileManifest& out) const {
    if (!db_ || file_id.empty()) return false;

    const char* sql_file =
        "SELECT file_id, original_name, file_size, sha256, shard_size, "
        "       data_shards_k, parity_shards_m, total_stripes, total_shards, "
        "       uploader_peer_id, uploaded_at_epoch_ms, status, version "
        "FROM files WHERE file_id = ?1;";

    {
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_file, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;
        sqlite3_bind_text(sg.stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(sg.stmt) != SQLITE_ROW) return false;

        const unsigned char* fid = sqlite3_column_text(sg.stmt, 0);
        const unsigned char* name = sqlite3_column_text(sg.stmt, 1);
        const unsigned char* sha = sqlite3_column_text(sg.stmt, 3);
        const unsigned char* up_peer = sqlite3_column_text(sg.stmt, 9);
        const unsigned char* st_txt = sqlite3_column_text(sg.stmt, 11);

        out = FileManifest{};
        out.file_id = fid ? reinterpret_cast<const char*>(fid) : "";
        out.original_name = name ? reinterpret_cast<const char*>(name) : "";
        out.file_size = static_cast<uint64_t>(sqlite3_column_int64(sg.stmt, 2));
        out.sha256 = sha ? reinterpret_cast<const char*>(sha) : "";
        out.shard_size = static_cast<uint64_t>(sqlite3_column_int64(sg.stmt, 4));
        out.data_shards_k = sqlite3_column_int(sg.stmt, 5);
        out.parity_shards_m = sqlite3_column_int(sg.stmt, 6);
        out.total_stripes = sqlite3_column_int(sg.stmt, 7);
        out.total_shards = sqlite3_column_int(sg.stmt, 8);
        out.uploader_peer_id = up_peer ? reinterpret_cast<const char*>(up_peer) : "";
        out.uploaded_at_epoch_ms = static_cast<int64_t>(sqlite3_column_int64(sg.stmt, 10));
        out.status = st_txt ? reinterpret_cast<const char*>(st_txt) : "AVAILABLE";
        out.version = sqlite3_column_int(sg.stmt, 12);
    }

    // Load stripes
    {
        const char* sql_st =
            "SELECT stripe_index, stripe_data_bytes, shard_size "
            "FROM stripes WHERE file_id = ?1 ORDER BY stripe_index ASC;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql_st, -1, &sg.stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(sg.stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
            while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
                StripeManifestEntry st;
                st.file_id = file_id;
                st.stripe_index = sqlite3_column_int(sg.stmt, 0);
                st.stripe_data_bytes = static_cast<uint64_t>(sqlite3_column_int64(sg.stmt, 1));
                st.shard_size = static_cast<uint64_t>(sqlite3_column_int64(sg.stmt, 2));
                out.stripes.push_back(std::move(st));
            }
        }
    }

    // Load shards & shard_locations
    {
        const char* sql_sh =
            "SELECT shard_id, stripe_index, shard_index, shard_type, shard_size, original_bytes, sha256 "
            "FROM shards WHERE file_id = ?1 ORDER BY stripe_index ASC, shard_index ASC;";
        const char* sql_loc =
            "SELECT peer_id, location_status, verified_at_epoch_ms, version "
            "FROM shard_locations WHERE shard_id = ?1 ORDER BY peer_id ASC;";

        StmtGuard sg_sh;
        StmtGuard sg_loc;
        if (sqlite3_prepare_v2(db_, sql_sh, -1, &sg_sh.stmt, nullptr) == SQLITE_OK &&
            sqlite3_prepare_v2(db_, sql_loc, -1, &sg_loc.stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(sg_sh.stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
            while (sqlite3_step(sg_sh.stmt) == SQLITE_ROW) {
                ShardManifestEntry sh;
                const unsigned char* sid = sqlite3_column_text(sg_sh.stmt, 0);
                const unsigned char* stype = sqlite3_column_text(sg_sh.stmt, 3);
                const unsigned char* ssha = sqlite3_column_text(sg_sh.stmt, 6);

                sh.shard_id = sid ? reinterpret_cast<const char*>(sid) : "";
                sh.file_id = file_id;
                sh.stripe_index = sqlite3_column_int(sg_sh.stmt, 1);
                sh.shard_index = sqlite3_column_int(sg_sh.stmt, 2);
                sh.shard_type = stype ? reinterpret_cast<const char*>(stype) : "DATA";
                sh.shard_size = static_cast<uint64_t>(sqlite3_column_int64(sg_sh.stmt, 4));
                sh.original_bytes = static_cast<uint64_t>(sqlite3_column_int64(sg_sh.stmt, 5));
                sh.sha256 = ssha ? reinterpret_cast<const char*>(ssha) : "";

                sqlite3_reset(sg_loc.stmt);
                sqlite3_clear_bindings(sg_loc.stmt);
                sqlite3_bind_text(sg_loc.stmt, 1, sh.shard_id.c_str(), -1, SQLITE_TRANSIENT);
                while (sqlite3_step(sg_loc.stmt) == SQLITE_ROW) {
                    ShardLocationEntry loc;
                    const unsigned char* pid = sqlite3_column_text(sg_loc.stmt, 0);
                    const unsigned char* lst = sqlite3_column_text(sg_loc.stmt, 1);
                    loc.shard_id = sh.shard_id;
                    loc.peer_id = pid ? reinterpret_cast<const char*>(pid) : "";
                    loc.location_status = lst ? reinterpret_cast<const char*>(lst) : "STORED";
                    loc.verified_at_epoch_ms = static_cast<int64_t>(sqlite3_column_int64(sg_loc.stmt, 2));
                    loc.version = sqlite3_column_int(sg_loc.stmt, 3);
                    sh.locations.push_back(std::move(loc));
                }
                out.shards.push_back(std::move(sh));
            }
        }
    }

    return true;
}

bool DatabaseManager::get_file_manifest(const std::string& file_id, FileManifest& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return load_file_manifest_unlocked(file_id, out);
}

std::vector<FileManifest> DatabaseManager::list_file_manifests() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<FileManifest> results;
    if (!db_) return results;

    std::vector<std::string> ids;
    {
        const char* sql = "SELECT file_id FROM files ORDER BY file_id ASC;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return results;
        while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
            const unsigned char* txt = sqlite3_column_text(sg.stmt, 0);
            if (txt) ids.emplace_back(reinterpret_cast<const char*>(txt));
        }
    }

    results.reserve(ids.size());
    for (const auto& id : ids) {
        FileManifest m;
        if (load_file_manifest_unlocked(id, m)) {
            results.push_back(std::move(m));
        }
    }
    return results;
}

std::vector<FileManifest> DatabaseManager::search_file_manifests(const std::string& query) const {
    if (query.empty()) return list_file_manifests();

    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<FileManifest> results;
    if (!db_) return results;

    std::vector<std::string> ids;
    {
        const char* sql =
            "SELECT file_id FROM files "
            "WHERE file_id = ?1 OR LOWER(original_name) LIKE LOWER(?2) OR LOWER(file_id) LIKE LOWER(?2) "
            "ORDER BY file_id ASC;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return results;
        const std::string pattern = "%" + query + "%";
        sqlite3_bind_text(sg.stmt, 1, query.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(sg.stmt, 2, pattern.c_str(), -1, SQLITE_TRANSIENT);

        while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
            const unsigned char* txt = sqlite3_column_text(sg.stmt, 0);
            if (txt) ids.emplace_back(reinterpret_cast<const char*>(txt));
        }
    }

    results.reserve(ids.size());
    for (const auto& id : ids) {
        FileManifest m;
        if (load_file_manifest_unlocked(id, m)) {
            results.push_back(std::move(m));
        }
    }
    return results;
}

std::map<std::string, int32_t> DatabaseManager::get_metadata_summary() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, int32_t> summary;
    if (!db_) return summary;

    const char* sql = "SELECT file_id, version FROM files ORDER BY file_id ASC;";
    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return summary;

    while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        const unsigned char* fid = sqlite3_column_text(sg.stmt, 0);
        const int32_t ver = sqlite3_column_int(sg.stmt, 1);
        if (fid) summary[reinterpret_cast<const char*>(fid)] = ver;
    }
    return summary;
}

json::Json DatabaseManager::get_metadata_summary_json() const {
    const auto summary = get_metadata_summary();
    json::Json obj = json::Json::object();
    for (const auto& kv : summary) {
        obj[kv.first] = static_cast<long long>(kv.second);
    }
    return obj;
}

CatalogueEntry DatabaseManager::evaluate_catalogue_entry_unlocked(
    const FileManifest& manifest,
    const std::map<std::string, PeerHealth>& peer_health_map) const {
    CatalogueEntry entry;
    entry.manifest = manifest;
    entry.total_shards_count = static_cast<int32_t>(manifest.shards.size());
    entry.total_stripes = manifest.total_stripes > 0
                              ? manifest.total_stripes
                              : static_cast<int32_t>(manifest.stripes.size());

    std::set<std::string> all_peers_set;
    std::set<std::string> online_peers_set;
    std::map<int32_t, int32_t> online_shards_per_stripe;
    std::map<int32_t, int32_t> total_shards_per_stripe;

    for (const auto& sh : manifest.shards) {
        total_shards_per_stripe[sh.stripe_index]++;
        bool shard_online = false;
        for (const auto& loc : sh.locations) {
            if (loc.peer_id.empty()) continue;
            all_peers_set.insert(loc.peer_id);
            if (loc.location_status == "STORED") {
                auto it = peer_health_map.find(loc.peer_id);
                // If peer is known in local peers table, require ONLINE state;
                // if not yet in peers table, treat as unknown/offline for safety.
                const bool peer_is_online = (it != peer_health_map.end() && it->second == PeerHealth::ONLINE);
                if (peer_is_online) {
                    shard_online = true;
                    online_peers_set.insert(loc.peer_id);
                }
            }
        }
        if (shard_online) {
            entry.online_shards_count++;
            online_shards_per_stripe[sh.stripe_index]++;
        }
    }

    entry.hosting_peers.assign(all_peers_set.begin(), all_peers_set.end());
    entry.online_hosting_peers.assign(online_peers_set.begin(), online_peers_set.end());

    const int32_t k = manifest.data_shards_k > 0 ? manifest.data_shards_k : 2;
    const int32_t n = k + (manifest.parity_shards_m >= 0 ? manifest.parity_shards_m : 1);

    int32_t full_stripes = 0;
    int32_t rec_stripes = 0;
    const int32_t num_stripes = entry.total_stripes;

    for (int32_t s = 0; s < num_stripes; ++s) {
        const int32_t online_in_s = online_shards_per_stripe[s];
        if (online_in_s >= k) ++rec_stripes;
        if (online_in_s >= n) ++full_stripes;
    }

    entry.reconstructable_stripes = rec_stripes;
    if (num_stripes > 0 && rec_stripes == num_stripes) {
        entry.reconstructable = true;
        entry.live_status = (full_stripes == num_stripes) ? "AVAILABLE" : "DEGRADED";
    } else if (num_stripes == 0 && manifest.file_size == 0) {
        entry.reconstructable = true;
        entry.live_status = "AVAILABLE";
    } else {
        entry.reconstructable = false;
        entry.live_status = "UNRECOVERABLE";
    }

    return entry;
}

CatalogueEntry DatabaseManager::evaluate_catalogue_entry(const FileManifest& manifest) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, PeerHealth> health_map;
    if (db_) {
        const char* sql = "SELECT peer_id, health_state FROM peers;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
                const unsigned char* pid = sqlite3_column_text(sg.stmt, 0);
                const unsigned char* hst = sqlite3_column_text(sg.stmt, 1);
                if (pid && hst) {
                    health_map[reinterpret_cast<const char*>(pid)] =
                        peer_health_from_str(reinterpret_cast<const char*>(hst));
                }
            }
        }
    }
    return evaluate_catalogue_entry_unlocked(manifest, health_map);
}

std::vector<CatalogueEntry> DatabaseManager::query_catalogue(const std::string& query) const {
    const auto manifests = search_file_manifests(query);
    std::lock_guard<std::mutex> lock(mutex_);

    std::map<std::string, PeerHealth> health_map;
    if (db_) {
        const char* sql = "SELECT peer_id, health_state FROM peers;";
        StmtGuard sg;
        if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
                const unsigned char* pid = sqlite3_column_text(sg.stmt, 0);
                const unsigned char* hst = sqlite3_column_text(sg.stmt, 1);
                if (pid && hst) {
                    health_map[reinterpret_cast<const char*>(pid)] =
                        peer_health_from_str(reinterpret_cast<const char*>(hst));
                }
            }
        }
    }

    std::vector<CatalogueEntry> entries;
    entries.reserve(manifests.size());
    for (const auto& m : manifests) {
        entries.push_back(evaluate_catalogue_entry_unlocked(m, health_map));
    }
    return entries;
}

json::Json DatabaseManager::query_catalogue_json(const std::string& query) const {
    const auto entries = query_catalogue(query);
    json::Json arr = json::Json::array();
    for (const auto& e : entries) {
        arr.push_back(e.to_json());
    }
    return arr;
}

std::vector<MetadataSyncEntry> DatabaseManager::get_sync_log_entries(int64_t limit) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<MetadataSyncEntry> list;
    if (!db_) return list;

    const char* sql =
        "SELECT update_id, origin_peer_id, origin_seq, entity_type, entity_key, payload_json, created_at_epoch_ms "
        "FROM metadata_sync_log ORDER BY created_at_epoch_ms ASC LIMIT ?1;";
    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return list;
    sqlite3_bind_int64(sg.stmt, 1, static_cast<sqlite3_int64>(limit > 0 ? limit : 100));

    while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        MetadataSyncEntry e;
        const unsigned char* uid = sqlite3_column_text(sg.stmt, 0);
        const unsigned char* opid = sqlite3_column_text(sg.stmt, 1);
        const unsigned char* etype = sqlite3_column_text(sg.stmt, 3);
        const unsigned char* ekey = sqlite3_column_text(sg.stmt, 4);
        const unsigned char* pjson = sqlite3_column_text(sg.stmt, 5);

        e.update_id = uid ? reinterpret_cast<const char*>(uid) : "";
        e.origin_peer_id = opid ? reinterpret_cast<const char*>(opid) : "";
        e.origin_seq = static_cast<int64_t>(sqlite3_column_int64(sg.stmt, 2));
        e.entity_type = etype ? reinterpret_cast<const char*>(etype) : "FILE";
        e.entity_key = ekey ? reinterpret_cast<const char*>(ekey) : "";
        e.payload_json = pjson ? reinterpret_cast<const char*>(pjson) : "";
        e.created_at_epoch_ms = static_cast<int64_t>(sqlite3_column_int64(sg.stmt, 6));
        list.push_back(std::move(e));
    }
    return list;
}

bool DatabaseManager::upsert_recovery_record(const RecoveryRecordEntry& rec) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || rec.recovery_id.empty()) return false;

    const char* sql =
        "INSERT INTO recovery_records ("
        "  recovery_id, failed_peer_id, shard_id, file_id, stripe_index, "
        "  rebuilder_peer_id, target_peer_id, status, reason, timestamp_epoch_ms"
        ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) "
        "ON CONFLICT(recovery_id) DO UPDATE SET "
        "  failed_peer_id = excluded.failed_peer_id, "
        "  rebuilder_peer_id = excluded.rebuilder_peer_id, "
        "  target_peer_id = excluded.target_peer_id, "
        "  status = excluded.status, "
        "  reason = excluded.reason, "
        "  timestamp_epoch_ms = excluded.timestamp_epoch_ms;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;

    const int64_t ts = rec.timestamp_epoch_ms > 0 ? rec.timestamp_epoch_ms : now_epoch_ms();
    sqlite3_bind_text(sg.stmt, 1, rec.recovery_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 2, rec.failed_peer_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 3, rec.shard_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 4, rec.file_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(sg.stmt, 5, rec.stripe_index);
    sqlite3_bind_text(sg.stmt, 6, rec.rebuilder_peer_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 7, rec.target_peer_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 8, rec.status.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(sg.stmt, 9, rec.reason.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(sg.stmt, 10, static_cast<sqlite3_int64>(ts));

    return sqlite3_step(sg.stmt) == SQLITE_DONE;
}

bool DatabaseManager::get_recovery_record(const std::string& recovery_id, RecoveryRecordEntry& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || recovery_id.empty()) return false;

    const char* sql =
        "SELECT recovery_id, failed_peer_id, shard_id, file_id, stripe_index, "
        "       rebuilder_peer_id, target_peer_id, status, reason, timestamp_epoch_ms "
        "FROM recovery_records WHERE recovery_id = ?1;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_text(sg.stmt, 1, recovery_id.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(sg.stmt) != SQLITE_ROW) return false;

    const unsigned char* rid = sqlite3_column_text(sg.stmt, 0);
    const unsigned char* fpid = sqlite3_column_text(sg.stmt, 1);
    const unsigned char* sid = sqlite3_column_text(sg.stmt, 2);
    const unsigned char* fid = sqlite3_column_text(sg.stmt, 3);
    const unsigned char* rbpid = sqlite3_column_text(sg.stmt, 5);
    const unsigned char* tpid = sqlite3_column_text(sg.stmt, 6);
    const unsigned char* st = sqlite3_column_text(sg.stmt, 7);
    const unsigned char* rsn = sqlite3_column_text(sg.stmt, 8);

    out = RecoveryRecordEntry{};
    out.recovery_id = rid ? reinterpret_cast<const char*>(rid) : "";
    out.failed_peer_id = fpid ? reinterpret_cast<const char*>(fpid) : "";
    out.shard_id = sid ? reinterpret_cast<const char*>(sid) : "";
    out.file_id = fid ? reinterpret_cast<const char*>(fid) : "";
    out.stripe_index = sqlite3_column_int(sg.stmt, 4);
    out.rebuilder_peer_id = rbpid ? reinterpret_cast<const char*>(rbpid) : "";
    out.target_peer_id = tpid ? reinterpret_cast<const char*>(tpid) : "";
    out.status = st ? reinterpret_cast<const char*>(st) : "";
    out.reason = rsn ? reinterpret_cast<const char*>(rsn) : "";
    out.timestamp_epoch_ms = static_cast<int64_t>(sqlite3_column_int64(sg.stmt, 9));
    return true;
}

std::vector<RecoveryRecordEntry> DatabaseManager::get_recovery_records(const std::string& file_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<RecoveryRecordEntry> list;
    if (!db_) return list;

    const char* sql = file_id.empty()
        ? "SELECT recovery_id, failed_peer_id, shard_id, file_id, stripe_index, "
          "       rebuilder_peer_id, target_peer_id, status, reason, timestamp_epoch_ms "
          "FROM recovery_records ORDER BY timestamp_epoch_ms ASC;"
        : "SELECT recovery_id, failed_peer_id, shard_id, file_id, stripe_index, "
          "       rebuilder_peer_id, target_peer_id, status, reason, timestamp_epoch_ms "
          "FROM recovery_records WHERE file_id = ?1 ORDER BY timestamp_epoch_ms ASC;";

    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql, -1, &sg.stmt, nullptr) != SQLITE_OK) return list;
    if (!file_id.empty()) {
        sqlite3_bind_text(sg.stmt, 1, file_id.c_str(), -1, SQLITE_TRANSIENT);
    }

    while (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        RecoveryRecordEntry r;
        const unsigned char* rid = sqlite3_column_text(sg.stmt, 0);
        const unsigned char* fpid = sqlite3_column_text(sg.stmt, 1);
        const unsigned char* sid = sqlite3_column_text(sg.stmt, 2);
        const unsigned char* fid = sqlite3_column_text(sg.stmt, 3);
        const unsigned char* rbpid = sqlite3_column_text(sg.stmt, 5);
        const unsigned char* tpid = sqlite3_column_text(sg.stmt, 6);
        const unsigned char* st = sqlite3_column_text(sg.stmt, 7);
        const unsigned char* rsn = sqlite3_column_text(sg.stmt, 8);

        r.recovery_id = rid ? reinterpret_cast<const char*>(rid) : "";
        r.failed_peer_id = fpid ? reinterpret_cast<const char*>(fpid) : "";
        r.shard_id = sid ? reinterpret_cast<const char*>(sid) : "";
        r.file_id = fid ? reinterpret_cast<const char*>(fid) : "";
        r.stripe_index = sqlite3_column_int(sg.stmt, 4);
        r.rebuilder_peer_id = rbpid ? reinterpret_cast<const char*>(rbpid) : "";
        r.target_peer_id = tpid ? reinterpret_cast<const char*>(tpid) : "";
        r.status = st ? reinterpret_cast<const char*>(st) : "";
        r.reason = rsn ? reinterpret_cast<const char*>(rsn) : "";
        r.timestamp_epoch_ms = static_cast<int64_t>(sqlite3_column_int64(sg.stmt, 9));
        list.push_back(std::move(r));
    }
    return list;
}

int64_t DatabaseManager::count_rows(const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_) return 0;

    // Allow-list valid schema tables to prevent any SQL injection
    if (table_name != "peers" && table_name != "local_config" &&
        table_name != "files" && table_name != "stripes" &&
        table_name != "shards" && table_name != "shard_locations" &&
        table_name != "metadata_sync_log" && table_name != "recovery_records") {
        return 0;
    }

    const std::string sql = "SELECT COUNT(*) FROM " + table_name + ";";
    StmtGuard sg;
    if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &sg.stmt, nullptr) != SQLITE_OK) return 0;
    if (sqlite3_step(sg.stmt) == SQLITE_ROW) {
        return static_cast<int64_t>(sqlite3_column_int64(sg.stmt, 0));
    }
    return 0;
}
