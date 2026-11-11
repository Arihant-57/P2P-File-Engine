// ============================================================================
// file_manifest.hpp - Distributed File Manifest & Catalogue Data Models (Task 2)
//
// Defines the canonical decentralized file manifest structure persisted across
// each peer's SQLite tables (files, stripes, shards, shard_locations,
// metadata_sync_log, recovery_records) and exchanged over TCP during
// peer-to-peer metadata synchronization.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../third_party/mini_json.hpp"
#include "chunk.hpp"

struct ShardLocationEntry {
    std::string shard_id;
    std::string peer_id;
    std::string location_status = "STORED";  // STORED, STALE, CORRUPTED, RECOVERING
    int64_t verified_at_epoch_ms = 0;
    int32_t version = 1;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["shard_id"] = shard_id;
        j["peer_id"] = peer_id;
        j["location_status"] = location_status;
        j["verified_at_epoch_ms"] = static_cast<long long>(verified_at_epoch_ms);
        j["version"] = static_cast<long long>(version);
        return j;
    }

    static ShardLocationEntry from_json(const json::Json& j) {
        ShardLocationEntry e;
        if (j.contains("shard_id")) e.shard_id = j["shard_id"].as_string();
        if (j.contains("peer_id")) e.peer_id = j["peer_id"].as_string();
        if (j.contains("location_status")) e.location_status = j["location_status"].as_string();
        if (j.contains("verified_at_epoch_ms"))
            e.verified_at_epoch_ms = static_cast<int64_t>(j["verified_at_epoch_ms"].as_int());
        if (j.contains("version"))
            e.version = static_cast<int32_t>(j["version"].as_int());
        return e;
    }
};

struct ShardManifestEntry {
    std::string shard_id;          // e.g. "F001-S000-D0"
    std::string file_id;           // e.g. "F001"
    int32_t stripe_index = 0;      // 0-based stripe order
    int32_t shard_index = 0;       // 0 = D0, 1 = D1, 2 = P0
    std::string shard_type = "DATA"; // "DATA" or "PARITY"
    uint64_t shard_size = 0;       // padded RS shard size in bytes
    uint64_t original_bytes = 0;   // unpadded payload bytes in this shard
    std::string sha256;            // SHA-256 hex digest of the binary shard
    std::vector<ShardLocationEntry> locations;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["shard_id"] = shard_id;
        j["file_id"] = file_id;
        j["stripe_index"] = static_cast<long long>(stripe_index);
        j["shard_index"] = static_cast<long long>(shard_index);
        j["shard_type"] = shard_type;
        j["shard_size"] = static_cast<long long>(shard_size);
        j["original_bytes"] = static_cast<long long>(original_bytes);
        j["sha256"] = sha256;
        json::Json locs = json::Json::array();
        for (const auto& loc : locations) locs.push_back(loc.to_json());
        j["locations"] = locs;
        return j;
    }

    static ShardManifestEntry from_json(const json::Json& j) {
        ShardManifestEntry s;
        if (j.contains("shard_id")) s.shard_id = j["shard_id"].as_string();
        if (j.contains("file_id")) s.file_id = j["file_id"].as_string();
        if (j.contains("stripe_index")) s.stripe_index = static_cast<int32_t>(j["stripe_index"].as_int());
        if (j.contains("shard_index")) s.shard_index = static_cast<int32_t>(j["shard_index"].as_int());
        if (j.contains("shard_type")) s.shard_type = j["shard_type"].as_string();
        if (j.contains("shard_size")) s.shard_size = static_cast<uint64_t>(j["shard_size"].as_int());
        if (j.contains("original_bytes")) s.original_bytes = static_cast<uint64_t>(j["original_bytes"].as_int());
        if (j.contains("sha256")) s.sha256 = j["sha256"].as_string();
        if (j.contains("locations") && j["locations"].is_array()) {
            for (const auto& item : j["locations"].items()) {
                ShardLocationEntry loc = ShardLocationEntry::from_json(item);
                if (loc.shard_id.empty()) loc.shard_id = s.shard_id;
                s.locations.push_back(std::move(loc));
            }
        }
        return s;
    }
};

struct StripeManifestEntry {
    std::string file_id;
    int32_t stripe_index = 0;
    uint64_t stripe_data_bytes = 0;
    uint64_t shard_size = 0;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["file_id"] = file_id;
        j["stripe_index"] = static_cast<long long>(stripe_index);
        j["stripe_data_bytes"] = static_cast<long long>(stripe_data_bytes);
        j["shard_size"] = static_cast<long long>(shard_size);
        return j;
    }

    static StripeManifestEntry from_json(const json::Json& j) {
        StripeManifestEntry st;
        if (j.contains("file_id")) st.file_id = j["file_id"].as_string();
        if (j.contains("stripe_index")) st.stripe_index = static_cast<int32_t>(j["stripe_index"].as_int());
        if (j.contains("stripe_data_bytes"))
            st.stripe_data_bytes = static_cast<uint64_t>(j["stripe_data_bytes"].as_int());
        if (j.contains("shard_size"))
            st.shard_size = static_cast<uint64_t>(j["shard_size"].as_int());
        return st;
    }
};

struct FileManifest {
    std::string file_id;
    std::string original_name;
    uint64_t file_size = 0;
    std::string sha256;
    uint64_t shard_size = 65536;
    int32_t data_shards_k = 2;
    int32_t parity_shards_m = 1;
    int32_t total_stripes = 0;
    int32_t total_shards = 0;
    std::string uploader_peer_id;
    int64_t uploaded_at_epoch_ms = 0;
    std::string status = "AVAILABLE"; // UPLOADING, AVAILABLE, DEGRADED, UNRECOVERABLE
    int32_t version = 1;

    std::vector<StripeManifestEntry> stripes;
    std::vector<ShardManifestEntry> shards;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["file_id"] = file_id;
        j["original_name"] = original_name;
        j["file_size"] = static_cast<long long>(file_size);
        j["sha256"] = sha256;
        j["shard_size"] = static_cast<long long>(shard_size);
        j["data_shards_k"] = static_cast<long long>(data_shards_k);
        j["parity_shards_m"] = static_cast<long long>(parity_shards_m);
        j["total_stripes"] = static_cast<long long>(total_stripes);
        j["total_shards"] = static_cast<long long>(total_shards);
        j["uploader_peer_id"] = uploader_peer_id;
        j["uploaded_at_epoch_ms"] = static_cast<long long>(uploaded_at_epoch_ms);
        j["status"] = status;
        j["version"] = static_cast<long long>(version);

        json::Json st_arr = json::Json::array();
        for (const auto& st : stripes) st_arr.push_back(st.to_json());
        j["stripes"] = st_arr;

        json::Json sh_arr = json::Json::array();
        for (const auto& sh : shards) sh_arr.push_back(sh.to_json());
        j["shards"] = sh_arr;
        return j;
    }

    static FileManifest from_json(const json::Json& j) {
        FileManifest m;
        if (j.contains("file_id")) m.file_id = j["file_id"].as_string();
        if (j.contains("original_name")) m.original_name = j["original_name"].as_string();
        if (j.contains("file_size")) m.file_size = static_cast<uint64_t>(j["file_size"].as_int());
        if (j.contains("sha256")) m.sha256 = j["sha256"].as_string();
        if (j.contains("shard_size")) m.shard_size = static_cast<uint64_t>(j["shard_size"].as_int());
        if (j.contains("data_shards_k")) m.data_shards_k = static_cast<int32_t>(j["data_shards_k"].as_int());
        if (j.contains("parity_shards_m")) m.parity_shards_m = static_cast<int32_t>(j["parity_shards_m"].as_int());
        if (j.contains("total_stripes")) m.total_stripes = static_cast<int32_t>(j["total_stripes"].as_int());
        if (j.contains("total_shards")) m.total_shards = static_cast<int32_t>(j["total_shards"].as_int());
        if (j.contains("uploader_peer_id")) m.uploader_peer_id = j["uploader_peer_id"].as_string();
        if (j.contains("uploaded_at_epoch_ms"))
            m.uploaded_at_epoch_ms = static_cast<int64_t>(j["uploaded_at_epoch_ms"].as_int());
        if (j.contains("status")) m.status = j["status"].as_string();
        if (j.contains("version")) m.version = static_cast<int32_t>(j["version"].as_int());

        if (j.contains("stripes") && j["stripes"].is_array()) {
            for (const auto& item : j["stripes"].items()) {
                StripeManifestEntry st = StripeManifestEntry::from_json(item);
                if (st.file_id.empty()) st.file_id = m.file_id;
                m.stripes.push_back(std::move(st));
            }
        }
        if (j.contains("shards") && j["shards"].is_array()) {
            for (const auto& item : j["shards"].items()) {
                ShardManifestEntry sh = ShardManifestEntry::from_json(item);
                if (sh.file_id.empty()) sh.file_id = m.file_id;
                m.shards.push_back(std::move(sh));
            }
        }
        return m;
    }
};

struct MetadataSyncEntry {
    std::string update_id;
    std::string origin_peer_id;
    int64_t origin_seq = 0;
    std::string entity_type = "FILE"; // FILE, SHARD_LOCATION, PEER, RECOVERY
    std::string entity_key;
    std::string payload_json;
    int64_t created_at_epoch_ms = 0;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["update_id"] = update_id;
        j["origin_peer_id"] = origin_peer_id;
        j["origin_seq"] = static_cast<long long>(origin_seq);
        j["entity_type"] = entity_type;
        j["entity_key"] = entity_key;
        j["payload_json"] = payload_json;
        j["created_at_epoch_ms"] = static_cast<long long>(created_at_epoch_ms);
        return j;
    }

    static MetadataSyncEntry from_json(const json::Json& j) {
        MetadataSyncEntry e;
        if (j.contains("update_id")) e.update_id = j["update_id"].as_string();
        if (j.contains("origin_peer_id")) e.origin_peer_id = j["origin_peer_id"].as_string();
        if (j.contains("origin_seq")) e.origin_seq = static_cast<int64_t>(j["origin_seq"].as_int());
        if (j.contains("entity_type")) e.entity_type = j["entity_type"].as_string();
        if (j.contains("entity_key")) e.entity_key = j["entity_key"].as_string();
        if (j.contains("payload_json")) e.payload_json = j["payload_json"].as_string();
        if (j.contains("created_at_epoch_ms"))
            e.created_at_epoch_ms = static_cast<int64_t>(j["created_at_epoch_ms"].as_int());
        return e;
    }
};

struct RecoveryRecordEntry {
    std::string recovery_id;
    std::string failed_peer_id;
    std::string shard_id;
    std::string file_id;
    int32_t stripe_index = 0;
    std::string rebuilder_peer_id;
    std::string target_peer_id;
    std::string status = "COMPLETED"; // STARTED, COMPLETED, FAILED, UNRECOVERABLE
    std::string reason;
    int64_t timestamp_epoch_ms = 0;

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["recovery_id"] = recovery_id;
        j["failed_peer_id"] = failed_peer_id;
        j["shard_id"] = shard_id;
        j["file_id"] = file_id;
        j["stripe_index"] = static_cast<long long>(stripe_index);
        j["rebuilder_peer_id"] = rebuilder_peer_id;
        j["target_peer_id"] = target_peer_id;
        j["status"] = status;
        j["reason"] = reason;
        j["timestamp_epoch_ms"] = static_cast<long long>(timestamp_epoch_ms);
        return j;
    }

    static RecoveryRecordEntry from_json(const json::Json& j) {
        RecoveryRecordEntry r;
        if (j.contains("recovery_id")) r.recovery_id = j["recovery_id"].as_string();
        if (j.contains("failed_peer_id")) r.failed_peer_id = j["failed_peer_id"].as_string();
        if (j.contains("shard_id")) r.shard_id = j["shard_id"].as_string();
        if (j.contains("file_id")) r.file_id = j["file_id"].as_string();
        if (j.contains("stripe_index")) r.stripe_index = static_cast<int32_t>(j["stripe_index"].as_int());
        if (j.contains("rebuilder_peer_id")) r.rebuilder_peer_id = j["rebuilder_peer_id"].as_string();
        if (j.contains("target_peer_id")) r.target_peer_id = j["target_peer_id"].as_string();
        if (j.contains("status")) r.status = j["status"].as_string();
        if (j.contains("reason")) r.reason = j["reason"].as_string();
        if (j.contains("timestamp_epoch_ms"))
            r.timestamp_epoch_ms = static_cast<int64_t>(j["timestamp_epoch_ms"].as_int());
        return r;
    }
};

// Catalogue availability view accounting for live peer health states (ONLINE vs OFFLINE)
struct CatalogueEntry {
    FileManifest manifest;
    bool reconstructable = false;
    std::string live_status = "UNRECOVERABLE"; // AVAILABLE, DEGRADED, UNRECOVERABLE
    int32_t online_shards_count = 0;
    int32_t total_shards_count = 0;
    int32_t reconstructable_stripes = 0;
    int32_t total_stripes = 0;
    std::vector<std::string> hosting_peers;
    std::vector<std::string> online_hosting_peers;

    json::Json to_json() const {
        json::Json j = manifest.to_json();
        j["reconstructable"] = reconstructable;
        j["live_status"] = live_status;
        j["online_shards_count"] = static_cast<long long>(online_shards_count);
        j["total_shards_count"] = static_cast<long long>(total_shards_count);
        j["reconstructable_stripes"] = static_cast<long long>(reconstructable_stripes);

        json::Json hp = json::Json::array();
        for (const auto& p : hosting_peers) hp.push_back(p);
        j["hosting_peers"] = hp;

        json::Json ohp = json::Json::array();
        for (const auto& p : online_hosting_peers) ohp.push_back(p);
        j["online_hosting_peers"] = ohp;
        return j;
    }
};
