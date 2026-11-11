// ============================================================================
// catalog_manager.hpp - Central Staging & Multi-File Catalog Engine (E6)
//
// Manages:
//   - Permanent central original staging: storage/central/<file_id>_<filename>
//   - Atomic per-file JSON manifests: metadata/files/<file_id>.json
//   - Global catalogue index: metadata/catalog.json
//   - File lifecycle states: STORED, DISTRIBUTING, AVAILABLE, DEGRADED, RECOVERING, UNRECOVERABLE
//   - Thread-safe per-file job locking
// ============================================================================
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../third_party/mini_json.hpp"

enum class FileDistributionStatus {
    STORED,          // Staged centrally, not yet distributed
    DISTRIBUTING,    // Shard distribution in progress
    AVAILABLE,       // All RS shards committed & protected across distinct peers
    DEGRADED,        // Reconstructable (>= 2 shards), but 1 shard offline
    RECOVERING,      // Durability repair in progress
    UNRECOVERABLE    // Insufficient surviving peer shards and no central backup
};

inline const char* file_status_str(FileDistributionStatus s) {
    switch (s) {
        case FileDistributionStatus::STORED:        return "STORED";
        case FileDistributionStatus::DISTRIBUTING:  return "DISTRIBUTING";
        case FileDistributionStatus::AVAILABLE:     return "AVAILABLE";
        case FileDistributionStatus::DEGRADED:      return "DEGRADED";
        case FileDistributionStatus::RECOVERING:    return "RECOVERING";
        case FileDistributionStatus::UNRECOVERABLE: return "UNRECOVERABLE";
        default:                                    return "UNKNOWN";
    }
}

inline FileDistributionStatus file_status_from_str(const std::string& str) {
    if (str == "STORED")        return FileDistributionStatus::STORED;
    if (str == "DISTRIBUTING")  return FileDistributionStatus::DISTRIBUTING;
    if (str == "AVAILABLE")     return FileDistributionStatus::AVAILABLE;
    if (str == "DEGRADED")      return FileDistributionStatus::DEGRADED;
    if (str == "RECOVERING")    return FileDistributionStatus::RECOVERING;
    if (str == "UNRECOVERABLE") return FileDistributionStatus::UNRECOVERABLE;
    return FileDistributionStatus::STORED;
}

struct FileRecord {
    std::string file_id;
    std::string filename;
    uint64_t    size = 0;
    std::string sha256;
    FileDistributionStatus status = FileDistributionStatus::STORED;
    std::string central_path;
    std::string upload_time;
    uint64_t    shard_size = 0;
    uint32_t    k = 2;
    uint32_t    m = 1;
    uint64_t    stripes_count = 0;
    std::vector<ShardMeta> shards;

    json::Json to_json() const;
    static FileRecord from_json(const json::Json& j);
};

class CatalogManager {
public:
    explicit CatalogManager(std::string base_metadata_dir = "metadata",
                            std::string base_storage_dir = "storage");

    // Copy original file to central staging, allocate unique File ID, record SHA-256,
    // and initialize manifest with status STORED.
    bool stage_file(const std::string& original_file_path,
                    FileRecord& record_out,
                    std::string& error_out);

    bool stage_bytes(const std::string& filename,
                     const std::vector<uint8_t>& bytes,
                     FileRecord& record_out,
                     std::string& error_out);

    bool get_file(const std::string& file_id, FileRecord& out) const;
    std::vector<FileRecord> list_files() const;
    std::vector<FileRecord> search_files(const std::string& query) const;

    // Update manifest and catalog status (atomic JSON rewrite)
    bool update_shards_and_status(const std::string& file_id,
                                  FileDistributionStatus status,
                                  const std::vector<ShardMeta>& shards,
                                  uint64_t shard_size);

    std::string central_storage_dir() const { return central_dir_; }

    // Per-file lock guard to prevent concurrent duplicate distribution
    std::mutex& get_file_mutex(const std::string& file_id);

private:
    std::string meta_dir_;
    std::string files_dir_;
    std::string central_dir_;
    std::string catalog_path_;
    std::string counter_path_;

    mutable std::mutex catalog_mu_;
    mutable std::mutex file_mutexes_mu_;
    std::map<std::string, std::unique_ptr<std::mutex>> file_mutexes_;

    std::string allocate_file_id();
    void persist_catalog_index(const std::vector<FileRecord>& records);
    void save_file_manifest(const FileRecord& record);
};
