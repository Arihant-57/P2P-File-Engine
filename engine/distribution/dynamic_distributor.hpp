// ============================================================================
// dynamic_distributor.hpp - Dynamic Peer Selection & Shard Distribution (E3 / Task 3)
//
// Dynamically selects distinct, healthy, capable peers per RS(3,2) stripe:
//   - Filter by ONLINE health state
//   - Filter by available disk capacity
//   - Filter by transfer workload (concurrent active transfers vs capacity)
//   - Balances shard counts across peers & rotates across stripes
//   - Sends raw binary shard bytes over TCP (never JSON-encoded strings)
//   - Verifies SHA-256 acknowledgements before committing locations
//   - Retries missing/failed placements onto alternative eligible peers
//   - Integrates with per-peer SQLite DatabaseManager and FileManifest (Task 3)
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/file_manifest.hpp"
#include "../../models/peer.hpp"
#include "../catalog/catalog_manager.hpp"
#include "../db/database_manager.hpp"
#include "../registry/peer_registry.hpp"

struct P2PUploadOptions {
    std::string custom_file_id;
    std::string fail_first_store_on_peer;       // Simulate peer becoming unavailable mid-upload
    bool corrupt_first_hash_on_peer = false;    // Simulate invalid shard SHA-256 on first store attempt
    bool simulate_interrupted_transfer = false; // Simulate interrupted TCP shard transfer on first attempt
    int duplicate_store_count = 0;              // Send duplicate STORE_CHUNK requests to verify idempotency
    uint64_t override_min_free_bytes = 0;       // Simulate insufficient storage threshold
};

struct P2PUploadResult {
    bool success = false;
    std::string file_id;
    std::string status = "UNRECOVERABLE"; // AVAILABLE, DEGRADED, UNRECOVERABLE
    std::string error_message;
    std::string placement_log;
    FileManifest manifest;
    uint32_t retries_performed = 0;
    uint32_t duplicate_acks_verified = 0;
};

class DynamicDistributor {
public:
    explicit DynamicDistributor(PeerRegistry& registry);

    // Gate-2 compatibility: Distribute shards to dynamically chosen distinct peers.
    FileDistributionStatus distribute_file(const std::string& file_id,
                                           std::vector<ShardMeta>& shards,
                                           const std::string& staging_dir,
                                           std::string& log_out);

    // Phase-2 Task-3: Decentralized file upload, adaptive chunking, RS(3,2) encoding,
    // 3-distinct-peer-per-stripe dynamic selection, direct TCP shard transfer with
    // bounded retry/failover, and transactional SQLite FileManifest persistence.
    P2PUploadResult upload_and_distribute_p2p(const std::string& uploader_peer_id,
                                              const std::string& source_file_path,
                                              const std::string& original_filename,
                                              const std::string& temp_staging_dir,
                                              DatabaseManager& db,
                                              const P2PUploadOptions& opts = {});

private:
    PeerRegistry& registry_;

    bool push_shard(const PeerInfo& peer,
                    const ShardMeta& shard,
                    const std::string& shard_file_path,
                    std::string& error_out);

    bool push_shard_p2p(const PeerInfo& peer,
                        const FileManifest& manifest,
                        const StripeManifestEntry& stripe,
                        const ShardManifestEntry& shard,
                        const std::string& shard_file_path,
                        bool corrupt_hash,
                        bool truncate_payload,
                        uint64_t min_free_bytes,
                        std::string& error_out);
};
