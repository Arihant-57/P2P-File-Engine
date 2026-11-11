// ============================================================================
// client_download.hpp - Direct Peer-to-Client Download & Local RS Reconstruction (E5)
//
// Protocol flow:
//   1. Client queries Coordinator for file metadata & shard peer endpoints.
//   2. Coordinator triggers first-demand RS(3,2) distribution if STORED.
//   3. Client connects DIRECTLY to storage peers over TCP (Coordinator does NOT
//      relay the shard payload).
//   4. Client retrieves any 2 valid shards per stripe, checks SHA-256 integrity.
//   5. Client performs local Reed-Solomon reconstruction.
//   6. Supports explicit central staging fallback: --source central.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/file_manifest.hpp"
#include "../catalog/catalog_manager.hpp"
#include "../coordinator/coordinator_service.hpp"

enum class DownloadSource {
    PEERS,   // Normal multi-peer direct retrieval with RS(3,2) local decode
    CENTRAL  // Explicit central staging fallback (--source central)
};

struct DownloadResult {
    bool success = false;
    std::string file_id;
    std::string filename;
    uint64_t file_size = 0;
    uint64_t downloaded_bytes = 0;
    std::string expected_sha256;
    std::string actual_sha256;
    std::string output_path;
    DownloadSource source_used = DownloadSource::PEERS;
    size_t stripes_total = 0;
    size_t stripes_reconstructed = 0;
    size_t shards_retrieved = 0;
    size_t failed_peer_connections = 0;
    size_t corrupted_shards_rejected = 0;
    std::string retrieval_log;
    std::string message;
};

class ClientDownload {
public:
    ClientDownload(std::string coordinator_address = "127.0.0.1",
                   uint16_t coordinator_port = 5000,
                   std::string output_dir = "output");

    // Execute Gate-2 download workflow
    DownloadResult download(const std::string& file_id,
                            DownloadSource source = DownloadSource::PEERS,
                            CoordinatorService* in_proc_coordinator = nullptr);

    // Phase-2 Task-4: Decentralized P2P file search, direct shard retrieval & RS(3,2) reconstruction
    // Contacts any participating peer (`coord_addr_:coord_port_`) to resolve the manifest and peer
    // endpoints, then fetches any 2 valid shards per stripe directly from storage peers over TCP.
    DownloadResult download_p2p(const std::string& file_id_or_name,
                                const std::string& explicit_output_path = "");

    // Reconstruct a file directly from a resolved FileManifest and known peer endpoints
    static DownloadResult reconstruct_from_manifest_p2p(const FileManifest& manifest,
                                                        const std::vector<PeerInfo>& peers,
                                                        const std::string& final_output_path,
                                                        const std::string& local_peer_id = "",
                                                        const std::string& local_storage_dir = "");

private:
    std::string coord_addr_;
    uint16_t coord_port_;
    std::string output_dir_;

    bool fetch_shard_from_peer(const std::string& peer_addr,
                               uint16_t peer_port,
                               const std::string& shard_id,
                               uint64_t expected_size,
                               const std::string& expected_sha256,
                               const std::string& save_path,
                               bool& integrity_failed_out,
                               std::string& error_out);
};
