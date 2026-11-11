// ============================================================================
// client_download.cpp - Direct Peer-to-Client Download & Local RS Reconstruction
// ============================================================================
#include "client_download.hpp"

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>

#include "../chunking/chunk_manager.hpp"
#include "../coding/adaptive_chunker.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"
#include "../distribution/dynamic_distributor.hpp"

namespace fs = std::filesystem;

ClientDownload::ClientDownload(std::string coordinator_address,
                               uint16_t coordinator_port,
                               std::string output_dir)
    : coord_addr_(std::move(coordinator_address)),
      coord_port_(coordinator_port),
      output_dir_(std::move(output_dir)) {}

bool ClientDownload::fetch_shard_from_peer(const std::string& peer_addr,
                                          uint16_t peer_port,
                                          const std::string& shard_id,
                                          uint64_t expected_size,
                                          const std::string& expected_sha256,
                                          const std::string& save_path,
                                          bool& integrity_failed_out,
                                          std::string& error_out) {
    integrity_failed_out = false;
    TCPClient client(2500, 2500);
    if (!client.connect_to(peer_addr, peer_port)) {
        error_out = "connection refused to peer at " + peer_addr + ":" + std::to_string(peer_port);
        return false;
    }

    json::Json req = json::Json::object();
    req["type"] = proto::GET_CHUNK;
    req["chunk_id"] = shard_id;

    json::Json resp;
    std::vector<uint8_t> payload;
    if (client.request(req, {}, resp, payload) != proto::RecvStatus::OK) {
        error_out = "network failure or interrupted stream retrieving shard " + shard_id;
        return false;
    }

    if (resp["type"].as_string() != proto::CHUNK_RESPONSE) {
        error_out = "peer rejected shard request: " +
                    (resp.contains("reason") ? resp["reason"].as_string() : "unknown error");
        return false;
    }

    if (expected_size > 0 && payload.size() != expected_size) {
        integrity_failed_out = true;
        error_out = "shard size mismatch for " + shard_id + " (got " +
                    std::to_string(payload.size()) + "B, expected " +
                    std::to_string(expected_size) + "B)";
        return false;
    }

    // Verify SHA-256 before accepting
    const std::string actual_sha = ChunkManager::sha256_of_bytes(payload);
    if (!expected_sha256.empty() && actual_sha != expected_sha256) {
        integrity_failed_out = true;
        error_out = "integrity error: SHA-256 mismatch for shard " + shard_id;
        return false;
    }

    // Write to staging
    std::ofstream out(fs::u8path(save_path), std::ios::binary | std::ios::trunc);
    if (!out) {
        error_out = "failed to create local file: " + save_path;
        return false;
    }
    if (!payload.empty()) {
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    }
    return static_cast<bool>(out);
}

DownloadResult ClientDownload::download(const std::string& file_id,
                                        DownloadSource source,
                                        CoordinatorService* in_proc_coordinator) {
    DownloadResult res;
    res.file_id = file_id;
    res.source_used = source;

    FileRecord rec;
    std::vector<PeerInfo> peers;

    // Retrieve file manifest & cluster peers
    if (in_proc_coordinator) {
        // In-process optimization for tests
        auto& catalog = in_proc_coordinator->catalog();
        std::lock_guard<std::mutex> file_lock(catalog.get_file_mutex(file_id));

        if (!catalog.get_file(file_id, rec)) {
            res.message = "file ID not found in catalogue: " + file_id;
            return res;
        }

        // On-demand first distribution if file is STORED
        if (rec.status == FileDistributionStatus::STORED) {
            std::cout << "[Coordinator] File " << file_id << " on-demand first distribution initiated...\n";
            AdaptiveChunker chunker;
            uint64_t chosen_size = 0;
            const std::string staging_dir = "output/staging/" + file_id;
            rec.shards = chunker.encode_file(file_id, rec.central_path, staging_dir, chosen_size);

            DynamicDistributor distributor(in_proc_coordinator->registry());
            std::string dist_log;
            rec.status = distributor.distribute_file(file_id, rec.shards, staging_dir, dist_log);
            std::cout << dist_log;

            catalog.update_shards_and_status(file_id, rec.status, rec.shards, chosen_size);
            catalog.get_file(file_id, rec);
        }
        peers = in_proc_coordinator->registry().get_all_peers();
    } else {
        // TCP query to Coordinator over wire
        TCPClient client(3000, 3000);
        if (!client.connect_to(coord_addr_, coord_port_)) {
            res.message = "cannot connect to Coordinator at " + coord_addr_ + ":" + std::to_string(coord_port_);
            return res;
        }

        json::Json req = json::Json::object();
        req["type"] = "DOWNLOAD_MANIFEST";
        req["file_id"] = file_id;

        json::Json resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
            !resp.contains("manifest")) {
            res.message = "failed to obtain file manifest from Coordinator";
            return res;
        }

        rec = FileRecord::from_json(resp["manifest"]);
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& pi : resp["peers"].items()) {
                peers.push_back(PeerInfo::from_json(pi));
            }
        }
    }

    res.filename = rec.filename;
    res.file_size = rec.size;
    res.expected_sha256 = rec.sha256;
    res.stripes_total = rec.stripes_count;

    std::error_code ec;
    fs::create_directories(output_dir_, ec);
    const std::string final_out_path = output_dir_ + "/" + rec.filename;
    res.output_path = final_out_path;

    // Path A: Explicit Central Fallback (--source central)
    if (source == DownloadSource::CENTRAL) {
        if (!fs::exists(rec.central_path)) {
            res.message = "central original file is missing at: " + rec.central_path;
            return res;
        }
        fs::copy_file(rec.central_path, final_out_path, fs::copy_options::overwrite_existing, ec);
        res.actual_sha256 = ChunkManager::sha256_of_file(final_out_path);
        res.success = (res.actual_sha256 == res.expected_sha256);
        res.message = "File retrieved directly from Coordinator central staging (fallback mode)";
        return res;
    }

    // Path B: Multi-Peer Direct Shard Retrieval with Local RS(3,2) Reconstruction
    std::map<std::string, PeerInfo> peer_map;
    for (const auto& p : peers) peer_map[p.id] = p;

    const std::string local_shards_dir = output_dir_ + "/shards_" + file_id;
    fs::create_directories(local_shards_dir, ec);

    // Group shards by stripe
    std::map<std::string, std::vector<ShardMeta>> stripes;
    for (const auto& sh : rec.shards) {
        stripes[sh.stripe_id].push_back(sh);
    }

    size_t total_retrieved = 0;

    for (const auto& kv : stripes) {
        const std::string& stripe_id = kv.first;
        const auto& stripe_shards = kv.second;

        size_t valid_in_stripe = 0;

        for (const auto& sm : stripe_shards) {
            // Need only 2 valid shards per stripe for RS(3,2) reconstruction!
            if (valid_in_stripe >= 2) break;

            if (sm.peers.empty()) continue;
            const std::string& peer_id = sm.peers[0];
            auto it = peer_map.find(peer_id);
            if (it == peer_map.end()) continue;

            const std::string save_file = local_shards_dir + "/" + sm.chunk_id + ".chunk";
            std::string err;
            bool integrity_bad = false;
            // DIRECT TCP retrieval from storage peer (Coordinator never relays bytes)
            if (fetch_shard_from_peer(it->second.address, it->second.port, sm.chunk_id,
                                      sm.size, sm.sha256, save_file, integrity_bad, err)) {
                ++valid_in_stripe;
                ++total_retrieved;
                res.downloaded_bytes += sm.size;
            } else {
                if (integrity_bad) ++res.corrupted_shards_rejected;
                else ++res.failed_peer_connections;
            }
        }

        if (valid_in_stripe < 2) {
            res.message = "insufficient surviving peer shards for stripe " + stripe_id +
                          " (retrieved " + std::to_string(valid_in_stripe) + "/2 required)";
            return res;
        }
        ++res.stripes_reconstructed;
    }

    res.shards_retrieved = total_retrieved;

    // Local Reed-Solomon Reconstruction directly on Client
    AdaptiveChunker chunker;
    std::string rec_err;
    if (!chunker.reconstruct_file(file_id, rec.size, rec.shards, local_shards_dir, final_out_path, rec_err)) {
        res.message = "Reed-Solomon client reconstruction failed: " + rec_err;
        return res;
    }

    res.actual_sha256 = ChunkManager::sha256_of_file(final_out_path);
    if (res.actual_sha256 != res.expected_sha256) {
        res.message = "reconstructed file SHA-256 mismatch (actual=" + res.actual_sha256 +
                      " expected=" + res.expected_sha256 + ")";
        return res;
    }

    // Cleanup local temporary shard files
    fs::remove_all(local_shards_dir, ec);

    res.success = true;
    res.message = "Direct peer-to-client download and RS(3,2) reconstruction complete. Integrity verified.";
    return res;
}

DownloadResult ClientDownload::reconstruct_from_manifest_p2p(
    const FileManifest& manifest,
    const std::vector<PeerInfo>& peers,
    const std::string& final_output_path,
    const std::string& local_peer_id,
    const std::string& local_storage_dir) {
    DownloadResult res;
    res.file_id = manifest.file_id;
    res.filename = manifest.original_name;
    res.file_size = manifest.file_size;
    res.expected_sha256 = manifest.sha256;
    res.stripes_total = manifest.total_stripes > 0
                            ? static_cast<size_t>(manifest.total_stripes)
                            : manifest.stripes.size();
    res.output_path = final_output_path;

    std::ostringstream log_ss;
    if (manifest.file_id.empty() || manifest.shards.empty()) {
        res.message = "UNRECOVERABLE: manifest for '" + manifest.file_id + "' contains no shard metadata";
        return res;
    }

    std::map<std::string, PeerInfo> peer_map;
    for (const auto& p : peers) {
        peer_map[p.id] = p;
    }

    static std::atomic<uint64_t> s_dl_seq{0};
    const uint64_t seq = ++s_dl_seq;
    const fs::path out_p = fs::u8path(final_output_path);
    const fs::path parent_dir = out_p.has_parent_path() ? out_p.parent_path() : fs::path("output");

    std::error_code ec;
    fs::create_directories(parent_dir, ec);

    const std::string staging_shards_dir =
        (parent_dir / (".rfe_dl_shards_" + manifest.file_id + "_" + std::to_string(seq))).u8string();
    const std::string temp_out_path =
        final_output_path + "." + std::to_string(seq) + ".tmp";

    fs::create_directories(fs::u8path(staging_shards_dir), ec);

    auto cleanup_staging = [&]() {
        std::error_code c_ec;
        fs::remove_all(fs::u8path(staging_shards_dir), c_ec);
        fs::remove(fs::u8path(temp_out_path), c_ec);
    };

    // Group shards by stripe_index in ascending stripe order
    std::map<int32_t, std::vector<ShardManifestEntry>> stripes_map;
    for (const auto& sh : manifest.shards) {
        stripes_map[sh.stripe_index].push_back(sh);
    }

    if (stripes_map.size() != res.stripes_total && res.stripes_total > 0) {
        cleanup_staging();
        res.message = "UNRECOVERABLE: manifest stripe count mismatch";
        return res;
    }

    std::vector<ShardMeta> chunker_manifest;
    chunker_manifest.reserve(manifest.shards.size());
    uint64_t seq_num = 1;

    ClientDownload helper;

    for (auto& kv : stripes_map) {
        const int32_t stripe_idx = kv.first;
        auto& stripe_shards = kv.second;
        std::sort(stripe_shards.begin(), stripe_shards.end(),
                  [](const ShardManifestEntry& a, const ShardManifestEntry& b) {
                      return a.shard_index < b.shard_index;
                  });

        std::ostringstream st_id_ss;
        st_id_ss << "S" << std::setfill('0') << std::setw(3) << stripe_idx;
        const std::string stripe_id_str = st_id_ss.str();

        size_t valid_in_stripe = 0;

        for (const auto& sh : stripe_shards) {
            ShardMeta sm;
            sm.chunk_id = sh.shard_id;
            sm.file_id = manifest.file_id;
            sm.filename = manifest.original_name;
            sm.stripe_id = stripe_id_str;
            sm.shard_type = sh.shard_type;
            sm.sequence = seq_num++;
            sm.size = sh.shard_size > 0 ? sh.shard_size : manifest.shard_size;
            sm.original_data_length = sh.original_bytes;
            sm.padding_length = (sh.shard_type == "DATA" && sm.size >= sh.original_bytes)
                                    ? (sm.size - sh.original_bytes)
                                    : 0;
            sm.sha256 = sh.sha256;
            for (const auto& loc : sh.locations) {
                if (!loc.peer_id.empty()) sm.peers.push_back(loc.peer_id);
            }
            chunker_manifest.push_back(sm);

            // RS(3,2) requires any 2 valid shards per stripe; once 2 are verified, skip remaining shard
            if (valid_in_stripe >= static_cast<size_t>(manifest.data_shards_k > 0 ? manifest.data_shards_k : 2)) {
                continue;
            }

            const std::string staged_shard_path = staging_shards_dir + "/" + sh.shard_id + ".chunk";
            bool fetched_this_shard = false;

            // Try each known storage location for this shard (with fallback to other known peers if needed)
            std::vector<std::string> candidate_peer_ids;
            for (const auto& loc : sh.locations) {
                if (!loc.peer_id.empty() && loc.location_status == "STORED") {
                    candidate_peer_ids.push_back(loc.peer_id);
                }
            }
            // Only try non-manifest peers if all STORED manifest locations for this shard are empty
            const size_t primary_loc_count = candidate_peer_ids.size();

            // Also allow probing other known peers in case shard was relocated
            for (const auto& p : peers) {
                if (std::find(candidate_peer_ids.begin(), candidate_peer_ids.end(), p.id) ==
                    candidate_peer_ids.end()) {
                    candidate_peer_ids.push_back(p.id);
                }
            }

            for (size_t c_idx = 0; c_idx < candidate_peer_ids.size(); ++c_idx) {
                if (c_idx >= primary_loc_count) break; // stick to recorded locations unless empty
                const std::string& pid = candidate_peer_ids[c_idx];

                // Fast path if the contacted peer itself holds the shard on local disk
                if (!local_peer_id.empty() && pid == local_peer_id && !local_storage_dir.empty()) {
                    const std::string local_chunk = local_storage_dir + "/" + sh.shard_id + ".chunk";
                    if (fs::exists(fs::u8path(local_chunk), ec)) {
                        const uint64_t lsz = static_cast<uint64_t>(fs::file_size(fs::u8path(local_chunk), ec));
                        const std::string lsha = ChunkManager::sha256_of_file(local_chunk);
                        if (lsz == sm.size && lsha == sh.sha256) {
                            fs::copy_file(fs::u8path(local_chunk), fs::u8path(staged_shard_path),
                                          fs::copy_options::overwrite_existing, ec);
                            if (!ec) {
                                fetched_this_shard = true;
                                ++valid_in_stripe;
                                ++res.shards_retrieved;
                                res.downloaded_bytes += sm.size;
                                log_ss << "  [P2P-Download] Verified local shard " << sh.shard_id
                                       << " on " << pid << " (SHA-256 OK)\n";
                                break;
                            }
                        } else {
                            ++res.corrupted_shards_rejected;
                            log_ss << "  [P2P-Download] Rejected corrupted local shard " << sh.shard_id
                                   << " on " << pid << " (SHA-256/size mismatch)\n";
                            continue;
                        }
                    }
                }

                auto it = peer_map.find(pid);
                if (it == peer_map.end() || it->second.port == 0) {
                    ++res.failed_peer_connections;
                    log_ss << "  [P2P-Download] Storage peer " << pid << " endpoint unknown for "
                           << sh.shard_id << "\n";
                    continue;
                }

                std::string fetch_err;
                bool integrity_bad = false;
                if (helper.fetch_shard_from_peer(it->second.address, it->second.port,
                                                 sh.shard_id, sm.size, sh.sha256,
                                                 staged_shard_path, integrity_bad, fetch_err)) {
                    fetched_this_shard = true;
                    ++valid_in_stripe;
                    ++res.shards_retrieved;
                    res.downloaded_bytes += sm.size;
                    log_ss << "  [P2P-Download] Retrieved & verified " << sh.shard_id
                           << " from Peer " << pid << " (" << it->second.address << ":"
                           << it->second.port << ")\n";
                    break;
                } else {
                    if (integrity_bad) {
                        ++res.corrupted_shards_rejected;
                        log_ss << "  [P2P-Download] REJECTED corrupted shard " << sh.shard_id
                               << " from Peer " << pid << ": " << fetch_err << "\n";
                    } else {
                        ++res.failed_peer_connections;
                        log_ss << "  [P2P-Download] Failed to fetch " << sh.shard_id
                               << " from Peer " << pid << ": " << fetch_err << "\n";
                    }
                }
            }
            (void)fetched_this_shard;
        }

        const size_t required_k = static_cast<size_t>(manifest.data_shards_k > 0 ? manifest.data_shards_k : 2);
        if (valid_in_stripe < required_k) {
            cleanup_staging();
            res.retrieval_log = log_ss.str();
            res.message = "UNRECOVERABLE: insufficient valid shards for stripe " + stripe_id_str +
                          " (retrieved " + std::to_string(valid_in_stripe) + "/" +
                          std::to_string(required_k) + " required)";
            return res;
        }
        ++res.stripes_reconstructed;
    }

    // Reconstruct to temporary output file first (Task 4.5: never publish unverified partial output)
    AdaptiveChunker chunker(AdaptivePolicy{},
                            static_cast<size_t>(manifest.data_shards_k > 0 ? manifest.data_shards_k : 2),
                            static_cast<size_t>(manifest.parity_shards_m > 0 ? manifest.parity_shards_m : 1));
    std::string rec_err;
    if (!chunker.reconstruct_file(manifest.file_id, manifest.file_size,
                                  chunker_manifest, staging_shards_dir, temp_out_path, rec_err)) {
        cleanup_staging();
        res.retrieval_log = log_ss.str();
        res.message = "UNRECOVERABLE: Reed-Solomon reconstruction failed: " + rec_err;
        return res;
    }

    // Verify full-file SHA-256 and exact byte size before publishing final output file
    const uint64_t reconstructed_size = static_cast<uint64_t>(fs::file_size(fs::u8path(temp_out_path), ec));
    res.actual_sha256 = ChunkManager::sha256_of_file(temp_out_path);
    if (ec || reconstructed_size != manifest.file_size || res.actual_sha256 != manifest.sha256) {
        cleanup_staging();
        res.retrieval_log = log_ss.str();
        res.message = "UNRECOVERABLE: reconstructed full-file SHA-256 or size mismatch (actual=" +
                      res.actual_sha256 + ", expected=" + manifest.sha256 + ")";
        return res;
    }

    // Atomically commit verified temporary output file to final destination path
    fs::rename(fs::u8path(temp_out_path), out_p, ec);
    if (ec) {
        fs::remove(out_p, ec);
        ec.clear();
        fs::rename(fs::u8path(temp_out_path), out_p, ec);
        if (ec) {
            cleanup_staging();
            res.retrieval_log = log_ss.str();
            res.message = "failed to commit verified output file to destination: " + final_output_path;
            return res;
        }
    }

    fs::remove_all(fs::u8path(staging_shards_dir), ec);
    res.retrieval_log = log_ss.str();
    res.success = true;
    res.message = "Decentralized P2P download and RS(3,2) reconstruction complete. Full-file SHA-256 verified.";
    return res;
}

DownloadResult ClientDownload::download_p2p(const std::string& file_id_or_name,
                                            const std::string& explicit_output_path) {
    DownloadResult res;
    res.file_id = file_id_or_name;

    // 1. Contact the entry peer (`coord_addr_:coord_port_`) to resolve the file manifest
    //    (including on-demand P2P metadata sync if local metadata is missing/stale)
    json::Json cat_resp;
    {
        TCPClient client(3000, 5000);
        if (!client.connect_to(coord_addr_, coord_port_)) {
            res.message = "cannot connect to peer at " + coord_addr_ + ":" + std::to_string(coord_port_);
            return res;
        }
        json::Json req = json::Json::object();
        req["type"] = proto::P2P_CATALOGUE_QUERY;
        req["query"] = file_id_or_name;
        req["sync_if_missing"] = true;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, cat_resp, pl) != proto::RecvStatus::OK ||
            cat_resp["type"].as_string() != proto::P2P_CATALOGUE_QUERY_ACK ||
            !cat_resp.contains("files") || !cat_resp["files"].is_array()) {
            res.message = "failed to query distributed catalogue on peer " +
                          coord_addr_ + ":" + std::to_string(coord_port_);
            return res;
        }
    }

    const auto& items = cat_resp["files"].items();
    if (items.empty()) {
        res.message = "file '" + file_id_or_name + "' not found in distributed catalogue";
        return res;
    }

    // Prefer exact file_id match first; otherwise check exact filename match
    std::vector<FileManifest> exact_id_matches;
    std::vector<FileManifest> exact_name_matches;
    std::vector<FileManifest> all_matches;
    for (const auto& item : items) {
        FileManifest m = FileManifest::from_json(item);
        if (m.file_id == file_id_or_name) exact_id_matches.push_back(m);
        if (m.original_name == file_id_or_name) exact_name_matches.push_back(m);
        all_matches.push_back(std::move(m));
    }

    FileManifest chosen;
    if (exact_id_matches.size() == 1) {
        chosen = exact_id_matches[0];
    } else if (!exact_name_matches.empty()) {
        if (exact_name_matches.size() > 1) {
            std::ostringstream amb;
            amb << "ambiguous filename '" << file_id_or_name << "' matches multiple File IDs (specify exact File ID): ";
            for (size_t i = 0; i < exact_name_matches.size(); ++i) {
                if (i > 0) amb << ", ";
                amb << exact_name_matches[i].file_id;
            }
            res.message = amb.str();
            return res;
        }
        chosen = exact_name_matches[0];
    } else if (all_matches.size() == 1) {
        chosen = all_matches[0];
    } else {
        std::ostringstream amb;
        amb << "ambiguous query '" << file_id_or_name << "' matches multiple File IDs (specify exact File ID): ";
        for (size_t i = 0; i < all_matches.size(); ++i) {
            if (i > 0) amb << ", ";
            amb << all_matches[i].file_id << " (" << all_matches[i].original_name << ")";
        }
        res.message = amb.str();
        return res;
    }

    // 2. Query peer membership table from the contacted peer to resolve peer_id -> ip:port
    std::vector<PeerInfo> peers;
    {
        TCPClient client(3000, 3000);
        if (!client.connect_to(coord_addr_, coord_port_)) {
            res.message = "lost connection to peer at " + coord_addr_ + ":" + std::to_string(coord_port_);
            return res;
        }
        json::Json req = json::Json::object();
        req["type"] = proto::P2P_QUERY_PEERS;
        json::Json peers_resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, peers_resp, pl) == proto::RecvStatus::OK &&
            peers_resp.contains("peers") && peers_resp["peers"].is_array()) {
            for (const auto& p_item : peers_resp["peers"].items()) {
                peers.push_back(PeerInfo::from_json(p_item));
            }
        }
    }

    // Determine final output path (sanitize original_name to prevent path traversal)
    std::string safe_orig_name = fs::u8path(chosen.original_name).filename().u8string();
    if (safe_orig_name.empty() || safe_orig_name == "." || safe_orig_name == "..") {
        safe_orig_name = chosen.file_id + ".bin";
    }
    std::string final_out = explicit_output_path;
    if (final_out.empty()) {
        std::error_code ec;
        fs::create_directories(output_dir_, ec);
        final_out = output_dir_ + "/" + safe_orig_name;
    } else {
        std::error_code ec;
        const fs::path p = fs::u8path(final_out);
        if (fs::is_directory(p, ec)) {
            final_out = (p / fs::u8path(safe_orig_name)).u8string();
        }
    }

    // 3. Directly retrieve shards from storage peers over TCP and reconstruct locally
    return reconstruct_from_manifest_p2p(chosen, peers, final_out);
}
