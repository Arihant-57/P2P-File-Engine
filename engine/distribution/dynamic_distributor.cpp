// ============================================================================
// dynamic_distributor.cpp - Dynamic Peer Selection & Shard Distribution (E3 / Task 3)
// ============================================================================
#include "dynamic_distributor.hpp"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

#include "../chunking/chunk_manager.hpp"
#include "../coding/adaptive_chunker.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"

namespace fs = std::filesystem;

DynamicDistributor::DynamicDistributor(PeerRegistry& registry) : registry_(registry) {}

bool DynamicDistributor::push_shard(const PeerInfo& peer,
                                    const ShardMeta& shard,
                                    const std::string& shard_file_path,
                                    std::string& error_out) {
    std::ifstream in(shard_file_path, std::ios::binary);
    if (!in) {
        error_out = "staged shard file not found: " + shard_file_path;
        return false;
    }

    std::vector<uint8_t> payload((std::istreambuf_iterator<char>(in)),
                                 std::istreambuf_iterator<char>());

    if (payload.size() != shard.size) {
        error_out = "shard size mismatch on disk: " + std::to_string(payload.size()) +
                    " vs expected " + std::to_string(shard.size);
        return false;
    }

    TCPClient client(3000, 3000);
    if (!client.connect_to(peer.address, peer.port)) {
        error_out = "failed to connect to peer " + peer.id + " (" + peer.address + ":" +
                    std::to_string(peer.port) + ")";
        return false;
    }

    json::Json req = json::Json::object();
    req["type"] = proto::STORE_CHUNK;
    req["chunk_id"] = shard.chunk_id;
    req["sha256"] = shard.sha256;

    json::Json resp;
    std::vector<uint8_t> resp_payload;
    auto status = client.request(req, payload, resp, resp_payload);
    if (status != proto::RecvStatus::OK) {
        error_out = "transfer error to peer " + peer.id;
        return false;
    }

    if (resp["type"].as_string() != proto::STORE_ACK) {
        error_out = "peer " + peer.id + " rejected shard store: " +
                    (resp.contains("reason") ? resp["reason"].as_string() : "unknown reason");
        return false;
    }

    return true;
}

FileDistributionStatus DynamicDistributor::distribute_file(const std::string& /*file_id*/,
                                                           std::vector<ShardMeta>& shards,
                                                           const std::string& staging_dir,
                                                           std::string& log_out) {
    std::ostringstream ss_log;
    auto online_peers = registry_.get_online_peers();
    if (online_peers.empty()) {
        log_out = "cannot distribute: 0 peers are online";
        return FileDistributionStatus::STORED;
    }

    std::map<std::string, std::vector<size_t>> stripes;
    for (size_t i = 0; i < shards.size(); ++i) {
        stripes[shards[i].stripe_id].push_back(i);
    }

    size_t stripe_index = 0;
    bool all_stripes_fully_placed = true;
    bool all_stripes_recoverable = true;

    for (const auto& kv : stripes) {
        (void)kv.first;
        const auto& shard_indices = kv.second;

        std::vector<PeerInfo> eligible;
        for (const auto& p : online_peers) {
            if (p.workload.is_busy()) continue;
            uint64_t req_space = shards[shard_indices[0]].size;
            if (p.workload.available_storage_bytes > 0 &&
                p.workload.available_storage_bytes < req_space) {
                continue;
            }
            eligible.push_back(p);
        }

        if (eligible.empty()) {
            eligible = online_peers;
        }

        std::sort(eligible.begin(), eligible.end(), [](const PeerInfo& a, const PeerInfo& b) {
            if (a.workload.active_transfers != b.workload.active_transfers)
                return a.workload.active_transfers < b.workload.active_transfers;
            return a.workload.available_storage_bytes > b.workload.available_storage_bytes;
        });

        if (eligible.size() > 1) {
            size_t rot = stripe_index % eligible.size();
            std::rotate(eligible.begin(), eligible.begin() + rot, eligible.end());
        }

        std::vector<std::string> used_peer_ids;
        size_t stored_in_stripe = 0;

        for (size_t s_idx : shard_indices) {
            ShardMeta& shard = shards[s_idx];
            const std::string shard_path = staging_dir + "/" + shard.chunk_id + ".chunk";

            bool placed = false;
            for (const auto& candidate : eligible) {
                if (std::find(used_peer_ids.begin(), used_peer_ids.end(), candidate.id) != used_peer_ids.end()) {
                    continue;
                }

                std::string err;
                if (push_shard(candidate, shard, shard_path, err)) {
                    shard.peers = {candidate.id};
                    used_peer_ids.push_back(candidate.id);
                    placed = true;
                    ++stored_in_stripe;
                    ss_log << "  [E3] Placed " << shard.chunk_id << " -> Peer " << candidate.id << "\n";
                    break;
                } else {
                    ss_log << "  [E3] Failed to store " << shard.chunk_id << " on Peer " << candidate.id
                           << ": " << err << " (retrying alternative)\n";
                }
            }

            if (!placed) {
                ss_log << "  [E3] Shortfall: could not place shard " << shard.chunk_id << " on any peer\n";
            }
        }

        if (stored_in_stripe < 3) all_stripes_fully_placed = false;
        if (stored_in_stripe < 2) all_stripes_recoverable = false;

        ++stripe_index;
    }

    log_out = ss_log.str();

    if (all_stripes_fully_placed) return FileDistributionStatus::AVAILABLE;
    if (all_stripes_recoverable)  return FileDistributionStatus::DEGRADED;
    return FileDistributionStatus::STORED;
}

// ============================================================================
// Phase-2 Task-3: Decentralized Upload & Direct Binary Shard Placement
// ============================================================================

bool DynamicDistributor::push_shard_p2p(const PeerInfo& peer,
                                        const FileManifest& manifest,
                                        const StripeManifestEntry& stripe,
                                        const ShardManifestEntry& shard,
                                        const std::string& shard_file_path,
                                        bool corrupt_hash,
                                        bool truncate_payload,
                                        uint64_t min_free_bytes,
                                        std::string& error_out) {
    std::ifstream in(fs::u8path(shard_file_path), std::ios::binary);
    if (!in) {
        error_out = "staged shard file not found: " + shard_file_path;
        return false;
    }

    std::vector<uint8_t> payload((std::istreambuf_iterator<char>(in)),
                                 std::istreambuf_iterator<char>());

    if (payload.size() != shard.shard_size) {
        error_out = "shard size mismatch on disk: " + std::to_string(payload.size()) +
                    " vs expected " + std::to_string(shard.shard_size);
        return false;
    }

    if (truncate_payload && payload.size() > 1) {
        payload.resize(payload.size() / 2);
    }

    TCPClient client(3000, 3000);
    if (!client.connect_to(peer.address, peer.port)) {
        error_out = "failed to connect to peer " + peer.id + " (" + peer.address + ":" +
                    std::to_string(peer.port) + ")";
        return false;
    }

    json::Json req = json::Json::object();
    req["type"] = proto::STORE_CHUNK;
    req["chunk_id"] = shard.shard_id;
    req["sha256"] = corrupt_hash ? ("0000000000000000" + shard.sha256.substr(16)) : shard.sha256;
    req["expected_size"] = static_cast<long long>(shard.shard_size);
    if (min_free_bytes > 0) {
        req["require_min_free_bytes"] = static_cast<long long>(min_free_bytes);
    }

    // Include relational shard metadata so destination peer can record the shard
    // in its own SQLite database immediately upon atomic file commit (Task 3.5).
    req["file_id"] = manifest.file_id;
    req["original_name"] = manifest.original_name;
    req["file_size"] = static_cast<long long>(manifest.file_size);
    req["file_sha256"] = manifest.sha256;
    req["total_stripes"] = static_cast<long long>(manifest.total_stripes);
    req["total_shards"] = static_cast<long long>(manifest.total_shards);
    req["uploader_peer_id"] = manifest.uploader_peer_id;
    req["stripe_index"] = static_cast<long long>(shard.stripe_index);
    req["stripe_data_bytes"] = static_cast<long long>(stripe.stripe_data_bytes);
    req["shard_index"] = static_cast<long long>(shard.shard_index);
    req["shard_type"] = shard.shard_type;
    req["original_bytes"] = static_cast<long long>(shard.original_bytes);

    json::Json resp;
    std::vector<uint8_t> resp_payload;
    const auto status = client.request(req, payload, resp, resp_payload);
    if (status != proto::RecvStatus::OK) {
        error_out = "TCP transfer interrupted or timed out to peer " + peer.id;
        return false;
    }

    if (resp["type"].as_string() != proto::STORE_ACK || resp["status"].as_string() != "OK") {
        error_out = "peer " + peer.id + " rejected shard store: " +
                    (resp.contains("reason") ? resp["reason"].as_string() : "unknown reason");
        return false;
    }

    if (resp.contains("sha256") && resp["sha256"].as_string() != shard.sha256) {
        error_out = "peer " + peer.id + " returned mismatched STORE_ACK sha256";
        return false;
    }

    return true;
}

P2PUploadResult DynamicDistributor::upload_and_distribute_p2p(
    const std::string& uploader_peer_id,
    const std::string& source_file_path,
    const std::string& original_filename,
    const std::string& temp_staging_dir,
    DatabaseManager& db,
    const P2PUploadOptions& opts) {
    P2PUploadResult res;
    std::ostringstream ss_log;

    std::error_code ec;
    if (!fs::is_regular_file(fs::u8path(source_file_path), ec)) {
        res.error_message = "input file does not exist or is not a regular file: " + source_file_path;
        return res;
    }

    const uint64_t file_size = static_cast<uint64_t>(fs::file_size(fs::u8path(source_file_path), ec));
    const std::string file_sha256 = ChunkManager::sha256_of_file(source_file_path);
    if (file_sha256.empty()) {
        res.error_message = "failed to compute SHA-256 of input file: " + source_file_path;
        return res;
    }

    std::string file_id = opts.custom_file_id;
    if (file_id.empty()) {
        std::string prefix = file_sha256.substr(0, 8);
        for (char& c : prefix) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        file_id = "F_" + prefix;
    }
    res.file_id = file_id;

    const std::string eff_filename = original_filename.empty()
                                         ? fs::u8path(source_file_path).filename().string()
                                         : original_filename;

    AdaptiveChunker chunker;
    const uint64_t expected_shard_size = chunker.calculate_shard_size(file_size);

    // Pre-verify at least 3 distinct online peers with sufficient storage are available
    {
        const auto online_now = registry_.get_online_peers();
        size_t capable_count = 0;
        for (const auto& p : online_now) {
            if (p.workload.is_busy()) continue;
            if (p.workload.available_storage_bytes > 0 &&
                p.workload.available_storage_bytes < expected_shard_size) {
                continue;
            }
            if (opts.override_min_free_bytes > 0 &&
                p.workload.available_storage_bytes < opts.override_min_free_bytes) {
                continue;
            }
            ++capable_count;
        }
        if (capable_count < 3) {
            res.status = "UNRECOVERABLE";
            res.error_message =
                "cannot achieve required RS(3,2) placement: need at least 3 distinct eligible online peers with sufficient storage, have " +
                std::to_string(capable_count) + " (total online=" + std::to_string(online_now.size()) + ")";
            return res;
        }
    }

    // 1. Adaptive chunking & RS(3,2) encoding into temp_staging_dir
    fs::remove_all(fs::u8path(temp_staging_dir), ec);
    fs::create_directories(fs::u8path(temp_staging_dir), ec);

    uint64_t chosen_shard_size = 0;
    std::vector<ShardMeta> encoded_shards;
    try {
        encoded_shards = chunker.encode_file(file_id, source_file_path, temp_staging_dir, chosen_shard_size);
    } catch (const std::exception& e) {
        res.error_message = std::string("adaptive chunking / RS(3,2) encoding failed: ") + e.what();
        fs::remove_all(fs::u8path(temp_staging_dir), ec);
        return res;
    }

    // 2. Build initial FileManifest in UPLOADING state (version 1)
    FileManifest manifest;
    manifest.file_id = file_id;
    manifest.original_name = eff_filename;
    manifest.file_size = file_size;
    manifest.sha256 = file_sha256;
    manifest.shard_size = chosen_shard_size;
    manifest.data_shards_k = 2;
    manifest.parity_shards_m = 1;
    manifest.uploader_peer_id = uploader_peer_id;
    manifest.uploaded_at_epoch_ms = DatabaseManager::now_epoch_ms();
    manifest.status = "UPLOADING";
    manifest.version = 1;

    std::map<int32_t, uint64_t> stripe_data_bytes_map;
    std::map<int32_t, std::vector<size_t>> stripe_to_shard_indices;

    for (const auto& sm : encoded_shards) {
        int32_t stripe_idx = 0;
        if (sm.stripe_id.size() > 1 && sm.stripe_id[0] == 'S') {
            try {
                stripe_idx = std::stoi(sm.stripe_id.substr(1));
            } catch (...) {
                stripe_idx = 0;
            }
        }
        int32_t shard_idx = 0;
        if (sm.chunk_id.rfind("-D0") != std::string::npos) shard_idx = 0;
        else if (sm.chunk_id.rfind("-D1") != std::string::npos) shard_idx = 1;
        else if (sm.chunk_id.rfind("-P0") != std::string::npos) shard_idx = 2;

        if (sm.shard_type == "DATA") {
            stripe_data_bytes_map[stripe_idx] += sm.original_data_length;
        } else {
            stripe_data_bytes_map.try_emplace(stripe_idx, 0);
        }

        ShardManifestEntry entry;
        entry.shard_id = sm.chunk_id;
        entry.file_id = file_id;
        entry.stripe_index = stripe_idx;
        entry.shard_index = shard_idx;
        entry.shard_type = sm.shard_type;
        entry.shard_size = sm.size;
        entry.original_bytes = sm.original_data_length;
        entry.sha256 = sm.sha256;

        const size_t pos = manifest.shards.size();
        manifest.shards.push_back(std::move(entry));
        stripe_to_shard_indices[stripe_idx].push_back(pos);
    }

    for (const auto& kv : stripe_data_bytes_map) {
        StripeManifestEntry st;
        st.file_id = file_id;
        st.stripe_index = kv.first;
        st.stripe_data_bytes = kv.second;
        st.shard_size = chosen_shard_size;
        manifest.stripes.push_back(st);
    }

    manifest.total_stripes = static_cast<int32_t>(manifest.stripes.size());
    manifest.total_shards = static_cast<int32_t>(manifest.shards.size());

    // Record UPLOADING state in local SQLite before transferring shards
    db.upsert_file_manifest(manifest, uploader_peer_id);

    // 3. Distribute each stripe's 3 shards across 3 distinct eligible online peers
    std::map<std::string, uint32_t> peer_shard_counts;
    bool did_fail_peer = false;
    bool did_corrupt_hash = false;
    bool did_interrupt_transfer = false;
    bool did_duplicate_store = false;

    for (size_t st_i = 0; st_i < manifest.stripes.size(); ++st_i) {
        const StripeManifestEntry& st_meta = manifest.stripes[st_i];
        const auto& shard_positions = stripe_to_shard_indices[st_meta.stripe_index];

        auto online_peers = registry_.get_online_peers();
        std::vector<PeerInfo> eligible;
        for (const auto& p : online_peers) {
            if (p.workload.is_busy()) continue;
            if (p.workload.available_storage_bytes > 0 &&
                p.workload.available_storage_bytes < chosen_shard_size) {
                continue;
            }
            if (opts.override_min_free_bytes > 0 &&
                p.workload.available_storage_bytes < opts.override_min_free_bytes) {
                continue;
            }
            eligible.push_back(p);
        }

        // Rank candidates: balanced shard count across upload -> lowest active transfers -> highest storage -> id
        std::sort(eligible.begin(), eligible.end(), [&](const PeerInfo& a, const PeerInfo& b) {
            const uint32_t ca = peer_shard_counts[a.id];
            const uint32_t cb = peer_shard_counts[b.id];
            if (ca != cb) return ca < cb;
            if (a.workload.active_transfers != b.workload.active_transfers)
                return a.workload.active_transfers < b.workload.active_transfers;
            if (a.workload.available_storage_bytes != b.workload.available_storage_bytes)
                return a.workload.available_storage_bytes > b.workload.available_storage_bytes;
            return a.id < b.id;
        });

        std::vector<std::string> used_peers_in_stripe;
        size_t placed_in_stripe = 0;

        for (size_t sh_pos : shard_positions) {
            ShardManifestEntry& sh_meta = manifest.shards[sh_pos];
            const std::string shard_path = temp_staging_dir + "/" + sh_meta.shard_id + ".chunk";
            bool shard_placed = false;

            for (const auto& candidate : eligible) {
                // Enforce strict 1-shard-per-peer per stripe rule
                if (std::find(used_peers_in_stripe.begin(),
                              used_peers_in_stripe.end(),
                              candidate.id) != used_peers_in_stripe.end()) {
                    continue;
                }

                // Fault-injection hook: simulate destination peer becoming unavailable mid-upload
                if (!opts.fail_first_store_on_peer.empty() &&
                    candidate.id == opts.fail_first_store_on_peer &&
                    !did_fail_peer) {
                    did_fail_peer = true;
                    ++res.retries_performed;
                    ss_log << "  [P2P-Upload] Simulated peer unavailable on " << candidate.id
                           << " for " << sh_meta.shard_id << " -> failing over to alternate peer\n";
                    continue;
                }

                // Bounded retry loop (up to 3 attempts on this candidate peer for transient errors)
                bool candidate_ok = false;
                for (int attempt = 1; attempt <= 3; ++attempt) {
                    const bool inject_bad_hash = (opts.corrupt_first_hash_on_peer && !did_corrupt_hash);
                    const bool inject_truncate = (!inject_bad_hash && opts.simulate_interrupted_transfer && !did_interrupt_transfer);

                    if (inject_bad_hash) did_corrupt_hash = true;
                    if (inject_truncate) did_interrupt_transfer = true;

                    std::string err;
                    if (push_shard_p2p(candidate, manifest, st_meta, sh_meta, shard_path,
                                       inject_bad_hash, inject_truncate, opts.override_min_free_bytes, err)) {
                        candidate_ok = true;

                        // Optional idempotency verification: send duplicate STORE_CHUNK requests
                        if (opts.duplicate_store_count > 0 && !did_duplicate_store) {
                            did_duplicate_store = true;
                            for (int d = 0; d < opts.duplicate_store_count; ++d) {
                                std::string dup_err;
                                if (push_shard_p2p(candidate, manifest, st_meta, sh_meta, shard_path,
                                                   false, false, 0, dup_err)) {
                                    ++res.duplicate_acks_verified;
                                }
                            }
                        }
                        break;
                    } else {
                        ++res.retries_performed;
                        ss_log << "  [P2P-Upload] Attempt " << attempt << " failed for "
                               << sh_meta.shard_id << " on Peer " << candidate.id
                               << " (" << err << ")\n";
                    }
                }

                if (candidate_ok) {
                    ShardLocationEntry loc;
                    loc.shard_id = sh_meta.shard_id;
                    loc.peer_id = candidate.id;
                    loc.location_status = "STORED";
                    loc.verified_at_epoch_ms = DatabaseManager::now_epoch_ms();
                    loc.version = 2;
                    sh_meta.locations.push_back(loc);

                    used_peers_in_stripe.push_back(candidate.id);
                    peer_shard_counts[candidate.id]++;
                    ++placed_in_stripe;
                    shard_placed = true;
                    ss_log << "  [P2P-Upload] Stored & SHA-256 verified " << sh_meta.shard_id
                           << " -> Peer " << candidate.id << " (" << candidate.address << ":"
                           << candidate.port << ")\n";
                    break;
                }
            }

            if (!shard_placed) {
                ss_log << "  [P2P-Upload] ERROR: Could not place shard " << sh_meta.shard_id
                       << " on any distinct eligible peer\n";
            }
        }

        // Strict RS(3,2) placement requirement: all 3 shards in the stripe must be stored on 3 distinct peers
        if (placed_in_stripe < shard_positions.size()) {
            manifest.status = "UNRECOVERABLE";
            manifest.version = 2;
            db.upsert_file_manifest(manifest, uploader_peer_id);
            fs::remove_all(fs::u8path(temp_staging_dir), ec);

            res.success = false;
            res.status = manifest.status;
            res.manifest = manifest;
            res.placement_log = ss_log.str();
            res.error_message = "incomplete upload on stripe " + std::to_string(st_meta.stripe_index) +
                                ": placed " + std::to_string(placed_in_stripe) + "/" +
                                std::to_string(shard_positions.size()) +
                                " shards across distinct peers (RS(3,2) requires 3 distinct peers per stripe)";
            return res;
        }
    }

    // 4. All stripes and shards placed & verified across 3 distinct peers per stripe!
    manifest.status = "AVAILABLE";
    manifest.version = 2;
    db.upsert_file_manifest(manifest, uploader_peer_id);

    fs::remove_all(fs::u8path(temp_staging_dir), ec);

    res.success = true;
    res.status = "AVAILABLE";
    res.manifest = manifest;
    res.placement_log = ss_log.str();
    return res;
}
