// ============================================================================
// rfe.cpp - Resilient File Engine Native Terminal CLI (Gate-2 & Phase-2 Task-3)
//
// Commands:
//   rfe connect [ip:port]
//   rfe upload <file-path> [--peer <ip:port>]
//   rfe files [--peer <ip:port>]
//   rfe search <filename> [--peer <ip:port>]
//   rfe info <file-id> [--peer <ip:port>]
//   rfe download <file-id> [--source central]
//   rfe peers [--peer <ip:port>]
//   rfe status [--peer <ip:port>]
//   rfe transfers [--peer <ip:port>]
// ============================================================================
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../engine/catalog/catalog_manager.hpp"
#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/communication/protocol.hpp"
#include "../engine/communication/socket_compat.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/process/process_manager.hpp"
#include "../engine/recovery/client_download.hpp"
#include "../models/file_manifest.hpp"
#include "../models/peer.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

namespace {
std::string format_bytes(uint64_t bytes) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2);
    if (bytes >= 1024ULL * 1024ULL * 1024ULL) {
        ss << (static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0)) << " GB";
    } else if (bytes >= 1024ULL * 1024ULL) {
        ss << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MB";
    } else if (bytes >= 1024ULL) {
        ss << (static_cast<double>(bytes) / 1024.0) << " KB";
    } else {
        ss << bytes << " B";
    }
    return ss.str();
}

bool parse_endpoint(const std::string& ep, std::string& ip_out, uint16_t& port_out) {
    const size_t colon = ep.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= ep.size()) return false;
    ip_out = ep.substr(0, colon);
    try {
        const int p = std::stoi(ep.substr(colon + 1));
        if (p < 1 || p > 65535) return false;
        port_out = static_cast<uint16_t>(p);
        return true;
    } catch (...) {
        return false;
    }
}

struct SessionConfig {
    std::string coord_address = "127.0.0.1";
    uint16_t coord_port = 5001;

    static SessionConfig load() {
        SessionConfig s;
        std::ifstream session_in("metadata/session.json");
        if (session_in) {
            try {
                std::stringstream ss;
                ss << session_in.rdbuf();
                auto j = json::Json::parse(ss.str());
                if (j.contains("address")) s.coord_address = j["address"].as_string();
                if (j.contains("port")) s.coord_port = static_cast<uint16_t>(j["port"].as_int());
                return s;
            } catch (...) {}
        }
        std::ifstream cfg("config.json");
        if (cfg) {
            try {
                std::stringstream ss;
                ss << cfg.rdbuf();
                auto j = json::Json::parse(ss.str());
                if (j.contains("coordinator")) {
                    s.coord_address = j["coordinator"]["address"].as_string();
                    s.coord_port = static_cast<uint16_t>(j["coordinator"]["port"].as_int());
                    if (s.coord_address == "0.0.0.0") s.coord_address = "127.0.0.1";
                }
            } catch (...) {}
        }
        return s;
    }

    void save() const {
        fs::create_directories("metadata");
        std::ofstream out("metadata/session.json", std::ios::trunc);
        json::Json j = json::Json::object();
        j["address"] = coord_address;
        j["port"] = static_cast<long long>(coord_port);
        out << j.dump(2);
    }
};

// Extract optional --peer <ip:port> from command line args
bool extract_peer_flag(std::vector<std::string>& args,
                       std::string& ip_inout,
                       uint16_t& port_inout,
                       bool& explicit_peer_out,
                       std::string& err_out) {
    explicit_peer_out = false;
    for (size_t i = 2; i < args.size(); ++i) {
        if (args[i] == "--peer") {
            if (i + 1 >= args.size()) {
                err_out = "missing <ip:port> argument after --peer";
                return false;
            }
            if (!parse_endpoint(args[i + 1], ip_inout, port_inout)) {
                err_out = "invalid --peer endpoint '" + args[i + 1] + "' (expected <ip:port>)";
                return false;
            }
            explicit_peer_out = true;
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
            return true;
        }
    }
    return true;
}

// Extract optional --output <path> or -o <path> from command line args
bool extract_output_flag(std::vector<std::string>& args,
                         std::string& output_out,
                         std::string& err_out) {
    output_out.clear();
    for (size_t i = 2; i < args.size(); ++i) {
        if (args[i] == "--output" || args[i] == "-o") {
            if (i + 1 >= args.size()) {
                err_out = "missing <destination> argument after " + args[i];
                return false;
            }
            output_out = args[i + 1];
            args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                       args.begin() + static_cast<std::ptrdiff_t>(i + 2));
            return true;
        }
    }
    return true;
}

void show_usage() {
    std::cout <<
        "==========================================================================\n"
        "  DECENTRALIZED PEER-TO-PEER FILE TRANSFER & RECOVERY ENGINE (RFE CLI)    \n"
        "==========================================================================\n"
        "Usage: rfe <command> [arguments] [--peer <ip:port>]\n\n"
        "Commands:\n"
        "  connect [ip:port]              Connect session to a Peer (e.g. 127.0.0.1:5701)\n"
        "  upload <file-path> [--peer <ip:port>]\n"
        "                                 Chunk, RS(3,2)-encode & distribute file via any peer\n"
        "  files [--peer <ip:port>]       List all files in the distributed catalogue\n"
        "  search <query> [--peer <ip:port>]\n"
        "                                 Search distributed catalogue by filename or File ID\n"
        "  info <file-id> [--peer <ip:port>]\n"
        "                                 Display detailed file manifest & shard locations\n"
        "  download <file-id|filename> [--peer <ip:port>] [--output <destination>]\n"
        "                                 Download shards directly from peers & RS-reconstruct\n"
        "  peers [--peer <ip:port>]       Display live peer membership, health and workloads\n"
        "  storage [--peer <ip:port>]     Display peer storage metrics, shard counts & quotas\n"
        "  transfers [--peer <ip:port>]   Display live active transfer statistics & telemetry\n"
        "  recovery [--peer <ip:port>]    Monitor autonomous recovery records and statistics\n"
        "  repair [file-id] [--peer <ip:port>]\n"
        "                                 Trigger on-demand durability recovery for missing shards\n"
        "  status [--peer <ip:port>]      Display cluster health, storage, and catalogue summary\n"
        "  shutdown [--peer <ip:port>]    Gracefully shut down target peer daemon (alias: stop)\n"
        "==========================================================================\n";
}
}  // namespace

int main(int argc, char** argv) {
    try {
        std::vector<std::string> args = proc::command_line_args(argc, argv);
        if (args.size() < 2) {
            show_usage();
            return 1;
        }

    const std::string cmd = args[1];
    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        show_usage();
        return 0;
    }

    if (!sock_init()) {
        std::cerr << "rfe error: socket subsystem initialization failed\n";
        return 1;
    }

    std::error_code cwd_ec;
    const fs::path invocation_cwd = fs::current_path(cwd_ec);

    std::string root, err;
    proc::enter_project_root(root, err);

    SessionConfig session = SessionConfig::load();
    bool explicit_peer = false;
    std::string explicit_output;
    std::string flag_err;
    if (!extract_peer_flag(args, session.coord_address, session.coord_port, explicit_peer, flag_err) ||
        !extract_output_flag(args, explicit_output, flag_err)) {
        std::cerr << "[RFE] Error: " << flag_err << "\n";
        return 1;
    }

    if (!explicit_output.empty()) {
        const fs::path out_p = fs::u8path(explicit_output);
        if (out_p.is_relative() && !invocation_cwd.empty()) {
            explicit_output = (invocation_cwd / out_p).lexically_normal().u8string();
        }
    }

    // 1. rfe connect [ip:port]
    if (cmd == "connect") {
        if (args.size() >= 3) {
            std::string ep = args[2];
            if (!parse_endpoint(ep, session.coord_address, session.coord_port)) {
                std::cerr << "[RFE] Error: invalid endpoint '" << ep
                          << "' (expected <ip:port> with port 1..65535)\n";
                return 1;
            }
        }
        std::cout << "[RFE] Probing Peer at " << session.coord_address << ":" << session.coord_port << "...\n";
        TCPClient client(2000, 2000);
        auto start_t = std::chrono::steady_clock::now();
        if (!client.connect_to(session.coord_address, session.coord_port)) {
            std::cerr << "[RFE] Connection failed: Peer unreachable at "
                      << session.coord_address << ":" << session.coord_port << "\n";
            return 1;
        }
        auto lat_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_t).count();
        session.save();
        std::cout << "[RFE] Connected successfully to Peer at "
                  << session.coord_address << ":" << session.coord_port
                  << " (latency: " << lat_ms << " ms)\n";
        std::cout << "[RFE] Active session saved.\n";
        return 0;
    }

    // 2. rfe upload <file-path> [--peer <ip:port>]
    if (cmd == "upload") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe upload <file-path> [--peer <ip:port>]\n";
            return 1;
        }
        fs::path p = fs::u8path(args[2]);
        if (p.is_relative() && !invocation_cwd.empty()) {
            p = (invocation_cwd / p).lexically_normal();
        }
        const std::string file_path = p.u8string();
        std::error_code ec;
        if (!fs::is_regular_file(p, ec)) {
            std::cerr << "[RFE] Upload failed: file does not exist or is not a regular file: " << args[2] << "\n";
            return 1;
        }

        const uint64_t fsize = static_cast<uint64_t>(fs::file_size(p, ec));
        const std::string expected_sha256 = ChunkManager::sha256_of_file(file_path);
        if (expected_sha256.empty()) {
            std::cerr << "[RFE] Upload failed: cannot read or hash file: " << file_path << "\n";
            return 1;
        }

        const bool is_local_peer =
            (session.coord_address == "127.0.0.1" || session.coord_address == "localhost");

        std::vector<uint8_t> payload;
        if (!is_local_peer && fsize > 0) {
            if (fsize > proto::MAX_PAYLOAD_SIZE) {
                std::cerr << "[RFE] Upload failed: file size (" << format_bytes(fsize)
                          << ") exceeds wire payload limit (" << format_bytes(proto::MAX_PAYLOAD_SIZE) << ")\n";
                return 1;
            }
            std::ifstream in(p, std::ios::binary);
            if (!in) {
                std::cerr << "[RFE] Upload failed: cannot open file: " << file_path << "\n";
                return 1;
            }
            payload.resize(static_cast<size_t>(fsize));
            in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(fsize));
        }

        std::cout << "[RFE] Uploading '" << p.filename().string() << "' ("
                  << format_bytes(fsize) << ") via peer "
                  << session.coord_address << ":" << session.coord_port << "...\n";

        const auto up_start = std::chrono::steady_clock::now();
        TCPClient p2p_client(15000, 120000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_UPLOAD_FILE;
            req["filename"] = p.filename().string();
            if (is_local_peer) {
                req["file_path"] = fs::absolute(p, ec).lexically_normal().u8string();
            }
            req["expected_sha256"] = expected_sha256;
            req["original_size"] = static_cast<long long>(fsize);

            json::Json resp;
            std::vector<uint8_t> rpl;
            if (p2p_client.request(req, payload, resp, rpl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_UPLOAD_FILE_ACK) {
                if (resp["status"].as_string() != "OK") {
                    std::cerr << "[RFE] Decentralized upload FAILED on peer "
                              << resp["peer_id"].as_string() << ": "
                              << resp["reason"].as_string() << "\n";
                    if (resp.contains("placement_log") && !resp["placement_log"].as_string().empty()) {
                        std::cerr << resp["placement_log"].as_string();
                    }
                    return 1;
                }

                const FileManifest m = FileManifest::from_json(resp["manifest"]);
                const auto up_end = std::chrono::steady_clock::now();
                const uint64_t elapsed_ms = resp.contains("elapsed_ms") && resp["elapsed_ms"].as_int() > 0
                    ? static_cast<uint64_t>(resp["elapsed_ms"].as_int())
                    : static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(up_end - up_start).count());
                const uint64_t eff_ms = elapsed_ms > 0 ? elapsed_ms : 1;
                const uint64_t speed_bps = resp.contains("speed_bps") && resp["speed_bps"].as_int() > 0
                    ? static_cast<uint64_t>(resp["speed_bps"].as_int())
                    : ((m.file_size * 1000ULL) / eff_ms);

                std::cout << "===================== DECENTRALIZED UPLOAD SUCCESS =====================\n"
                          << "  Entry Peer:     " << resp["peer_id"].as_string()
                          << " (" << session.coord_address << ":" << session.coord_port << ")\n"
                          << "  File ID:        " << m.file_id << "\n"
                          << "  Filename:       " << m.original_name << "\n"
                          << "  Original Size:  " << format_bytes(m.file_size) << " (" << m.file_size << " bytes)\n"
                          << "  SHA-256:        " << m.sha256 << "\n"
                          << "  Shard Size:     " << format_bytes(m.shard_size) << "\n"
                          << "  RS Coding:      RS(3,2) (" << m.data_shards_k << " data + "
                          << m.parity_shards_m << " parity per stripe)\n"
                          << "  Stripes/Shards: " << m.total_stripes << " stripes / "
                          << m.total_shards << " shards\n"
                          << "  Transfer Time:  " << elapsed_ms << " ms\n"
                          << "  Transfer Speed: " << format_bytes(speed_bps) << "/s\n"
                          << "  Status:         " << m.status << " (Version " << m.version << ")\n"
                          << "------------------------------------------------------------------------\n"
                          << "  SHARD DISTRIBUTION ACROSS PEERS:\n"
                          << resp["placement_log"].as_string()
                          << "========================================================================\n";
                return 0;
            }
        } else if (explicit_peer) {
            std::cerr << "[RFE] Upload failed: target peer unreachable at "
                      << session.coord_address << ":" << session.coord_port << "\n";
            return 1;
        }

        // Legacy Gate-2 Coordinator fallback when not targeting a decentralized peer
        TCPClient client(5000, 5000);
        bool tcp_success = false;
        std::string file_id, filename, sha256, status_str, central_path, upload_time;
        uint64_t rec_size = fsize;

        if (client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = "UPLOAD_FILE";
            req["filename"] = p.filename().string();
            json::Json resp;
            std::vector<uint8_t> rpl;
            if (client.request(req, payload, resp, rpl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == "UPLOAD_FILE_ACK") {
                tcp_success = true;
                file_id = resp["file_id"].as_string();
                filename = resp["filename"].as_string();
                rec_size = static_cast<uint64_t>(resp["size"].as_int());
                sha256 = resp["sha256"].as_string();
                status_str = resp["status_str"].as_string();
                central_path = resp["central_path"].as_string();
                upload_time = resp["upload_time"].as_string();
            }
        }

        if (!tcp_success) {
            CatalogManager catalog;
            FileRecord rec;
            std::string upload_err;
            if (!catalog.stage_file(file_path, rec, upload_err)) {
                std::cerr << "[RFE] Upload failed: " << upload_err << "\n";
                return 1;
            }
            file_id = rec.file_id;
            filename = rec.filename;
            rec_size = rec.size;
            sha256 = rec.sha256;
            status_str = file_status_str(rec.status);
            central_path = rec.central_path;
            upload_time = rec.upload_time;
        }

        std::cout << "========================== FILE UPLOAD SUCCESS ==========================\n"
                  << "  File ID:      " << file_id << "\n"
                  << "  Filename:     " << filename << "\n"
                  << "  Size:         " << format_bytes(rec_size) << " (" << rec_size << " bytes)\n"
                  << "  SHA-256:      " << sha256 << "\n"
                  << "  Status:       " << status_str << " (Safety backup staged centrally)\n"
                  << "  Staged Path:  " << central_path << "\n"
                  << "  Upload Time:  " << upload_time << "\n"
                  << "=========================================================================\n";
        return 0;
    }

    // 3. rfe files [--peer <ip:port>]
    if (cmd == "files") {
        TCPClient p2p_client(3000, 3000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_CATALOGUE_QUERY;
            req["sync_if_missing"] = true;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_CATALOGUE_QUERY_ACK &&
                resp.contains("files") && resp["files"].is_array()) {
                std::cout << "==================================== DISTRIBUTED FILE CATALOGUE ====================================\n"
                          << std::left
                          << std::setw(14) << "FILE ID"
                          << std::setw(24) << "FILENAME"
                          << std::setw(12) << "SIZE"
                          << std::setw(14) << "LIVE STATUS"
                          << std::setw(12) << "SHARDS ONL"
                          << std::setw(22) << "HOSTING PEERS"
                          << "SHA-256\n"
                          << "----------------------------------------------------------------------------------------------------\n";
                const auto& items = resp["files"].items();
                if (items.empty()) {
                    std::cout << "  (no files in distributed catalogue yet)\n";
                } else {
                    for (const auto& f : items) {
                        const std::string fid = f["file_id"].as_string();
                        const std::string fname = f["original_name"].as_string();
                        const uint64_t fsz = static_cast<uint64_t>(f["file_size"].as_int());
                        const std::string lst = f["live_status"].as_string();
                        const std::string sh_ratio = std::to_string(f["online_shards_count"].as_int()) + "/" +
                                                     std::to_string(f["total_shards_count"].as_int());
                        const std::string sha = f["sha256"].as_string();
                        std::string peers_joined;
                        if (f.contains("hosting_peers") && f["hosting_peers"].is_array()) {
                            for (size_t pi = 0; pi < f["hosting_peers"].size(); ++pi) {
                                if (pi > 0) peers_joined += ",";
                                peers_joined += f["hosting_peers"][pi].as_string();
                            }
                        }
                        std::cout << std::left
                                  << std::setw(14) << fid
                                  << std::setw(24) << (fname.size() > 22 ? fname.substr(0, 19) + "..." : fname)
                                  << std::setw(12) << format_bytes(fsz)
                                  << std::setw(14) << lst
                                  << std::setw(12) << sh_ratio
                                  << std::setw(22) << (peers_joined.empty() ? "-" : peers_joined)
                                  << (sha.size() >= 16 ? sha.substr(0, 16) + "..." : sha) << "\n";
                    }
                }
                std::cout << "====================================================================================================\n";
                return 0;
            }
        }

        // Legacy fallback
        CatalogManager catalog;
        const auto files = catalog.list_files();
        std::cout << "==================================== FILE CATALOGUE ====================================\n";
        for (const auto& f : files) {
            std::cout << "  [" << f.file_id << "] " << f.filename << " (" << format_bytes(f.size)
                      << ") - " << file_status_str(f.status) << "\n";
        }
        return 0;
    }

    // 4. rfe search <filename|file-id> [--peer <ip:port>]
    if (cmd == "search") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe search <query> [--peer <ip:port>]\n";
            return 1;
        }
        const std::string query = args[2];
        TCPClient p2p_client(3000, 3000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_CATALOGUE_QUERY;
            req["query"] = query;
            req["sync_if_missing"] = true;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_CATALOGUE_QUERY_ACK &&
                resp.contains("files") && resp["files"].is_array()) {
                std::cout << "===================== DECENTRALIZED CATALOGUE SEARCH =====================\n"
                          << "  Query:          \"" << query << "\"\n"
                          << "  Contacted Peer: " << resp["peer_id"].as_string()
                          << " (" << session.coord_address << ":" << session.coord_port << ")\n"
                          << "  Matches Found:  " << resp["files"].size() << "\n"
                          << "--------------------------------------------------------------------------\n";
                if (resp["files"].size() == 0) {
                    std::cout << "  (no matching files found)\n";
                } else {
                    for (const auto& f : resp["files"].items()) {
                        const uint64_t fsz = static_cast<uint64_t>(f["file_size"].as_int());
                        std::string hosting_str;
                        if (f.contains("hosting_peers") && f["hosting_peers"].is_array()) {
                            for (size_t i = 0; i < f["hosting_peers"].size(); ++i) {
                                if (i > 0) hosting_str += ", ";
                                hosting_str += f["hosting_peers"][i].as_string();
                            }
                        }
                        std::cout << "  File ID:        " << f["file_id"].as_string() << "\n"
                                  << "  Filename:       " << f["original_name"].as_string() << "\n"
                                  << "  File Size:      " << format_bytes(fsz) << " (" << fsz << " bytes)\n"
                                  << "  SHA-256:        " << f["sha256"].as_string() << "\n"
                                  << "  Availability:   " << f["live_status"].as_string()
                                  << " (Reconstructable: " << (f["reconstructable"].as_bool() ? "YES" : "NO")
                                  << ", Online Shards: " << f["online_shards_count"].as_int()
                                  << "/" << f["total_shards_count"].as_int() << ")\n"
                                  << "  Storage Peers:  " << (hosting_str.empty() ? "(none)" : hosting_str) << "\n";
                        if (f.contains("shards") && f["shards"].is_array()) {
                            std::cout << "  Shard Map:      ";
                            for (size_t si = 0; si < f["shards"].size(); ++si) {
                                const auto& sh = f["shards"][si];
                                std::string p_id = "(none)";
                                if (sh.contains("locations") && sh["locations"].is_array() && sh["locations"].size() > 0) {
                                    for (size_t li = 0; li < sh["locations"].size(); ++li) {
                                        const auto& loc = sh["locations"][li];
                                        if (!loc.contains("location_status") ||
                                            loc["location_status"].as_string() == "STORED") {
                                            p_id = loc["peer_id"].as_string();
                                            break;
                                        }
                                    }
                                    if (p_id == "(none)") {
                                        p_id = sh["locations"][0]["peer_id"].as_string();
                                    }
                                }
                                if (si > 0) std::cout << " | ";
                                std::cout << sh["shard_id"].as_string() << "@" << p_id;
                            }
                            std::cout << "\n";
                        }
                        std::cout << "--------------------------------------------------------------------------\n";
                    }
                    if (resp["files"].size() > 1) {
                        std::cout << "  [Note] Multiple files matched. Use the exact File ID with 'rfe download <FILE_ID>'.\n";
                    }
                }
                std::cout << "==========================================================================\n";
                return 0;
            }
        }

        CatalogManager catalog;
        auto matches = catalog.search_files(query);
        std::cout << "Search results for: \"" << query << "\":\n";
        for (const auto& f : matches) {
            std::cout << "  [" << f.file_id << "] " << f.filename
                      << " (" << format_bytes(f.size) << ") - " << file_status_str(f.status) << "\n";
        }
        if (matches.empty()) std::cout << "  (no matching files found)\n";
        return 0;
    }

    // 5. rfe info <file-id> [--peer <ip:port>]
    if (cmd == "info") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe info <file-id> [--peer <ip:port>]\n";
            return 1;
        }
        const std::string fid = args[2];
        TCPClient p2p_client(3000, 3000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_CATALOGUE_QUERY;
            req["file_id"] = fid;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_CATALOGUE_QUERY_ACK &&
                resp.contains("files") && resp["files"].is_array() &&
                resp["files"].size() > 0) {
                const auto& f = resp["files"][0];
                const FileManifest m = FileManifest::from_json(f);
                std::cout << "============================ FILE MANIFEST INFO ============================\n"
                          << "  File ID:         " << m.file_id << "\n"
                          << "  Filename:        " << m.original_name << "\n"
                          << "  Original Size:   " << format_bytes(m.file_size) << " (" << m.file_size << " bytes)\n"
                          << "  Full SHA-256:    " << m.sha256 << "\n"
                          << "  Live Status:     " << f["live_status"].as_string()
                          << " (Reconstructable: " << (f["reconstructable"].as_bool() ? "YES" : "NO") << ")\n"
                          << "  Coding Policy:   RS(3,2) Systematic Reed-Solomon\n"
                          << "  Data Shard Size: " << format_bytes(m.shard_size) << "\n"
                          << "  Total Stripes:   " << m.total_stripes << "\n"
                          << "  Total Shards:    " << m.total_shards << "\n"
                          << "  Uploader Peer:   " << m.uploader_peer_id << "\n"
                          << "----------------------------------------------------------------------------\n"
                          << "  SHARDS DISTRIBUTION MAP:\n";
                for (const auto& sh : m.shards) {
                    std::string peer_str = "(none)";
                    for (const auto& loc : sh.locations) {
                        if (loc.location_status == "STORED") {
                            peer_str = loc.peer_id;
                            break;
                        }
                    }
                    if (peer_str == "(none)" && !sh.locations.empty()) {
                        peer_str = sh.locations[0].peer_id + " (" + sh.locations[0].location_status + ")";
                    }
                    std::cout << "    [S" << std::setfill('0') << std::setw(3) << sh.stripe_index
                              << std::setfill(' ') << "] " << sh.shard_id
                              << "  Type: " << std::left << std::setw(8) << sh.shard_type
                              << " Orig: " << std::setw(8) << sh.original_bytes
                              << " Peer: " << std::setw(8) << peer_str
                              << " SHA: " << (sh.sha256.size() >= 16 ? sh.sha256.substr(0, 16) : sh.sha256) << "...\n";
                }
                std::cout << "============================================================================\n";
                return 0;
            }
        }

        CatalogManager catalog;
        FileRecord rec;
        if (!catalog.get_file(fid, rec)) {
            std::cerr << "[RFE] File ID not found: " << fid << "\n";
            return 1;
        }
        std::cout << "  File ID: " << rec.file_id << " (" << rec.filename << ")\n";
        return 0;
    }

    // 6. rfe download <file-id|filename> [--peer <ip:port>] [--output <destination>] [--source central]
    if (cmd == "download") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe download <file-id|filename> [--peer <ip:port>] [--output <destination>]\n";
            return 1;
        }
        const std::string fid = args[2];
        DownloadSource src = DownloadSource::PEERS;
        if (args.size() >= 4 && args[3] == "--source" && args.size() >= 5 && args[4] == "central") {
            src = DownloadSource::CENTRAL;
        }

        // First attempt decentralized P2P download via the contacted peer (when not explicitly central)
        if (src == DownloadSource::PEERS) {
            const auto dl_start = std::chrono::steady_clock::now();
            ClientDownload p2p_downloader(session.coord_address, session.coord_port, "output");
            const DownloadResult p2p_res = p2p_downloader.download_p2p(fid, explicit_output);
            if (p2p_res.success) {
                const auto dl_end = std::chrono::steady_clock::now();
                const uint64_t dl_ms = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(dl_end - dl_start).count());
                const uint64_t eff_ms = dl_ms > 0 ? dl_ms : 1;
                const uint64_t speed_bps = (p2p_res.file_size * 1000ULL) / eff_ms;

                std::cout << "===================== DECENTRALIZED DOWNLOAD COMPLETE =====================\n"
                          << "  Contacted Peer:       " << session.coord_address << ":" << session.coord_port << "\n"
                          << "  File ID:              " << p2p_res.file_id << "\n"
                          << "  File Name:            " << p2p_res.filename << "\n"
                          << "  Original File Size:   " << format_bytes(p2p_res.file_size)
                          << " (" << p2p_res.file_size << " bytes)\n"
                          << "  Downloaded Bytes:     " << format_bytes(p2p_res.downloaded_bytes)
                          << " (" << p2p_res.downloaded_bytes << " bytes)\n"
                          << "  Retrieved Shards:     " << p2p_res.shards_retrieved << "\n"
                          << "  Reconstructed Stripes:" << p2p_res.stripes_reconstructed << "/"
                          << p2p_res.stripes_total << "\n"
                          << "  Failed Connections:   " << p2p_res.failed_peer_connections << "\n"
                          << "  Corrupted Rejected:   " << p2p_res.corrupted_shards_rejected << "\n"
                          << "  Transfer Time:        " << dl_ms << " ms\n"
                          << "  Transfer Speed:       " << format_bytes(speed_bps) << "/s\n"
                          << "  Integrity Check:      VERIFIED (SHA-256: " << p2p_res.actual_sha256 << ")\n"
                          << "  Final Destination:    " << p2p_res.output_path << "\n"
                          << "---------------------------------------------------------------------------\n"
                          << "  SHARD RETRIEVAL LOG:\n"
                          << p2p_res.retrieval_log
                          << "===========================================================================\n";
                return 0;
            }
            if (explicit_peer || p2p_res.message.find("UNRECOVERABLE") != std::string::npos ||
                p2p_res.message.find("ambiguous") != std::string::npos ||
                p2p_res.message.find("not found") != std::string::npos) {
                std::cerr << "[RFE] Decentralized download FAILED: " << p2p_res.message << "\n";
                if (!p2p_res.retrieval_log.empty()) {
                    std::cerr << p2p_res.retrieval_log;
                }
                return 1;
            }
        }

        std::cout << "[RFE] Requesting download for " << fid << " (source: "
                  << (src == DownloadSource::CENTRAL ? "CENTRAL STAGING" : "MULTI-PEER TCP DIRECT") << ")...\n";

        ClientDownload downloader(session.coord_address, session.coord_port,
                                  explicit_output.empty() ? "output" : explicit_output);
        auto res = downloader.download(fid, src);

        if (!res.success) {
            std::cerr << "[RFE] Download FAILED: " << res.message << "\n";
            return 1;
        }

        std::cout << "=========================== DOWNLOAD COMPLETE ===========================\n"
                  << "  File ID:         " << res.file_id << "\n"
                  << "  Output File:     " << res.output_path << "\n"
                  << "  File Size:       " << format_bytes(res.file_size) << "\n"
                  << "  Stripes:         " << res.stripes_total << "\n"
                  << "  Shards Fetched:  " << res.shards_retrieved << " (Retrieved directly from peers)\n"
                  << "  Integrity Check: MATCH VERIFIED (" << res.actual_sha256 << ")\n"
                  << "  Message:         " << res.message << "\n"
                  << "=========================================================================\n";
        return 0;
    }

    // 7. rfe peers [--peer <ip:port>]
    if (cmd == "peers") {
        TCPClient client(2000, 2000);
        if (!client.connect_to(session.coord_address, session.coord_port)) {
            std::cerr << "[RFE] Cannot connect to Peer at "
                      << session.coord_address << ":" << session.coord_port << "\n";
            return 1;
        }
        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        json::Json resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK) {
            std::cerr << "[RFE] Failed to query peers\n";
            return 1;
        }
        PeerRegistry reg;
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                PeerInfo pi = PeerInfo::from_json(item);
                std::string msg;
                reg.register_peer(pi, msg);
            }
        }
        std::cout << reg.format_table();
        return 0;
    }

    // 8. rfe status [--peer <ip:port>]
    if (cmd == "status") {
        TCPClient client(2000, 2000);
        if (!client.connect_to(session.coord_address, session.coord_port)) {
            std::cerr << "[RFE] Peer is OFFLINE or unreachable at "
                      << session.coord_address << ":" << session.coord_port << "\n";
            return 1;
        }

        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        json::Json resp;
        std::vector<uint8_t> pl;
        client.request(req, {}, resp, pl);

        size_t online = 0, suspect = 0, offline = 0;
        uint64_t total_storage = 0;
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                PeerInfo pi = PeerInfo::from_json(item);
                if (pi.state == PeerHealth::ONLINE) ++online;
                else if (pi.state == PeerHealth::SUSPECT) ++suspect;
                else ++offline;
                total_storage += pi.workload.available_storage_bytes;
            }
        }

        std::cout << "============================ CLUSTER STATUS ============================\n"
                  << "  Contacted Peer:    ONLINE (" << session.coord_address << ":" << session.coord_port << ")\n"
                  << "  Cluster Health:    " << (offline > 0 ? "DEGRADED" : "HEALTHY") << "\n"
                  << "  Peers:             " << (online + suspect + offline)
                  << " total (" << online << " ONLINE, " << suspect << " SUSPECT, " << offline << " OFFLINE)\n"
                  << "  Total Available:   " << format_bytes(total_storage) << "\n"
                  << "========================================================================\n";
        return 0;
    }

    // rfe storage [--peer <ip:port>]
    if (cmd == "storage") {
        TCPClient p2p_client(3000, 3000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_STORAGE_INFO;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_STORAGE_INFO_ACK) {
                const uint64_t used = static_cast<uint64_t>(resp["used_bytes"].as_int());
                const uint64_t avail = static_cast<uint64_t>(resp["available_bytes"].as_int());
                const uint64_t max_quota = static_cast<uint64_t>(resp["max_storage_bytes"].as_int());
                const uint64_t db_sz = static_cast<uint64_t>(resp["db_size_bytes"].as_int());
                const int64_t shard_cnt = resp["local_shards_count"].as_int();
                const int64_t cat_cnt = resp["catalogue_files_count"].as_int();

                std::cout << "========================== PEER STORAGE TELEMETRY ==========================\n"
                          << "  Peer ID:            " << resp["peer_id"].as_string()
                          << " (" << session.coord_address << ":" << session.coord_port << ")\n"
                          << "  Storage Directory:  " << resp["storage_dir"].as_string() << "\n"
                          << "  SQLite Database:    " << resp["db_path"].as_string() << " (" << format_bytes(db_sz) << ")\n"
                          << "  Storage Used:       " << format_bytes(used) << " (" << used << " bytes)\n"
                          << "  Storage Available:  " << format_bytes(avail) << " (" << avail << " bytes)\n"
                          << "  Storage Quota:      " << (max_quota > 0 ? format_bytes(max_quota) : "UNLIMITED") << "\n"
                          << "  Local Shard Count:  " << shard_cnt << "\n"
                          << "  Distributed Files:  " << cat_cnt << "\n";
                if (resp.contains("local_shards") && resp["local_shards"].is_array() && resp["local_shards"].size() > 0) {
                    std::cout << "----------------------------------------------------------------------------\n"
                              << "  LOCAL STORED SHARDS (First " << std::min<size_t>(resp["local_shards"].size(), 10)
                              << " of " << resp["local_shards"].size() << "):\n";
                    for (size_t si = 0; si < resp["local_shards"].size() && si < 10; ++si) {
                        const auto& s = resp["local_shards"][si];
                        std::cout << "    [" << std::setw(2) << (si + 1) << "] "
                                  << std::left << std::setw(36) << s["shard_id"].as_string()
                                  << " (" << format_bytes(static_cast<uint64_t>(s["size_bytes"].as_int())) << ")\n";
                    }
                }
                std::cout << "============================================================================\n";
                return 0;
            }
        }
        std::cerr << "[RFE] Storage info failed: target peer unreachable at "
                  << session.coord_address << ":" << session.coord_port << "\n";
        return 1;
    }

    // 9. rfe transfers [--peer <ip:port>]
    if (cmd == "transfers") {
        TCPClient p2p_client(2000, 2000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json t_req = json::Json::object();
            t_req["type"] = proto::P2P_TRANSFERS_QUERY;
            json::Json t_resp;
            std::vector<uint8_t> t_pl;
            if (p2p_client.request(t_req, {}, t_resp, t_pl) == proto::RecvStatus::OK &&
                t_resp["type"].as_string() == proto::P2P_TRANSFERS_QUERY_ACK) {
                std::cout << "======================== NODE TRANSFER TELEMETRY ========================\n"
                          << "  Peer ID:            " << t_resp["peer_id"].as_string()
                          << " (" << session.coord_address << ":" << session.coord_port << ")\n"
                          << "  Active Uploads:     " << t_resp["active_uploads"].as_int() << "\n"
                          << "  Active Downloads:   " << t_resp["active_downloads"].as_int() << "\n";
                if (t_resp.contains("transfers") && t_resp["transfers"].is_array() && t_resp["transfers"].size() > 0) {
                    std::cout << "-------------------------------------------------------------------------\n"
                              << std::left
                              << std::setw(8) << "DIR"
                              << std::setw(12) << "STATUS"
                              << std::setw(24) << "FILENAME"
                              << std::setw(12) << "SIZE"
                              << std::setw(12) << "SPEED"
                              << "ELAPSED\n"
                              << "-------------------------------------------------------------------------\n";
                    for (const auto& item : t_resp["transfers"].items()) {
                        const std::string fn = item["filename"].as_string();
                        const uint64_t bytes = static_cast<uint64_t>(item["total_bytes"].as_int());
                        const uint64_t spd = static_cast<uint64_t>(item["speed_bps"].as_int());
                        const uint64_t ms = static_cast<uint64_t>(item["elapsed_ms"].as_int());
                        std::cout << std::left
                                  << std::setw(8) << item["direction"].as_string()
                                  << std::setw(12) << item["status"].as_string()
                                  << std::setw(24) << (fn.size() > 22 ? fn.substr(0, 19) + "..." : fn)
                                  << std::setw(12) << format_bytes(bytes)
                                  << std::setw(12) << (spd > 0 ? (format_bytes(spd) + "/s") : "-")
                                  << ms << " ms\n";
                    }
                }
                std::cout << "=========================================================================\n\n";
            }
        }

        TCPClient client(2000, 2000);
        if (!client.connect_to(session.coord_address, session.coord_port)) {
            std::cerr << "[RFE] Peer unreachable\n";
            return 1;
        }
        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        json::Json resp;
        std::vector<uint8_t> pl;
        client.request(req, {}, resp, pl);

        std::cout << "======================== ACTIVE WORKLOAD & TRANSFERS ========================\n"
                  << std::left
                  << std::setw(10) << "PEER ID"
                  << std::setw(22) << "ENDPOINT"
                  << std::setw(14) << "HEALTH"
                  << std::setw(18) << "ACTIVE / CAPACITY"
                  << "BUSY STATE\n"
                  << "-----------------------------------------------------------------------------\n";
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                PeerInfo pi = PeerInfo::from_json(item);
                std::string ep = pi.address + ":" + std::to_string(pi.port);
                std::string trans = std::to_string(pi.workload.active_transfers) + " / " +
                                    std::to_string(pi.workload.transfer_capacity);
                std::cout << std::left
                          << std::setw(10) << pi.id
                          << std::setw(22) << ep
                          << std::setw(14) << peer_health_str(pi.state)
                          << std::setw(18) << trans
                          << (pi.workload.is_busy() ? "BUSY (Capacity saturated)" : "IDLE / ACCEPTING") << "\n";
            }
        }
        std::cout << "=============================================================================\n";
        return 0;
    }

    // rfe recovery [--peer <ip:port>] [--trigger]
    if (cmd == "recovery") {
        bool trigger = false;
        for (size_t i = 2; i < args.size(); ++i) {
            if (args[i] == "--trigger" || args[i] == "-t") trigger = true;
        }

        TCPClient p2p_client(5000, 10000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_RECOVERY_STATUS;
            req["trigger_scan"] = trigger;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_RECOVERY_STATUS_ACK) {
                const int64_t started = resp.contains("started_count") ? resp["started_count"].as_int() : 0;
                const int64_t completed = resp.contains("completed_count") ? resp["completed_count"].as_int() : 0;
                const int64_t failed = resp.contains("failed_count") ? resp["failed_count"].as_int() : 0;

                const json::Json* recs_ptr = nullptr;
                if (resp.contains("records") && resp["records"].is_array()) recs_ptr = &resp["records"];
                else if (resp.contains("recovery_records") && resp["recovery_records"].is_array()) recs_ptr = &resp["recovery_records"];

                const size_t total_recs = resp.contains("total_records")
                                              ? static_cast<size_t>(resp["total_records"].as_int())
                                              : (recs_ptr ? recs_ptr->size() : 0);

                std::cout << "======================== AUTONOMOUS RECOVERY STATUS ========================\n"
                          << "  Peer ID:            " << resp["peer_id"].as_string()
                          << " (" << session.coord_address << ":" << session.coord_port << ")\n"
                          << "  Started / Active:   " << started << "\n"
                          << "  Completed Repairs:  " << completed << "\n"
                          << "  Failed / Aborted:   " << failed << "\n"
                          << "  Total Event Logs:   " << total_recs << "\n";
                if (recs_ptr && recs_ptr->size() > 0) {
                    std::cout << "----------------------------------------------------------------------------\n"
                              << std::left
                              << std::setw(12) << "STATUS"
                              << std::setw(28) << "SHARD ID"
                              << std::setw(12) << "FAILED"
                              << std::setw(12) << "TARGET"
                              << std::setw(12) << "REBUILDER"
                              << "REASON\n"
                              << "----------------------------------------------------------------------------\n";
                    for (const auto& r : recs_ptr->items()) {
                        std::cout << std::left
                                  << std::setw(12) << (r.contains("status") ? r["status"].as_string() : "-")
                                  << std::setw(28) << (r.contains("shard_id") ? r["shard_id"].as_string() : "-")
                                  << std::setw(12) << (r.contains("failed_peer_id") ? r["failed_peer_id"].as_string() : "-")
                                  << std::setw(12) << (r.contains("target_peer_id") && !r["target_peer_id"].as_string().empty() ? r["target_peer_id"].as_string() : "-")
                                  << std::setw(12) << (r.contains("rebuilder_peer_id") ? r["rebuilder_peer_id"].as_string() : "-")
                                  << (r.contains("reason") ? r["reason"].as_string() : "") << "\n";
                    }
                } else {
                    std::cout << "  (no recovery events recorded — network durability healthy)\n";
                }
                std::cout << "============================================================================\n";
                return 0;
            }
        }
        std::cerr << "[RFE] Recovery query failed: target peer unreachable at "
                  << session.coord_address << ":" << session.coord_port << "\n";
        return 1;
    }

    // rfe shutdown [--peer <ip:port>] / rfe stop
    if (cmd == "shutdown" || cmd == "stop") {
        TCPClient p2p_client(3000, 3000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_SHUTDOWN;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_SHUTDOWN_ACK) {
                std::cout << "[RFE] " << resp["message"].as_string() << " ("
                          << session.coord_address << ":" << session.coord_port << ")\n";
                return 0;
            }
        }
        std::cerr << "[RFE] Shutdown request failed: target peer unreachable at "
                  << session.coord_address << ":" << session.coord_port << "\n";
        return 1;
    }

    // 10. rfe repair [file-id] [--peer <ip:port>]
    if (cmd == "repair") {
        TCPClient p2p_client(5000, 10000);
        if (p2p_client.connect_to(session.coord_address, session.coord_port)) {
            json::Json req = json::Json::object();
            req["type"] = proto::P2P_REPAIR_TRIGGER;
            req["force_owner"] = true;
            json::Json resp;
            std::vector<uint8_t> pl;
            if (p2p_client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                resp["type"].as_string() == proto::P2P_REPAIR_TRIGGER_ACK) {
                std::cout << "===================== DECENTRALIZED RECOVERY SCAN =====================\n"
                          << "  Contacted Peer:  " << resp["peer_id"].as_string() << "\n"
                          << "  Shards Repaired: " << resp["repaired_count"].as_int() << "\n";
                if (resp.contains("recovery_records") && resp["recovery_records"].is_array() &&
                    resp["recovery_records"].size() > 0) {
                    std::cout << "  RECOVERY RECORDS:\n";
                    for (const auto& r : resp["recovery_records"].items()) {
                        std::cout << "    [" << r["status"].as_string() << "] "
                                  << r["shard_id"].as_string()
                                  << " (failed=" << r["failed_peer_id"].as_string()
                                  << " -> target=" << r["target_peer_id"].as_string()
                                  << " by " << r["rebuilder_peer_id"].as_string() << ") - "
                                  << r["reason"].as_string() << "\n";
                    }
                }
                std::cout << "=======================================================================\n";
                return 0;
            }
        }

        if (args.size() < 3) {
            std::cerr << "usage: rfe repair [file-id] [--peer <ip:port>]\n";
            return 1;
        }
        const std::string fid = args[2];
        TCPClient client(5000, 5000);
        if (!client.connect_to(session.coord_address, session.coord_port)) {
            std::cerr << "[RFE] Peer unreachable\n";
            return 1;
        }
        json::Json req = json::Json::object();
        req["type"] = "REPAIR_FILE";
        req["file_id"] = fid;
        json::Json resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
            resp["type"].as_string() == "REPAIR_FILE_ACK") {
            std::cout << "Repair result: " << resp["status"].as_string() << "\n"
                      << resp["report"].as_string();
            return 0;
        }
        std::cerr << "[RFE] Repair failed or timed out\n";
        return 1;
    }

    std::cerr << "[RFE] Error: unknown command '" << cmd << "'.\n";
    show_usage();
    return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[RFE] Fatal error: " << ex.what() << "\n";
        return 1;
    } catch (...) {
        std::cerr << "[RFE] Fatal unknown error occurred.\n";
        return 1;
    }
}
