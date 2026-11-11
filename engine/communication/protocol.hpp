// ============================================================================
// protocol.hpp - E2: application-level wire protocol over TCP
//
// Frame format (simple request/response, spec §5):
//
//   +--------------------+-----------------------+------------------------+
//   | header_len (4B BE) | JSON header (UTF-8)   | binary payload         |
//   +--------------------+-----------------------+------------------------+
//
// The JSON header always carries "type". Chunk transfers declare
// "payload_size" in the header; the payload follows as raw binary
// (chunk payloads are NEVER converted to text, spec §19).
//
// Message types: REGISTER, REGISTER_ACK, HEARTBEAT, HEARTBEAT_ACK,
//                STORE_CHUNK, STORE_ACK, GET_CHUNK, CHUNK_RESPONSE,
//                TRANSFER_ERROR.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../third_party/mini_json.hpp"
#include "socket_compat.hpp"

namespace proto {

// Message type constants.
constexpr const char* REGISTER       = "REGISTER";
constexpr const char* REGISTER_ACK   = "REGISTER_ACK";
constexpr const char* HEARTBEAT      = "HEARTBEAT";
constexpr const char* HEARTBEAT_ACK  = "HEARTBEAT_ACK";
constexpr const char* STORE_CHUNK    = "STORE_CHUNK";
constexpr const char* STORE_ACK      = "STORE_ACK";
constexpr const char* GET_CHUNK      = "GET_CHUNK";
constexpr const char* CHUNK_RESPONSE = "CHUNK_RESPONSE";
constexpr const char* TRANSFER_ERROR = "TRANSFER_ERROR";

// Gate-2 Dynamic Peer Registration & Cluster operations
constexpr const char* PEER_REGISTER       = "PEER_REGISTER";
constexpr const char* PEER_REGISTER_ACK   = "PEER_REGISTER_ACK";
constexpr const char* PEER_HEARTBEAT      = "PEER_HEARTBEAT";
constexpr const char* PEER_HEARTBEAT_ACK  = "PEER_HEARTBEAT_ACK";
constexpr const char* QUERY_PEERS         = "QUERY_PEERS";
constexpr const char* QUERY_PEERS_ACK     = "QUERY_PEERS_ACK";

// Phase-2 Task-1 Decentralized Peer-to-Peer Mesh Operations
constexpr const char* P2P_HANDSHAKE       = "P2P_HANDSHAKE";
constexpr const char* P2P_HANDSHAKE_ACK   = "P2P_HANDSHAKE_ACK";
constexpr const char* P2P_HEARTBEAT       = "P2P_HEARTBEAT";
constexpr const char* P2P_HEARTBEAT_ACK   = "P2P_HEARTBEAT_ACK";
constexpr const char* P2P_QUERY_PEERS     = "P2P_QUERY_PEERS";
constexpr const char* P2P_QUERY_PEERS_ACK = "P2P_QUERY_PEERS_ACK";

// Phase-2 Task-2 Distributed Metadata Synchronization & Catalogue Operations
constexpr const char* P2P_METADATA_PUSH        = "P2P_METADATA_PUSH";
constexpr const char* P2P_METADATA_PUSH_ACK    = "P2P_METADATA_PUSH_ACK";
constexpr const char* P2P_METADATA_SUMMARY     = "P2P_METADATA_SUMMARY";
constexpr const char* P2P_METADATA_SUMMARY_ACK = "P2P_METADATA_SUMMARY_ACK";
constexpr const char* P2P_METADATA_REQUEST     = "P2P_METADATA_REQUEST";
constexpr const char* P2P_METADATA_RESPONSE    = "P2P_METADATA_RESPONSE";
constexpr const char* P2P_CATALOGUE_QUERY      = "P2P_CATALOGUE_QUERY";
constexpr const char* P2P_CATALOGUE_QUERY_ACK  = "P2P_CATALOGUE_QUERY_ACK";

// Phase-2 Task-3 Decentralized File Upload & Direct Shard Placement
constexpr const char* P2P_UPLOAD_FILE          = "P2P_UPLOAD_FILE";
constexpr const char* P2P_UPLOAD_FILE_ACK      = "P2P_UPLOAD_FILE_ACK";

// Phase-2 Task-4 Decentralized File Search, Download & RS(3,2) Reconstruction
constexpr const char* P2P_DOWNLOAD_FILE        = "P2P_DOWNLOAD_FILE";
constexpr const char* P2P_DOWNLOAD_FILE_ACK    = "P2P_DOWNLOAD_FILE_ACK";

// Phase-2 Task-5 Autonomous P2P Recovery & Distributed Lease Coordination
constexpr const char* P2P_RECOVERY_LEASE_REQ   = "P2P_RECOVERY_LEASE_REQ";
constexpr const char* P2P_RECOVERY_LEASE_ACK   = "P2P_RECOVERY_LEASE_ACK";
constexpr const char* P2P_REPAIR_TRIGGER       = "P2P_REPAIR_TRIGGER";
constexpr const char* P2P_REPAIR_TRIGGER_ACK   = "P2P_REPAIR_TRIGGER_ACK";

// Phase-3 CLI & Per-Peer Node Telemetry / Graceful Shutdown Operations
constexpr const char* P2P_STORAGE_INFO         = "P2P_STORAGE_INFO";
constexpr const char* P2P_STORAGE_INFO_ACK     = "P2P_STORAGE_INFO_ACK";
constexpr const char* P2P_TRANSFERS_QUERY      = "P2P_TRANSFERS_QUERY";
constexpr const char* P2P_TRANSFERS_QUERY_ACK  = "P2P_TRANSFERS_QUERY_ACK";
constexpr const char* P2P_RECOVERY_STATUS      = "P2P_RECOVERY_STATUS";
constexpr const char* P2P_RECOVERY_STATUS_ACK  = "P2P_RECOVERY_STATUS_ACK";
constexpr const char* P2P_SHUTDOWN             = "P2P_SHUTDOWN";
constexpr const char* P2P_SHUTDOWN_ACK         = "P2P_SHUTDOWN_ACK";

constexpr uint32_t MAX_HEADER_SIZE  = 4 * 1024 * 1024;   // 4 MB header bound for large multi-stripe manifests
constexpr uint32_t MAX_PAYLOAD_SIZE = 128 * 1024 * 1024; // 128 MB payload bound for direct file transfers

enum class RecvStatus { OK, CLOSED, TIMEOUT, IO_ERROR };

// Set send/recv timeouts on a socket (milliseconds).
bool set_timeouts(socket_t fd, int recv_ms, int send_ms);

// Per-socket tuning shared by client and server sockets: TCP_NODELAY (the
// protocol sends small frames and waits for the reply, so Nagle + delayed
// ACK would add up to ~200 ms per round trip on Windows) and, on BSD/macOS,
// SO_NOSIGPIPE.
void tune_socket(socket_t fd);

// Reliable send/recv of exactly n bytes — handles partial socket
// reads/writes (spec §19: never assume one recv() gets a whole message).
bool send_all(socket_t fd, const void* data, size_t n);
RecvStatus recv_all(socket_t fd, void* data, size_t n);

// Send one framed message: JSON header + optional binary payload.
bool send_message(socket_t fd, const json::Json& header,
                  const std::vector<uint8_t>& payload = {});

// Receive one framed message. On OK, `header` and `payload` are filled;
// payload size is taken from the header's "payload_size" field and
// validated against MAX_PAYLOAD_SIZE.
RecvStatus recv_message(socket_t fd, json::Json& header,
                        std::vector<uint8_t>& payload);

}  // namespace proto
