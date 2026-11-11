# Autonomous Engineering Progress Log
## Peer-to-Peer Decentralized File Transfer and Recovery Engine
**Course:** 22ECAW301 | **Team:** 13  
**Active Project Location:** `C:\Users\ariha\Documents\Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine`  
**Report Date:** 2026-11-11  

---

## 1. Executive Summary

This project implements a fully decentralized, genuinely peer-to-peer file transfer, storage, and autonomous durability recovery system in C++17. In accordance with strict decentralization requirements:
- There is **no permanent coordinator**.
- There is **no central database**.
- There is **no central metadata server**.
- Every peer node is an autonomous OS process that maintains its own embedded SQLite database (`storage/peer_<id>/metadata.db`), runs a TCP server/client, advertises itself via UDP discovery beacons, participates in distributed anti-entropy metadata synchronization, and executes autonomous durability recovery when peers fail.

To provide a dual-interface user experience for collegiate demonstration and real-world deployment, the engine provides:
1. **Interface A — Enhanced Native Terminal CLI (`rfe.exe`)**: 14 comprehensive operations covering lifecycle, transfers, storage, recovery, and cluster status with real-time transfer speeds and progress.
2. **Interface B — Local Web Dashboard (`web/`) & Embedded Per-Peer C++ HTTP Server (`http_server.hpp/.cpp`)**: Lightweight Winsock2-based HTTP/1.1 server serving static UI assets (`index.html`, `styles.css`, `app.js`) and versioned REST APIs (`/api/v1/*`) with CSRF protection, file upload, search, and streaming download.

---

## 2. Engineering Verification Suite (`PASS=16 FAIL=0`)

All 16 automated end-to-end regression tests in `controller.exe test` execute and pass with 0 failures:

| Test ID | Name | Subsystem Verified | Result |
|---------|------|--------------------|--------|
| **T01** | Round-Robin Placement | E3 placement across 3 peers with replication factor 2 | **PASS** |
| **T02** | Single Peer Failure & Recovery | E4 failure detection and replica recovery | **PASS** |
| **T03** | Interrupted Transfer Retry | Transaction rollback, temp cleanup, and retry | **PASS** |
| **T04** | SHA-256 Corruption Rejection | Shard checksum verification and secondary replica fetch | **PASS** |
| **T05** | Unrecoverable File Refusal | Refusal of reconstruction when sources are lost | **PASS** |
| **T06** | In-Order Reconstruction | Byte-identical concatenation to original file | **PASS** |
| **T07** | Two-Peer Failure Partition | Recovery of surviving chunks, UNRECOVERABLE detection | **PASS** |
| **T08_M2** | Gate-2 Dynamic Heartbeats | Registration, storage reporting, SUSPECT -> OFFLINE | **PASS** |
| **T09_RS** | RS(3,2) Systematic Coding | Galois Field GF($2^8$) systematic encode & decode | **PASS** |
| **T10_GATE2** | Gate-2 E2E Verification | Dynamic registration, RS(3,2) distribution, peer fetch | **PASS** |
| **T11_PEER_FOUNDATION** | 4-Peer Decentralized Mesh | UDP discovery, TCP handshake, per-peer SQLite, heartbeats | **PASS** |
| **T12_DISTRIBUTED_METADATA** | Distributed Metadata Sync | Push + anti-entropy sync, offline catch-up, DB persistence | **PASS** |
| **T13_P2P_FILE_UPLOAD** | P2P File Upload & Placement | Text/PDF/binary, RS(3,2) 3 distinct peers/stripe, retries | **PASS** |
| **T14_P2P_FILE_DOWNLOAD** | P2P Search & Reconstruction | Any-peer search, offline uploader, corrupted shard rejection | **PASS** |
| **T15_AUTONOMOUS_RECOVERY** | Autonomous Durability Repair | Lease coordination, RS(3,2) rebuild, stale shard cleanup | **PASS** |
| **T16_WEB_DASHBOARD_E2E** | Web Dashboard & HTTP Server | HTTP server, static UI, REST API v1, CSRF, upload & stream | **PASS** |

---

## 3. Key Subsystem Implementations

### 3.1 Embedded Per-Peer C++ HTTP Server (`engine/communication/http_server.hpp/.cpp`)
- **Native Winsock Implementation**: Built directly upon Winsock2 (`WSASocket`, non-blocking I/O, `proto::set_timeouts`, `select`).
- **Streaming Upload Staging**: Incoming multipart/form-data uploads stream directly to disk in `storage/peer_<id>/staging/` in 64 KB buffers, bounding RAM footprint to under 1 MB regardless of file size (up to 128 MB payload bound).
- **Zero-Copy Streaming Download**: Files reconstructed on-the-fly stream directly from disk to the client socket without buffering entire files in memory.
- **Dynamic Port Binding with Fallback**: Peer attempts default UI port (`8080 + (port % 100)`). If already bound, it scans upward for the next free port and stores the bound port in `metadata.db` under key `ui_port`.

### 3.2 Web Dashboard Frontend (`web/index.html`, `web/styles.css`, `web/app.js`)
- **Modern Responsive Design**: Pure vanilla HTML5, CSS3 variables (dark-mode theme with indigo/emerald accents), and ES6 JavaScript with zero external CDN dependencies (works completely offline on air-gapped LANs).
- **9 Core Functional Tabs**:
  1. System Telemetry & Overview
  2. Peer Mesh Topology & Workloads
  3. Drag-and-Drop File Upload with Speedometer
  4. Distributed File Catalogue
  5. File Details & Shard Placement Map Modal
  6. On-Demand Streaming Download & Integrity Check
  7. Active Transfers & Capacity Telemetry
  8. Autonomous Durability Recovery Monitor
  9. Node Settings & Storage Quota Editor

### 3.3 CLI Enhancements (`cli/rfe.cpp`)
- Added support for all 14 requested operations:
  - `rfe storage`: Queries `P2P_STORAGE_INFO`, displays local storage metrics, quotas, and stored shards list.
  - `rfe recovery`: Queries `P2P_RECOVERY_STATUS`, displays started/completed/failed repairs and event history.
  - `rfe shutdown` / `rfe stop`: Sends `P2P_SHUTDOWN` to gracefully stop remote or local peer daemons.
  - `rfe upload`: Displays transfer duration (`ms`) and effective throughput (`MB/s`).
  - `rfe download`: Displays retrieval duration (`ms`) and effective throughput (`MB/s`).
  - Enhanced `rfe transfers`: Queries node transfer telemetry and cluster workload simultaneously.

### 3.4 Wire Protocol Extensions (`engine/communication/protocol.hpp`)
- `P2P_STORAGE_INFO` & `P2P_STORAGE_INFO_ACK`
- `P2P_TRANSFERS_QUERY` & `P2P_TRANSFERS_QUERY_ACK`
- `P2P_RECOVERY_STATUS` & `P2P_RECOVERY_STATUS_ACK`
- `P2P_SHUTDOWN` & `P2P_SHUTDOWN_ACK`

---

## 4. Multi-Process PowerShell Integration Test

The automated end-to-end verification script (`scripts/run_real_world_e2e_audit.ps1`) verifies:
- **Scenario A**: Upload via Web UI on Peer A (`POST /api/v1/uploads`), download via Web UI on Peer D (`GET /api/v1/downloads/{file_id}`), verifying SHA-256 byte-for-byte.
- **Scenario B**: Upload via CLI (`rfe upload`), search and download via Web UI on Peer D.
- **Scenario C**: Upload via Web UI, download via CLI (`rfe download`).
- **Scenario D**: Stop Peer B, observe recovery status via `rfe recovery` and `/api/v1/recovery`.
- **Scenario E**: Restart Peer B, verify rejoin and stale shard cleanup via `rfe storage`.
- Verification of `rfe storage`, `rfe recovery`, and `rfe shutdown`.

---

## 5. Release Packaging & Verification

The packaging script (`scripts/build_release_packages.ps1`) generates:
1. `dist/P2P_File_Engine_Windows_x64.zip` (portable distribution with `peer.exe`, `rfe.exe`, `libcrypto-3-x64.dll`, `web/` assets, setup/start/stop scripts, configs, docs).
2. `dist/P2P_File_Engine_Source.zip` (clean source archive).
3. `dist/SHA256SUMS.txt` (cryptographic integrity hashes).
4. Automated verification in a fresh isolated temporary folder to confirm standalone portability.

---

## 6. Verification Status Summary

| Test Suite / Scope | Execution Command | Result |
| :--- | :--- | :--- |
| **Comprehensive C++ Automated Test Suite** | `.\build\Release\controller.exe test` | **`PASS=16 FAIL=0`** (T01 through T16 all passing) |
| **Real-World Multi-Peer E2E Audit** | `powershell -ExecutionPolicy Bypass -File .\scripts\run_real_world_e2e_audit.ps1` | **`ALL 8 SCENARIOS PASSED (100% VERIFIED)`** |
| **Extracted Portable Package Verification** | Executed automatically in `build_release_packages.ps1` | **`ALL CHECKS PASSED`** (Convergence, upload, download, SHA-256 match, storage, recovery, web health, and shutdown) |

### Checksums of Generated Packages
```text
f8e6a7c5b78c3480610011acccf826784c1cc38e5a80eac053140a8ce4063b62  P2P_File_Engine_Windows_x64.zip
ffec0a52506e92c2e28637e9270fadc982ac829ad2d70dcc8fcf4cacdc622d81  P2P_File_Engine_Source.zip
```
