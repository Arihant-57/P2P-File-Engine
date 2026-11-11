# Release Notes — v2.0.0 (Final Decentralized Release)

**Project:** Peer-to-Peer Decentralized File Transfer and Recovery Engine  
**Target Platform:** Windows 10 / 11 (x64)  
**C++ Standard:** ISO C++17 (MSVC / Visual Studio 2022, CMake 3.16+)

---

## 1. Release Highlights

Version **2.0.0** completes the transition from a coordinator-assisted cluster to a **100% decentralized, autonomous peer-to-peer file transfer and durability recovery engine**:

1. **Zero Central Coordinator / Zero Single Point of Failure:**
   - Every node runs an identical `peer.exe` binary acting simultaneously as a multi-threaded **TCP server**, **TCP client**, **UDP discovery beacon**, **SQLite metadata catalogue**, and **Reed-Solomon RS(3,2) recovery agent**.
   - Any participating peer can accept file uploads, answer file searches, orchestrate downloads, and repair lost shards when a peer goes offline.

2. **Hybrid LAN Discovery & Health State Machine (Task 1):**
   - Automatic peer discovery over UDP (`255.255.255.255`, directed `/24` subnet broadcast, and loopback) combined with explicit `--seed <ip:port>` TCP/UDP probing for Wi-Fi networks with AP client isolation.
   - Distributed heartbeat monitoring with deterministic state transitions (`ONLINE` → `SUSPECT` → `OFFLINE` → `ONLINE` rejoin).

3. **Synchronized Per-Peer SQLite Catalogue (Task 2):**
   - Each peer maintains its own ACID SQLite database (`metadata.db` in WAL mode) tracking `peers`, `files`, `stripes`, `shards`, `shard_locations`, `recovery_records`, and `metadata_sync_log`.
   - Event-driven `P2P_METADATA_PUSH` broadcast plus periodic anti-entropy summary reconciliation (`P2P_METADATA_SUMMARY` / `P2P_METADATA_REQUEST`) keep all online and rejoining peers converged.

4. **Adaptive Chunking & Systematic Reed-Solomon RS(3,2) Over $\text{GF}(2^8)$ (Task 3):**
   - Files of any size (`0` bytes up to multi-megabyte binary streams) are split into adaptive stripes (`64 KB`, `256 KB`, or `1 MB` per data shard) and encoded into $k=2$ data shards (`D0`, `D1`) and $m=1$ parity shard (`P0`).
   - All 3 shards of each stripe are distributed across **3 distinct eligible online peers** with atomic `.tmp` → `.chunk` commit and per-shard SHA-256 verification.

5. **Any-Peer Search, Download & Erasure Reconstruction (Task 4):**
   - Users can search by filename or File ID (`rfe search`) and download (`rfe download`) through **any** participating peer—even when the original uploader peer is offline.
   - If 1 shard in a stripe is unreachable or fails SHA-256 verification on disk, the missing data shard is transparently reconstructed using $\text{GF}(2^8)$ matrix inversion from the remaining 2 shards.

6. **Autonomous Decentralized Durability Recovery (Task 5):**
   - When a storage peer transitions to `OFFLINE`, surviving peers deterministically select a recovery owner (`DJB2(file_id) % online_peers.size()`), acquire a distributed lease (`P2P_RECOVERY_LEASE_REQ`), fetch 2 surviving shards, reconstruct the missing shard via RS(3,2), place it on a 4th distinct peer, and broadcast the incremented manifest version—without any manual command.
   - When a previously failed peer restarts and rejoins, it synchronizes the newer manifest version and automatically purges superseded stale `.chunk` files.

7. **Independent Audit & Engineering Hardening (Final Phase):**
   - Added `--storage-dir <path>` and JSON `"storage_dir"` configuration support for portable deployments.
   - Preserved user invocation working directory (`invocation_cwd`) in `rfe.exe` so relative upload and `--output` paths resolve accurately from any directory.
   - Fixed `0`-byte (empty) file upload/download handling and raised wire frame bounds (`MAX_HEADER_SIZE = 4 MB`, `MAX_PAYLOAD_SIZE = 128 MB`) and socket timeouts (`60s–120s`) for large multi-stripe files.
   - Eliminated multi-stripe concurrent manifest version race during autonomous recovery by aligning primary recovery ownership per `file_id` and merging higher-version `shard_locations` in SQLite.
   - Added heartbeat keepalive bursts during large multi-stripe repairs so heavy I/O never starves peer heartbeat timers.

---

## 2. Distribution Archives (`dist/`)

| Archive | Purpose |
| :--- | :--- |
| `dist/P2P_File_Engine_Windows_x64.zip` | Ready-to-run Windows x64 portable package (`peer.exe`, `rfe.exe`, `libcrypto-3-x64.dll`, interactive `setup_peer.ps1` wizard, start/stop scripts, sample configs, and user documentation). |
| `dist/P2P_File_Engine_Source.zip` | Clean, self-contained C++17 source distribution with CMake build configuration, bundled SQLite 3 amalgamation, bundled OpenSSL 3 x64 SDK, automated test suites (`T01`–`T15`), E2E scripts, and full engineering documentation. |
| `dist/SHA256SUMS.txt` | SHA-256 checksums for verifying package integrity. |
