# Decentralized Resilient File Engine (RFE) — CLI Guide

This document is the comprehensive reference guide for the native C++ Command-Line Interface (`rfe.exe`) of the **Peer-to-Peer Decentralized File Transfer and Recovery Engine**.

Every peer node runs an autonomous server-client daemon (`peer.exe`), maintaining its own local SQLite database, storage directory, UDP discovery, and TCP wire protocol. The `rfe.exe` CLI allows users and automated scripts to interact with the peer-to-peer mesh through any reachable peer without requiring any central coordinator.

---

## 1. Global Usage & Flags

```powershell
rfe <command> [arguments] [--peer <ip:port>] [--output <destination>]
```

### Global Flags:
- `--peer <ip:port>`: Target a specific peer's TCP endpoint (e.g. `127.0.0.1:5701` or `192.168.1.102:5702`). If omitted, the CLI automatically uses the active session endpoint saved in `metadata/session.json` or `config.json`.
- `--output <path>` / `-o <path>`: Specifies custom output destination path for file downloads.
- `help` / `--help` / `-h`: Displays the command usage summary.

### Summary of Supported Operations:
| # | Command | Primary Purpose |
|---|---------|-----------------|
| 1 | `start_peer.ps1` / `peer <ID> <IP> <Port>` | Starts an autonomous peer node process |
| 2 | `rfe connect [ip:port]` | Probes target peer and saves active CLI session |
| 3 | `rfe status [--peer <ip:port>]` | Displays cluster health and catalogue summary |
| 4 | `rfe peers [--peer <ip:port>]` | Displays live peer membership, health states, and workloads |
| 5 | `rfe storage [--peer <ip:port>]` | Queries peer storage telemetry, local shard counts, and quotas |
| 6 | `rfe upload <file-path> [--peer <ip:port>]` | Chunks, RS(3,2)-encodes, and distributes file across mesh |
| 7 | `rfe files [--peer <ip:port>]` | Lists all files registered in the distributed catalogue |
| 8 | `rfe search <query> [--peer <ip:port>]` | Searches catalogue by filename substring or File ID |
| 9 | `rfe info <file-id> [--peer <ip:port>]` | Displays detailed manifest, stripes, and shard placement map |
| 10 | `rfe download <file-id\|filename>` | Retrieves shards directly from storage peers & reconstructs original |
| 11 | `rfe transfers [--peer <ip:port>]` | Displays node transfer telemetry, speeds, and cluster workload |
| 12 | `rfe recovery [--peer <ip:port>]` | Monitors autonomous durability recovery records and statistics |
| 13 | `rfe repair [file-id] [--peer <ip:port>]` | Manually triggers durability recovery scan for missing shards |
| 14 | `rfe shutdown [--peer <ip:port>]` / `rfe stop` | Gracefully shuts down target peer daemon |

---

## 2. Detailed Command Specifications

### 2.1 Starting a Peer Node
Launch a peer daemon process either interactively or directly:
```powershell
# Interactive startup (uses configs/active_peer.json or setup wizard)
.\start_peer.ps1

# Direct execution
.\peer.exe A 127.0.0.1 5701 p2p 5705 --ui-port 8081
```

---

### 2.2 Joining / Connecting to a Peer (`rfe connect`)
Verify connectivity to a peer node and persist it as the default endpoint for subsequent CLI commands:
```powershell
rfe connect 127.0.0.1:5701
```
**Sample Output:**
```
[RFE] Probing Peer at 127.0.0.1:5701...
[RFE] Connected successfully to Peer at 127.0.0.1:5701 (latency: 1 ms)
[RFE] Active session saved.
```

---

### 2.3 Peer Network Membership (`rfe peers`)
Queries the target peer's in-memory and SQLite-backed registry to show live peer discovery, state transitions (`ONLINE`, `SUSPECT`, `OFFLINE`), and transfer workloads:
```powershell
rfe peers --peer 127.0.0.1:5701
```
**Sample Output:**
```
====================================================================================================
PEER ID    ENDPOINT               HEALTH         FAILURES   TRANSFERS     STORAGE AVAIL   LAST SEEN (s)
----------------------------------------------------------------------------------------------------
A          127.0.0.1:5701         ONLINE         0          0/10          97.38 GB        0.1
B          127.0.0.1:5702         ONLINE         0          0/10          97.38 GB        0.2
C          127.0.0.1:5703         ONLINE         0          0/10          97.38 GB        0.2
D          127.0.0.1:5704         ONLINE         0          0/10          97.38 GB        0.1
====================================================================================================
```

---

### 2.4 Peer Storage Telemetry (`rfe storage`)
Queries local storage utilization, disk quota, SQLite database metrics, and lists locally held shards:
```powershell
rfe storage --peer 127.0.0.1:5701
```
**Sample Output:**
```
========================== PEER STORAGE TELEMETRY ==========================
  Peer ID:            A (127.0.0.1:5701)
  Storage Directory:  storage/peer_A/chunks
  SQLite Database:    storage/peer_A/metadata.db (28.00 KB)
  Storage Used:       256.00 KB (262144 bytes)
  Storage Available:  97.38 GB (104562479104 bytes)
  Storage Quota:      UNLIMITED
  Local Shard Count:  4
  Distributed Files:  2
----------------------------------------------------------------------------
  LOCAL STORED SHARDS (First 4 of 4):
    [ 1] F_7B89A120-S000-D0                   (64.00 KB)
    [ 2] F_7B89A120-S001-P0                   (64.00 KB)
    [ 3] F_C3D4E5F6-S000-D1                   (64.00 KB)
    [ 4] F_C3D4E5F6-S000-P0                   (64.00 KB)
============================================================================
```

---

### 2.5 Decentralized File Upload (`rfe upload`)
Takes any local file, computes its SHA-256 hash, splits it into stripes of 2 data shards, computes 1 parity shard via Reed-Solomon RS(3,2), places the 3 shards across 3 distinct online peers, and commits the manifest to the distributed SQLite catalogue:
```powershell
rfe upload "C:\Files\sample_document.pdf" --peer 127.0.0.1:5701
```
**Sample Output:**
```
[RFE] Uploading 'sample_document.pdf' (160.00 KB) via peer 127.0.0.1:5701...
===================== DECENTRALIZED UPLOAD SUCCESS =====================
  Entry Peer:     A (127.0.0.1:5701)
  File ID:        F_8A2C3E10
  Filename:       sample_document.pdf
  Original Size:  160.00 KB (163840 bytes)
  SHA-256:        9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
  Shard Size:     64.00 KB
  RS Coding:      RS(3,2) (2 data + 1 parity per stripe)
  Stripes/Shards: 2 stripes / 6 shards
  Transfer Time:  142 ms
  Transfer Speed: 1.10 MB/s
  Status:         AVAILABLE (Version 1)
------------------------------------------------------------------------
  SHARD DISTRIBUTION ACROSS PEERS:
    [Stripe 0] D0 -> B, D1 -> C, P0 -> D
    [Stripe 1] D0 -> C, D1 -> D, P0 -> B
========================================================================
```

---

### 2.6 Distributed File Catalogue Listing (`rfe files`)
Queries the contacted peer's local catalogue for all known files, showing real-time health and hosting peer summaries:
```powershell
rfe files --peer 127.0.0.1:5704
```
**Sample Output:**
```
==================================== DISTRIBUTED FILE CATALOGUE ====================================
FILE ID       FILENAME                 SIZE        LIVE STATUS   SHARDS ONL  HOSTING PEERS         SHA-256
----------------------------------------------------------------------------------------------------
F_8A2C3E10    sample_document.pdf      160.00 KB   AVAILABLE     6/6         B,C,D                 9f86d081884c7d65...
F_3F19A2BC    telemetry_data.bin       85.50 KB    AVAILABLE     3/3         A,B,C                 3a5b6c7d8e9f0012...
====================================================================================================
```

---

### 2.7 Searching the Catalogue (`rfe search`)
Performs case-insensitive substring search by filename or exact lookup by File ID:
```powershell
rfe search "document" --peer 127.0.0.1:5703
```
**Sample Output:**
```
===================== DECENTRALIZED CATALOGUE SEARCH =====================
  Query:          "document"
  Contacted Peer: C (127.0.0.1:5703)
  Matches Found:  1
--------------------------------------------------------------------------
  File ID:        F_8A2C3E10
  Filename:       sample_document.pdf
  File Size:      160.00 KB (163840 bytes)
  SHA-256:        9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
  Availability:   AVAILABLE (Reconstructable: YES, Online Shards: 6/6)
  Storage Peers:  B, C, D
  Shard Map:      F_8A2C3E10-S000-D0@B | F_8A2C3E10-S000-D1@C | F_8A2C3E10-S000-P0@D
                  F_8A2C3E10-S001-D0@C | F_8A2C3E10-S001-D1@D | F_8A2C3E10-S001-P0@B
==========================================================================
```

---

### 2.8 Manifest & Shard Placement Details (`rfe info`)
Inspects full stripe composition, shard hashes, shard types (`DATA` vs `PARITY`), and storage peer locations:
```powershell
rfe info F_8A2C3E10 --peer 127.0.0.1:5702
```
**Sample Output:**
```
============================ FILE MANIFEST INFO ============================
  File ID:         F_8A2C3E10
  Filename:        sample_document.pdf
  Original Size:   160.00 KB (163840 bytes)
  Full SHA-256:    9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
  Live Status:     AVAILABLE (Reconstructable: YES)
  Coding Policy:   RS(3,2) Systematic Reed-Solomon
  Data Shard Size: 64.00 KB
  Total Stripes:   2
  Total Shards:    6
  Uploader Peer:   A
----------------------------------------------------------------------------
  SHARDS DISTRIBUTION MAP:
    [S000] F_8A2C3E10-S000-D0  Type: DATA     Orig: 65536    Peer: B        SHA: 7a8b9c0d1e2f3a4b...
    [S000] F_8A2C3E10-S000-D1  Type: DATA     Orig: 65536    Peer: C        SHA: 1f2e3d4c5b6a7081...
    [S000] F_8A2C3E10-S000-P0  Type: PARITY   Orig: 65536    Peer: D        SHA: 9081726354453423...
    [S001] F_8A2C3E10-S001-D0  Type: DATA     Orig: 32768    Peer: C        SHA: abcd1234ef567890...
    [S001] F_8A2C3E10-S001-D1  Type: DATA     Orig: 0        Peer: D        SHA: fedcba0987654321...
    [S001] F_8A2C3E10-S001-P0  Type: PARITY   Orig: 32768    Peer: B        SHA: 5544332211009988...
============================================================================
```

---

### 2.9 Decentralized File Download & Reconstruction (`rfe download`)
Retrieves any 2 valid shards per stripe directly from storage peers over TCP, decodes RS(3,2) on-the-fly, strips any stripe padding, and verifies full-file SHA-256 integrity:
```powershell
rfe download F_8A2C3E10 --peer 127.0.0.1:5704 --output "output/restored_document.pdf"
```
**Sample Output:**
```
===================== DECENTRALIZED DOWNLOAD COMPLETE =====================
  Contacted Peer:       127.0.0.1:5704
  File ID:              F_8A2C3E10
  File Name:            sample_document.pdf
  Original File Size:   160.00 KB (163840 bytes)
  Downloaded Bytes:     160.00 KB (163840 bytes)
  Retrieved Shards:     4
  Reconstructed Stripes:2/2
  Failed Connections:   0
  Corrupted Rejected:   0
  Transfer Time:        68 ms
  Transfer Speed:       2.35 MB/s
  Integrity Check:      VERIFIED (SHA-256: 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08)
  Final Destination:    output/restored_document.pdf
---------------------------------------------------------------------------
  SHARD RETRIEVAL LOG:
    [Stripe 0] Fetched D0 (B) & D1 (C) -> Systematic Data reconstruction OK
    [Stripe 1] Fetched D0 (C) & P0 (B) -> RS(3,2) algebraic decode for D1 OK
===========================================================================
```

---

### 2.10 Active Workloads & Transfer Telemetry (`rfe transfers`)
Displays real-time upload and download metrics, bandwidth speeds, elapsed durations, and peer capacity saturation:
```powershell
rfe transfers --peer 127.0.0.1:5701
```
**Sample Output:**
```
======================== NODE TRANSFER TELEMETRY ========================
  Peer ID:            A (127.0.0.1:5701)
  Active Uploads:     0
  Active Downloads:   0
-------------------------------------------------------------------------
DIR     STATUS      FILENAME                 SIZE        SPEED       ELAPSED
-------------------------------------------------------------------------
UPLOAD  COMPLETED   sample_document.pdf      160.00 KB   1.10 MB/s   142 ms
DOWNLOAD COMPLETED  telemetry_data.bin       85.50 KB    2.40 MB/s   35 ms
=========================================================================

======================== ACTIVE WORKLOAD & TRANSFERS ========================
PEER ID   ENDPOINT               HEALTH        ACTIVE / CAPACITY BUSY STATE
-----------------------------------------------------------------------------
A         127.0.0.1:5701         ONLINE        0 / 10            IDLE / ACCEPTING
B         127.0.0.1:5702         ONLINE        0 / 10            IDLE / ACCEPTING
C         127.0.0.1:5703         ONLINE        0 / 10            IDLE / ACCEPTING
D         127.0.0.1:5704         ONLINE        0 / 10            IDLE / ACCEPTING
=============================================================================
```

---

### 2.11 Autonomous Recovery Monitoring (`rfe recovery`)
Queries the distributed recovery log recorded across SQLite databases when nodes fail:
```powershell
rfe recovery --peer 127.0.0.1:5701
```
**Sample Output:**
```
======================== AUTONOMOUS RECOVERY STATUS ========================
  Peer ID:            A (127.0.0.1:5701)
  Started / Active:   0
  Completed Repairs:  2
  Failed / Aborted:   0
  Total Event Logs:   2
----------------------------------------------------------------------------
STATUS      SHARD ID                    FAILED      TARGET      REBUILDER   REASON
----------------------------------------------------------------------------
COMPLETED   F_8A2C3E10-S000-D0          B           D           A           RS(3,2) rebuilt & placed on D
COMPLETED   F_8A2C3E10-S001-P0          B           C           A           RS(3,2) rebuilt & placed on C
============================================================================
```

To explicitly request a durability check and recovery scan on the target peer:
```powershell
rfe recovery --peer 127.0.0.1:5701 --trigger
```

---

### 2.12 On-Demand Durability Repair (`rfe repair`)
Manually forces an autonomous recovery evaluation for a specific file or all missing shards:
```powershell
rfe repair F_8A2C3E10 --peer 127.0.0.1:5701
```
**Sample Output:**
```
===================== DECENTRALIZED RECOVERY SCAN =====================
  Contacted Peer:  A
  Shards Repaired: 0
  RECOVERY RECORDS:
    [COMPLETED] F_8A2C3E10-S000-D0 (failed=B -> target=D by A) - RS(3,2) rebuilt & placed on D
=======================================================================
```

---

### 2.13 Cluster Summary Status (`rfe status`)
Provides a rapid bird's-eye overview of cluster health, reachable nodes, and aggregate storage capacity:
```powershell
rfe status --peer 127.0.0.1:5701
```
**Sample Output:**
```
============================ CLUSTER STATUS ============================
  Contacted Peer:    ONLINE (127.0.0.1:5701)
  Cluster Health:    HEALTHY
  Peers:             4 total (4 ONLINE, 0 SUSPECT, 0 OFFLINE)
  Total Available:   389.52 GB
========================================================================
```

---

### 2.14 Graceful Peer Daemon Shutdown (`rfe shutdown` / `rfe stop`)
Gracefully notifies a local or remote peer daemon to flush its SQLite database, broadcast goodbye beacons, close active TCP connections, and exit cleanly:
```powershell
rfe shutdown --peer 127.0.0.1:5704
```
**Sample Output:**
```
[RFE] Graceful shutdown initiated for peer D (127.0.0.1:5704)
```

---

## 3. Exit Codes & Automation Scripting

When invoking `rfe.exe` inside PowerShell or batch scripts, check `$LASTEXITCODE`:
- `0`: Operation succeeded completely.
- `1`: Operation failed (e.g. peer unreachable, SHA-256 verification failed, unrecoverable file state, invalid argument).

Example automated script snippet:
```powershell
$output = (& .\rfe.exe upload "sample.txt" --peer 127.0.0.1:5701 2>&1) | Out-String
if ($LASTEXITCODE -ne 0) {
    Write-Error "Upload failed: $output"
    exit 1
}
Write-Host "Uploaded successfully!"
```
