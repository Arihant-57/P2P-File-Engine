# Decentralized Resilient File Engine (RFE) — Web Dashboard Guide

This document describes the **Local Web Dashboard & Embedded Per-Peer C++ HTTP Server** architecture, user interface, and versioned REST API.

---

## 1. Architecture Overview

In accordance with our strict decentralized architecture:
- **No Central Web Server**: There is no dedicated or centralized web server node.
- **Embedded Per-Peer HTTP Server**: Every `peer.exe` process embeds a high-performance, multi-threaded C++17 HTTP/1.1 server running in the background.
- **Local Loopback by Default**: By default, the server binds to `127.0.0.1:<ui_port>` to protect sensitive peer operations from untrusted LAN traffic.
- **Optional LAN Mode**: Nodes can be configured with `--ui-lan` and `--ui-token <token>` to safely expose the dashboard to other team members on the LAN.
- **Native Static Asset Delivery**: All HTML, CSS, and JS files (`web/index.html`, `web/styles.css`, `web/app.js`) are served directly from memory or disk without requiring Node.js, Python, or Apache/Nginx.

```
       +-------------------------------------------------------+
       |                  Web Browser / User                   |
       +-------------------------------------------------------+
                                  |
                                  | HTTP/1.1 (GET/POST)
                                  v
+-----------------------------------------------------------------------+
|  Autonomous Peer Daemon (peer.exe)                                    |
|                                                                       |
|  +-------------------------+      +--------------------------------+  |
|  | Embedded C++ HttpServer | <--> | REST API Dispatcher (/api/v1)  |  |
|  +-------------------------+      +--------------------------------+  |
|               |                                   |                   |
|               v                                   v                   |
|  +-------------------------+      +--------------------------------+  |
|  | Static Assets (web/*)   |      | Peer Engine Components         |  |
|  | - index.html            |      | - PeerRegistry (Mesh Topo)     |  |
|  | - styles.css            |      | - DatabaseManager (SQLite)     |  |
|  | - app.js                |      | - DynamicDistributor (Upload)  |  |
|  +-------------------------+      | - ClientDownload (RS Decode)   |  |
|                                   | - P2PManager (Autonomous Rec)  |  |
|                                   | - PeerTelemetry (Transfers)    |  |
|                                   +--------------------------------+  |
+-----------------------------------------------------------------------+
```

---

## 2. Configuration & Startup

### Default Port Derivation
When not explicitly configured, the Web Dashboard UI port is derived automatically from the peer's TCP port:
- Peer Port `5001` or `5701` -> UI Port `8080` (or `8081`)
- Peer Port `5002` or `5702` -> UI Port `8082`
- Peer Port `5003` or `5703` -> UI Port `8083`
- Peer Port `5004` or `5704` -> UI Port `8084`
- Custom Port `N` -> `8080 + (N % 100)`

If the derived port is occupied, the HTTP server scans upward for the next available port and records the bound port in the peer's local SQLite database (`storage/peer_<id>/metadata.db`).

### Command-Line Arguments:
```powershell
# Specify explicit UI port
.\peer.exe A 127.0.0.1 5701 p2p 5705 --ui-port 8090

# Expose dashboard to LAN with token security
.\peer.exe A 192.168.1.101 5701 p2p 5705 --ui-lan --ui-token secret123

# Headless mode (disable Web Dashboard completely)
.\peer.exe A 127.0.0.1 5701 p2p 5705 --no-ui
```

### Accessing the Dashboard:
Open any standard modern web browser (Edge, Chrome, Firefox) and navigate to:
```
http://127.0.0.1:8080
```
(Or whichever port the peer displayed in stdout: `[peer A] ONLINE ... ui=http://127.0.0.1:8080`)

---

## 3. Web Dashboard Sections

The dashboard is designed as a dark-mode, high-productivity single-page web app with 9 distinct tabs:

### 1. Overview & System Telemetry
- Real-time node identity, IP address, and P2P listen port.
- Node uptime, total memory/storage usage, and cluster health badge (`HEALTHY` or `DEGRADED`).
- Dynamic gauges for Storage Quota vs Available Disk Space.
- Live network peer counters (Online, Suspect, Offline).

### 2. Peer Network Topology
- Interactive grid/matrix of all discovered peer nodes.
- Shows Peer ID, IP:Port endpoint, current health status (`ONLINE`, `SUSPECT`, `OFFLINE`).
- Shows failure counters, active transfer workloads, and available storage contributions.
- Real-time heartbeat recency (`Last seen X ms ago`).

### 3. Drag-and-Drop File Upload
- Drag-and-drop zone supporting arbitrary files (text, images, binaries, documents).
- File size inspection and pre-upload hash computation.
- Real-time upload progress bar with calculated transfer speed (`MB/s`) and elapsed milliseconds.
- Visual breakdown of Reed-Solomon RS(3,2) chunking: data shards (`D0`, `D1`) and parity shard (`P0`) with peer destination tags.

### 4. Distributed File Catalogue
- Live searchable table of all files published across the decentralized mesh.
- Search filter with instant debounced querying.
- Shows File ID, Filename, Original File Size, Health Status badge (`AVAILABLE`, `DEGRADED`, `UNRECOVERABLE`).
- Online Shard Ratio (e.g. `6/6` or `4/6`).
- Quick actions: "Details" (opens Manifest Modal) and "Download".

### 5. File Details & Shard Placement Modal
- Complete manifest inspection: original SHA-256 hash, chunk size, total stripes, and total shards.
- Stripe-by-stripe visualization table showing each shard (`-D0`, `-D1`, `-P0`), shard size, shard SHA-256, and the hosting peer.
- Identifies whether each shard is currently accessible on an online peer.

### 6. Streaming Download & Integrity Check
- Direct browser download link `/api/v1/downloads/{file_id}`.
- Contacted peer retrieves required surviving shards directly from storage peers over TCP, decodes RS(3,2), removes padding, verifies SHA-256, and streams bytes to the browser.
- Browser triggers native "Save As" file download.

### 7. Active Transfers & Workload Monitor
- Live log of in-progress, completed, and failed uploads and downloads.
- Shows transfer ID, direction, filename, transfer speed (`MB/s`), and duration (`ms`).
- Shows node capacity utilization.

### 8. Autonomous Durability Recovery Monitor
- Real-time feed of background repair actions orchestrated by the decentralized mesh.
- Displays recovery ID, failed peer ID, rebuilder peer, target storage peer, and repair status (`STARTED`, `COMPLETED`, `FAILED`).
- Includes a "Scan & Repair Now" button triggering diagnostic durability scans.

### 9. Node Settings & Quota Manager
- Configure maximum disk space allocation (in MB) contributed to the network.
- Add dynamic seed peers (`<ip:port>`) to assist UDP discovery on segmented networks.
- Real-time updates stored atomically in the peer's SQLite database.

---

## 4. Versioned REST API Reference (`/api/v1`)

All endpoints are rooted at `/api/v1` and return JSON payloads (except streaming download).

### 4.1 Node Health
- **Endpoint**: `GET /api/v1/health`
- **Auth**: None
- **Response**:
```json
{
  "status": "OK",
  "peer_id": "A",
  "uptime_seconds": 128
}
```

---

### 4.2 Node Information
- **Endpoint**: `GET /api/v1/node`
- **Auth**: Required if `ui_token` is enabled
- **Response**:
```json
{
  "status": "OK",
  "peer_id": "A",
  "address": "127.0.0.1",
  "port": 5701,
  "mode": "p2p",
  "discovery_port": 5705,
  "bound_ui_port": 8080,
  "storage_dir": "storage/peer_A/chunks",
  "db_path": "storage/peer_A/metadata.db",
  "uptime_seconds": 145,
  "active_transfers": 0,
  "online_peers": 4,
  "suspect_peers": 0,
  "offline_peers": 0,
  "total_discovered_peers": 4,
  "available_storage_bytes": 104562479104,
  "used_storage_bytes": 262144,
  "max_storage_bytes": 0
}
```

---

### 4.3 Peer Cluster List
- **Endpoint**: `GET /api/v1/peers`
- **Response**:
```json
{
  "status": "OK",
  "peer_id": "A",
  "peers": [
    {
      "peer_id": "A",
      "address": "127.0.0.1",
      "port": 5701,
      "health": "ONLINE",
      "consecutive_failures": 0,
      "active_transfers": 0,
      "transfer_capacity": 10,
      "available_storage_bytes": 104562479104,
      "last_seen_seconds_ago": 0.1
    },
    {
      "peer_id": "B",
      "address": "127.0.0.1",
      "port": 5702,
      "health": "ONLINE",
      "consecutive_failures": 0,
      "active_transfers": 0,
      "transfer_capacity": 10,
      "available_storage_bytes": 104562479104,
      "last_seen_seconds_ago": 0.2
    }
  ]
}
```

---

### 4.4 File Catalogue Query & Search
- **Endpoint**: `GET /api/v1/files` or `GET /api/v1/search?q=<query>`
- **Response**:
```json
{
  "status": "OK",
  "peer_id": "A",
  "query": "doc",
  "files": [
    {
      "file_id": "F_8A2C3E10",
      "original_name": "sample_document.pdf",
      "file_size": 163840,
      "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
      "version": 1,
      "live_status": "AVAILABLE",
      "reconstructable": true,
      "online_shards_count": 6,
      "total_shards_count": 6,
      "hosting_peers": ["B", "C", "D"],
      "uploader_peer_id": "A"
    }
  ]
}
```

---

### 4.5 File Manifest Details
- **Endpoint**: `GET /api/v1/files/{file_id}`
- **Response**: Returns full manifest including all stripes and shards with their hosting peer IDs and location statuses.

---

### 4.6 File Upload
- **Endpoint**: `POST /api/v1/uploads`
- **Headers**:
  - `Content-Type: multipart/form-data; boundary=...`
  - `X-RFE-Request: 1` (CSRF Protection Header)
- **Response**:
```json
{
  "status": "OK",
  "peer_id": "A",
  "transfer_id": "TX_UP_1",
  "file_id": "F_8A2C3E10",
  "filename": "sample_document.pdf",
  "file_size": 163840,
  "elapsed_ms": 142,
  "speed_bps": 1153802,
  "manifest": { ... }
}
```

---

### 4.7 Streaming File Download
- **Endpoint**: `GET /api/v1/downloads/{file_id}`
- **Response**:
  - `Content-Type: application/octet-stream`
  - `Content-Disposition: attachment; filename="..."`
  - Raw binary stream of the reconstructed file.

---

### 4.8 Storage Telemetry
- **Endpoint**: `GET /api/v1/storage`
- **Response**: Returns used bytes, available bytes, disk quota, local stored shards count, catalogue files count, and list of locally held `.chunk` files.

---

### 4.9 Transfers & Events Telemetry
- **Endpoint**: `GET /api/v1/transfers` -> List of active and completed transfer records.
- **Endpoint**: `GET /api/v1/events` -> Chronological peer event logs.
- **Endpoint**: `GET /api/v1/recovery` -> Durability recovery records.
- **Endpoint**: `POST /api/v1/recovery/scan` -> Trigger diagnostic recovery scan.

---

### 4.10 Settings
- **Endpoint**: `POST /api/v1/settings`
- **Headers**: `X-RFE-Request: 1`, `Content-Type: application/json`
- **Body**:
```json
{
  "max_storage_mb": 1024,
  "seed_peer": "192.168.1.102:5702"
}
```

---

## 5. Security & Defense Baseline

1. **CSRF Protection**: All state-modifying requests (`POST`) require the custom header `X-RFE-Request: 1`. Cross-site scripting in other browser tabs cannot forge this header without explicit CORS pre-flight authorization.
2. **Origin Validation**: When LAN access is disabled (`--ui-lan` omitted), `Origin` and `Host` headers pointing to non-loopback domains are strictly rejected with HTTP 403 Forbidden.
3. **Optional Token Authentication**: When running in multi-user LAN mode (`--ui-lan`), passing `--ui-token <token>` enforces that all API requests present `X-RFE-Token: <token>`.
4. **Path Traversal Sanitization**: All file upload names and shard IDs are stripped of any directory traversal sequences (`..`, `/`, `\`, `:`). Shards are stored strictly in `storage/peer_<id>/chunks/`.
5. **Streaming Disk Staging**: Uploads are streamed directly to disk in `storage/peer_<id>/staging/` in 64 KB increments with strict payload bounds (up to 128 MB), preventing memory exhaustion attacks.
