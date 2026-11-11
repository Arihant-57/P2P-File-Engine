# Peer-to-Peer Decentralized File Transfer and Recovery Engine

[![Release](https://img.shields.io/badge/Release-v1.0.0-blue.svg)](https://github.com/Arihant-57/P2P-File-Engine/releases/latest)
[![C++17](https://img.shields.io/badge/Language-ISO%20C%2B%2B17-00599C.svg)](https://en.cppreference.com/w/cpp/17)
[![Platform](https://img.shields.io/badge/Platform-Windows%20x64-0078D6.svg)](https://www.microsoft.com/windows)
[![Tests](https://img.shields.io/badge/Tests-16%2F16%20Passed-brightgreen.svg)](#automated-testing)
[![Erasure Coding](https://img.shields.io/badge/Erasure%20Coding-RS(3%2C2)%20over%20GF(2%5E8)-orange.svg)](#reed-solomon-rs32)
[![Persistence](https://img.shields.io/badge/Database-SQLite%203.54.1%20WAL-lightgrey.svg)](#distributed-metadata)

**Course:** 22ECAW301 — Capstone Project | **Team 13**  
**Repository:** [https://github.com/Arihant-57/P2P-File-Engine](https://github.com/Arihant-57/P2P-File-Engine)  

---

## Quick Navigation

- [**Download Ready-to-Run Windows App**](#1-download-ready-to-run-windows-application) *(For end-users & classmates)*
- [**Complete Technical Project Guide (PDF)**](#2-complete-project-guide--documentation) *(21-page architectural manual)*
- [**Project Overview & Key Guarantees**](#3-project-overview)
- [**Dual Interfaces: Web Dashboard & CLI**](#4-interfaces)
- [**Developer Setup & Building from Source**](#5-building-from-source)
- [**Automated Verification Suite (T01–T16)**](#6-automated-testing)
- [**Four-Laptop LAN Demonstration Script**](#7-four-laptop-lan-demonstration)
- [**Collaboration Guide for Teammates**](#8-collaboration-guide-for-teammates)

---

## 1. Download Ready-to-Run Windows Application

For classmates, friends, and evaluators who want to run the application immediately **without installing Visual Studio, CMake, Python, or a database server**:

| Download Asset | Description | Size |
| :--- | :--- | :--- |
| [**`P2P_File_Engine_Windows_x64.zip`**](https://github.com/Arihant-57/P2P-File-Engine/releases/latest/download/P2P_File_Engine_Windows_x64.zip) | Standalone Windows portable distribution (`peer.exe`, `rfe.exe`, Web UI, setup wizard, DLLs) | ~4.2 MB |
| [**`P2P_File_Engine_Complete_Project_Guide.pdf`**](https://github.com/Arihant-57/P2P-File-Engine/releases/latest/download/P2P_File_Engine_Complete_Project_Guide.pdf) | Comprehensive 21-page educational handbook, diagrams, math, and source code walkthrough | ~750 KB |
| [**`P2P_File_Engine_Source.zip`**](https://github.com/Arihant-57/P2P-File-Engine/releases/latest/download/P2P_File_Engine_Source.zip) | Clean source code bundle for independent offline compilation | ~5.8 MB |
| [**`SHA256SUMS.txt`**](https://github.com/Arihant-57/P2P-File-Engine/releases/latest/download/SHA256SUMS.txt) | Cryptographic verification digests | 271 B |

### How to Run in 3 Minutes:
1. Download and extract **`P2P_File_Engine_Windows_x64.zip`** to any directory (e.g. `C:\P2P_Engine\`).
2. Open PowerShell in that folder and run the configuration wizard:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\setup_peer.ps1
   ```
   *(Select a Peer ID like `A`, `B`, `C`, or `D`, and press Enter to accept the defaults).*
3. Start the peer daemon:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\start_peer.ps1
   ```
4. Open your web browser:
   ```text
   http://127.0.0.1:8080
   ```
   *You will see the live Web Dashboard with peer discovery, drag-and-drop file upload, distributed catalogue, and recovery monitor!*

---

## 2. Complete Project Guide & Documentation

We have prepared comprehensive documentation for students, faculty, and systems engineers:

- [**`P2P_File_Engine_Complete_Project_Guide.pdf`**](docs/COMPLETE_PROJECT_GUIDE.md) — The master 21-page technical handbook covering CS fundamentals (OOP, DSA, OS, Networks, DBMS, Cryptography), Reed-Solomon math proofs, and workflows.
- [**`ARCHITECTURE.md`**](ARCHITECTURE.md) — End-to-end decentralized architecture, SQLite schema, wire protocol, and workflows.
- [**`CLI_GUIDE.md`**](CLI_GUIDE.md) — Complete user manual for all 14 CLI commands.
- [**`WEB_DASHBOARD_GUIDE.md`**](WEB_DASHBOARD_GUIDE.md) — Embedded Web Dashboard user guide, REST API v1 specification, and security design.
- [**`PORTABLE_RELEASE_GUIDE.md`**](PORTABLE_RELEASE_GUIDE.md) — Portable extraction and deployment guide.
- [**`FOUR_LAPTOP_DEMO_GUIDE.md`**](FOUR_LAPTOP_DEMO_GUIDE.md) — 16-step live demonstration checklist for faculty evaluators.
- [**`FINAL_ENGINEERING_AUDIT.md`**](FINAL_ENGINEERING_AUDIT.md) — Independent engineering audit report and production readiness certification.

---

## 3. Project Overview

The **Peer-to-Peer Decentralized File Transfer and Recovery Engine** is an institutional-grade, fault-tolerant distributed storage and file recovery system engineered entirely in modern C++17 for Local Area Networks (LAN).

Every participating computer (`peer.exe`) operates as **both** a framed TCP server and a TCP client, maintains its own isolated local SQLite database (`storage/peer_<ID>/metadata.db`) and shard store (`storage/peer_<ID>/chunks/`), discovers peers automatically via UDP broadcast beacons (`UDPBeacon`) and TCP gossip, monitors peer health (`ONLINE` $\rightarrow$ `SUSPECT` $\rightarrow$ `OFFLINE`), synchronizes file manifests via push broadcast and periodic anti-entropy reconciliation, and autonomously repairs lost shards using `RS(3,2)` erasure coding when any peer fails.

### Key Architectural Guarantees:
- **No Permanent Central Coordinator:** Any peer can accept file uploads, answer catalogue searches, orchestrate downloads, and participate in durability repair.
- **No Central Database Server:** Each peer owns an embedded SQLite 3.54.1 database (`storage/peer_<ID>/metadata.db`).
- **Any-Peer Upload & Any-Peer Download:** Upload through Peer A and search/download through Peer D, even if Peer A is completely powered off.
- **Single-Peer Fault Tolerance (`RS(3,2)`):** Every stripe splits data into $K=2$ data shards ($D_0, D_1$) and $M=1$ parity shard ($P_0$) placed on **3 distinct physical peers**. Any 2 of the 3 shards reconstruct the original stripe byte-for-byte.
- **Autonomous Peer-to-Peer Durability Repair:** When a peer transitions to `OFFLINE`, surviving peers deterministically coordinate a recovery lease (`DJB2(file_id) % N`), reconstruct missing shards via `RS(3,2)`, and store them on a healthy 4th peer.

---

## 4. Interfaces

### Interface A: Local Web Dashboard & REST API
Every `peer.exe` embeds an asynchronous Winsock HTTP/1.1 multi-threaded server serving a responsive single-page application (`http://127.0.0.1:8080`):
- **9 Functional Sections:** Node Overview, Peer Mesh Topology, Drag-and-Drop Upload Studio, Distributed File Catalogue, Shard Map Modal, On-Demand Streaming Download, Active Transfers, Durability Monitor, Node Settings.
- **11 REST API v1 Endpoints:** `/api/v1/health`, `/api/v1/node`, `/api/v1/peers`, `/api/v1/files`, `/api/v1/files/{file_id}`, `/api/v1/search`, `/api/v1/uploads`, `/api/v1/downloads/{file_id}`, `/api/v1/transfers`, `/api/v1/recovery`, `/api/v1/storage`.
- **Security:** Bound to `127.0.0.1` by default, CSRF token validation (`X-RFE-Request: 1`), Origin check, token auth for LAN access (`--ui-lan`), and direct disk streaming.

### Interface B: Native Command-Line Interface (`rfe.exe`)
All 14 administrative and user operations supported:
```powershell
rfe status [--peer <ip:port>]     # Display cluster health and storage metrics
rfe peers [--peer <ip:port>]      # Show live peer membership and health states
rfe storage [--peer <ip:port>]    # Display local storage usage, quotas, and shards
rfe upload <file> [--peer <ip:port>] # Upload and RS(3,2)-distribute file (reports ms & MB/s)
rfe files [--peer <ip:port>]      # List distributed file catalogue
rfe search <query> [--peer <ip:port>] # Search by filename or File ID
rfe info <file-id> [--peer <ip:port>] # Show stripe layout and shard peer locations
rfe download <id> [--peer <ip:port>] [-o <dest>] # Download & RS-reconstruct file
rfe transfers [--peer <ip:port>]  # View live active transfer bandwidth telemetry
rfe recovery [--peer <ip:port>]   # View durability recovery event log
rfe repair [file-id] [--peer <ip:port>] # Trigger immediate durability repair scan
rfe shutdown [--peer <ip:port>]   # Gracefully shut down peer daemon
```

---

## 5. Building from Source

### Prerequisites:
- Windows 10 / 11 (x64)
- Visual Studio 2022 (MSVC v143 toolset with C++17 support)
- CMake 3.16+
- Git

### Build Instructions:
```powershell
# 1. Clone repository
git clone https://github.com/Arihant-57/P2P-File-Engine.git
cd P2P-File-Engine

# 2. Configure CMake with bundled OpenSSL
cmake -S . -B build -DOPENSSL_ROOT_DIR="$PWD/third_party/openssl"

# 3. Build Release binaries
cmake --build build --config Release
```
This compiles `sqlite3.lib`, `engine.lib`, `peer.exe`, `rfe.exe`, and `controller.exe` into `build\Release\`.

---

## 6. Automated Testing

### C++ Automated Test Suite (`T01`–`T16`)
```powershell
.\build\Release\controller.exe test
```
**Result: `PASS=16 FAIL=0`**
- `T01`–`T07`: Round-robin chunk distribution, single-peer failure recovery, SHA-256 validation, unrecoverable states.
- `T08`–`T10`: Dynamic TCP registration, storage reporting, heartbeats, and Gate-2 RS(3,2) repair.
- `T11`–`T15`: 4-peer UDP discovery, distributed SQLite metadata sync, P2P upload/download, autonomous recovery.
- `T16`: Web Dashboard HTTP server, UI assets, REST API v1, CSRF security, streaming upload/download.

### Real-World Multi-Process Audit
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_real_world_e2e_audit.ps1
```
**Result: `ALL 8 REAL-WORLD END-TO-END SCENARIOS PASSED (100% VERIFIED)`**

---

## 7. Four-Laptop LAN Demonstration

```text
Laptop 1 (Peer A - 192.168.1.101:5701) <===> Laptop 2 (Peer B - 192.168.1.102:5702)
                     ^                                     ^
                     |                                     |
Laptop 4 (Peer D - 192.168.1.104:5704) <===> Laptop 3 (Peer C - 192.168.1.103:5703)
```

1. Extract `P2P_File_Engine_Windows_x64.zip` on 4 laptops.
2. Run `configure_lan_firewall.ps1` (as Administrator) once per laptop.
3. Run `setup_peer.ps1` on each laptop, assigning IDs `A`, `B`, `C`, `D` and each laptop's LAN IP.
4. Run `start_peer.ps1` on all 4 laptops.
5. Upload a file on Laptop 1 &rarr; search and download from Laptop 4 &rarr; power off Laptop 2 holding a shard &rarr; observe automatic RS(3,2) reconstruction and self-healing onto Laptop 4!

*See [**`FOUR_LAPTOP_DEMO_GUIDE.md`**](FOUR_LAPTOP_DEMO_GUIDE.md) for the detailed step-by-step evaluator checklist.*

---

## 8. Collaboration Guide for Teammates

### For College Teammates (Collaborating on Code):
1. **Repository Access:** If the repository is currently private, request an invite from `@Arihant-57`.
   *(Owner: Go to **Settings** &rarr; **Collaborators** &rarr; **Add people** &rarr; Enter your teammate's GitHub username).*
2. **Accept Invitation:** Check your email or go to `https://github.com/Arihant-57/P2P-File-Engine/invitations`.
3. **Clone & Branch:**
   ```powershell
   git clone https://github.com/Arihant-57/P2P-File-Engine.git
   cd P2P-File-Engine
   git checkout -b feature/your-feature-name
   ```
4. **Build & Verify:**
   Run `.\build\Release\controller.exe test` before committing to ensure zero regressions.
5. **Submit a Pull Request (PR):** Push your branch to GitHub and create a PR to `main`.

---

## 9. Security & Limitations

- **LAN Scope:** Designed and optimized for Local Area Networks (LAN) and private Wi-Fi subnets. Does not implement NAT traversal.
- **Wi-Fi Client Isolation:** On public campus Wi-Fi networks where router AP isolation is enabled, UDP broadcast discovery may be filtered; peers should supply explicit seed endpoints via `--seed <ip:port>`.
- **Encryption:** Binary shard transfers over TCP occur in plain binary without TLS encryption. Plain HTTP is used for local dashboard access.

---

## 10. License & Third-Party Notices

- **SQLite 3.54.1:** Public Domain ([`third_party/sqlite/`](third_party/sqlite/))
- **OpenSSL 3.6.3:** Apache License 2.0 ([`third_party/openssl/`](third_party/openssl/))
- **mini_json:** MIT License ([`third_party/mini_json.hpp`](third_party/mini_json.hpp))

*See [`THIRD_PARTY_NOTICES.txt`](THIRD_PARTY_NOTICES.txt) for complete license texts.*
