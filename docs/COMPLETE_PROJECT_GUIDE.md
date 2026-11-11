# Peer-to-Peer Decentralized File Transfer and Recovery Engine
## Complete Technical Learning Guide, System Architecture, and Implementation Handbook

**Course:** 22ECAW301 — Capstone Design Project | **Team 13**  
**Document Classification:** Technical Reference Manual & Educational Guide  
**Release Target:** Windows 10 / 11 (x64) | **Language Standard:** ISO C++17  
**Publication Date:** November 2026 | **Version:** 2.0.0 (Production Release)  

---

## Executive Abstract

The **Peer-to-Peer Decentralized File Transfer and Recovery Engine** is an institutional-grade, fault-tolerant distributed storage and file recovery system engineered entirely in modern C++17. Designed specifically for Local Area Networks (LAN), the system eliminates all single points of failure by replacing traditional centralized client-server models with a symmetrical peer mesh. Every participating node operates concurrently as a framed TCP server, a TCP client, an embedded SQLite database host, a UDP discovery broadcaster, a Reed-Solomon erasure encoder/decoder, and an autonomous failure detector and recovery coordinator.

This guide provides an end-to-end educational and architectural walkthrough of the system. It bridges theoretical computer science concepts—spanning Object-Oriented Programming (OOP), Data Structures and Algorithms (DSA), Operating Systems (OS), Computer Networks (CN), Database Management Systems (DBMS), and Cryptographic Data Integrity—with their concrete, high-performance implementations in C++17.

---

## Table of Contents

1. [Project Introduction and Theoretical Foundations](#1-project-introduction-and-theoretical-foundations)
2. [Computer Science Syllabus Mapping](#2-computer-science-syllabus-mapping)
   - [2.1 Object-Oriented Programming (OOP)](#21-object-oriented-programming-oop)
   - [2.2 Data Structures and Algorithms (DSA)](#22-data-structures-and-algorithms-dsa)
   - [2.3 Operating Systems & Systems Programming (OS)](#23-operating-systems--systems-programming-os)
   - [2.4 Computer Networks](#24-computer-networks)
   - [2.5 Database Management Systems (DBMS)](#25-database-management-systems-dbms)
   - [2.6 Cryptography and Data Integrity](#26-cryptography-and-data-integrity)
   - [2.7 Software Engineering & Verification Methodologies](#27-software-engineering--verification-methodologies)
3. [System Architecture and Mesh Topology](#3-system-architecture-and-mesh-topology)
4. [Peer Discovery, Wire Protocol, and Network Lifecycle](#4-peer-discovery-wire-protocol-and-network-lifecycle)
5. [Complete File Upload Workflow](#5-complete-file-upload-workflow)
6. [Reed-Solomon RS(3,2) Erasure Coding Mathematics](#6-reed-solomon-rs32-erasure-coding-mathematics)
7. [Distributed Metadata Management and SQLite Schema](#7-distributed-metadata-management-and-sqlite-schema)
8. [File Search, Download, and Reconstruction Workflow](#8-file-search-download-and-reconstruction-workflow)
9. [Autonomous Failure Detection and Durability Recovery](#9-autonomous-failure-detection-and-durability-recovery)
10. [Embedded Web Dashboard and REST API Architecture](#10-embedded-web-dashboard-and-rest-api-architecture)
11. [Complete Source Code Walkthrough](#11-complete-source-code-walkthrough)
12. [Installation, Deployment, and User Guide](#12-installation-deployment-and-user-guide)
13. [Markdown and Project File-Type Reference](#13-markdown-and-project-file-type-reference)
14. [Comprehensive Testing and Verification Report](#14-comprehensive-testing-and-verification-report)
15. [Four-Laptop Demonstration Script & Faculty Evaluation Guide](#15-four-laptop-demonstration-script--faculty-evaluation-guide)
16. [Security Analysis and Engineering Limitations](#16-security-analysis-and-engineering-limitations)
17. [Glossary of Technical Terms & Educational Outcomes](#17-glossary-of-technical-terms--educational-outcomes)
18. [Official Artifacts and Cryptographic Checksums](#18-official-artifacts-and-cryptographic-checksums)

---

# 1. Project Introduction and Theoretical Foundations

### 1.1 What is a Distributed File System?
A distributed file system manages the storage and retrieval of digital files across a physical network of independent computing nodes. To the human user or client application, a distributed file system presents the illusion of a single, coherent, local storage drive. Under the surface, however, the data is partitioned, transmitted, and redundantly persisted across multiple autonomous machines.

### 1.2 The Problem with Centralized Architectures
Traditional file sharing and storage systems (e.g., FTP servers, basic cloud drives, or network-attached storage appliances) rely on a **central coordinator or central file server**:
- **Single Point of Failure (SPOF):** If the central server crashes, suffers hardware failure, or loses network connectivity, the entire storage cluster becomes completely inaccessible.
- **Network Bottleneck:** Every upload, download, and status query must funnel through the central server's network interface card (NIC), resulting in severe bandwidth throttling as user count grows.
- **Storage Scalability Limits:** Expanding storage requires provisioning larger and more expensive physical storage arrays on the central machine (vertical scaling) rather than pooling consumer commodity laptops (horizontal scaling).
- **High Administrative Burden:** A centralized server requires dedicated system administration, database maintenance, user management, and expensive high-availability infrastructure.

### 1.3 The Peer-to-Peer (P2P) Paradigm
In a **genuinely decentralized peer-to-peer system**, every participating computer is treated as an equal peer (*servent* = server + client):
- **Symmetry:** Every node runs the exact same software stack.
- **Zero Central Coordinator:** No node possesses permanent administrative authority or proprietary control over the network.
- **Pooled Resources:** Storage capacity, processing power, and network bandwidth scale organically as new peers join the network.
- **Initiator vs. Coordinator:** When a user uploads a file through Peer A, Peer A simply acts as the *transient initiator* for that specific transaction. Peer A does not become a permanent coordinator; once the upload completes and shards are distributed, Peer A can be powered off immediately without preventing other peers from searching for and downloading the file.

### 1.4 Fault Tolerance and Erasure Coding
Distributed systems must operate under the assumption that hardware, software, and networks **will fail**. In a consumer LAN environment, laptops may close their lids, run out of battery, disconnect from Wi-Fi, or crash.

To protect against data loss when storage nodes disappear, systems use redundancy:
1. **Simple Replication (Mirroring):** Storing 3 full copies of a file requires 200% storage overhead ($3\times$ the original size).
2. **Erasure Coding (Reed-Solomon):** Instead of copying the entire file, the file is segmented into $K$ data fragments and mathematically transformed into $K + M$ total fragments. Any $K$ of these $K + M$ fragments are mathematically sufficient to reconstruct the entire file.
   - In our project, we implement systematic **$RS(3,2)$**: each stripe is split into $K=2$ data shards ($D_0, D_1$) and $M=1$ parity shard ($P_0$), yielding $N=3$ total shards placed on **3 distinct physical peers**.
   - Storage overhead is only **50%** ($1.5\times$ original size), yet the system can tolerate the instantaneous, catastrophic failure of any single peer holding a shard without data loss.

### 1.5 Problem Statement and Project Objectives
The objective of this project is to design, implement from scratch in C++17, rigorously test, and package a production-grade, decentralized peer-to-peer file transfer and recovery engine that:
1. Operates without any centralized servers, databases, or coordinators.
2. Supports arbitrary binary file formats (text, PDF, audio, video, raw executables) of any size.
3. Implements systematic Reed-Solomon $RS(3,2)$ erasure coding over Galois Field $\text{GF}(2^8)$ to guarantee single-node fault tolerance with minimal storage overhead.
4. Distributes shards across distinct physical machines based on capacity and real-time workload.
5. Employs independent, embedded SQLite 3 databases on every node, synchronizing metadata using peer-to-peer gossip and anti-entropy algorithms.
6. Continuously monitors peer liveness via UDP and TCP heartbeats, detecting node failure and autonomously rebuilding missing shards onto surviving nodes without human intervention.
7. Provides two intuitive user interfaces: a native Windows console CLI (`rfe.exe`) and an embedded local Web Dashboard (`http://127.0.0.1:8080`) served by a native C++ HTTP server.
8. Distributes as a zero-dependency, self-contained Windows x64 portable package.

---

# 2. Computer Science Syllabus Mapping

This section maps the theoretical principles taught across undergraduate computer science curricula directly to the C++17 implementation in our codebase.

```
+-----------------------------------------------------------------------------------+
|               COMPUTER SCIENCE SYLLABUS INTEGRATION IN TALENT HUB P2P             |
+-----------------------------------------------------------------------------------+
|  OOP          | Encapsulation, RAII, Composition, Abstract Interfaces             |
|  DSA          | Vectors, Hash Maps, Priority Queues, DJB2 Hashing, O(1) Lookups   |
|  OS & OSPP    | Multithreading, Mutexes, Condition Variables, Atomic File Commit  |
|  NETWORKS     | TCP/IP Sockets, UDP Broadcast, Custom Framing, Wire Protocol      |
|  DBMS         | SQLite 3, WAL Mode, ACID Transactions, Relational Foreign Keys    |
|  CRYPTOGRAPHY | OpenSSL SHA-256 Digesting, Shard Integrity, Tamper Detection      |
|  SOFTWARE ENG | Modular Ports/Adapters, TDD (T01-T16), Multi-Process E2E Testing |
+-----------------------------------------------------------------------------------+
```

---

## 2.1 Object-Oriented Programming (OOP)

| OOP Concept | Project Application & Code Evidence | File References |
| :--- | :--- | :--- |
| **Encapsulation** | Internal state (sockets, database connections, active thread pools) is strictly declared `private`. Public methods expose clean, thread-safe operational verbs. | [`tcp_server.hpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/communication/tcp_server.hpp)<br>[`database_manager.hpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/db/database_manager.hpp) |
| **Abstraction** | Callers interact with simple abstractions (`distribute_file`, `download_file`, `reconstruct_stripe`) without needing to know low-level socket handles, Vandermonde matrix inversions, or SQLite B-tree layouts. | [`dynamic_distributor.hpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/distribution/dynamic_distributor.hpp)<br>[`client_download.hpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/recovery/client_download.hpp) |
| **RAII (Resource Acquisition Is Initialization)** | Sockets, open file descriptors, thread handles, and database statements automatically close and clean up their OS resources when their owning object leaves scope, even if an exception is thrown. | [`tcp_client.cpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/communication/tcp_client.cpp)<br>[`database_manager.cpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/db/database_manager.cpp) (`StmtGuard`) |
| **Composition & Modularity** | The top-level `peer.cpp` daemon does not use inheritance bloat; instead, it is composed of specialized engine modules: `TCPServer`, `UDPBeacon`, `DatabaseManager`, `DynamicDistributor`, `HttpServer`. | [`peer/peer.cpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/peer/peer.cpp) |

### Concrete Code Example: RAII Statement Guard
In [`database_manager.cpp`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/engine/db/database_manager.cpp), SQLite prepared statements are wrapped in an RAII struct that guarantees `sqlite3_finalize` is invoked immediately upon exiting the scope:

```cpp
// RAII guard ensuring sqlite3_stmt is never leaked on early return or exception
struct StmtGuard {
    sqlite3_stmt* stmt = nullptr;
    explicit StmtGuard(sqlite3_stmt* s) : stmt(s) {}
    ~StmtGuard() {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
    }
    // Prevent copying to maintain unique ownership
    StmtGuard(const StmtGuard&) = delete;
    StmtGuard& operator=(const StmtGuard&) = delete;
};
```

---

## 2.2 Data Structures and Algorithms (DSA)

### 1. Hash Maps (`std::unordered_map`)
- **Use:** Used in `PeerRegistry` for $O(1)$ lookups of online peers by their string `peer_id`, and in `ChunkManager` for caching in-flight transfer states.
- **Complexity:** Average $O(1)$ search, insert, and delete.

### 2. Contiguous Dynamic Arrays (`std::vector<uint8_t>`)
- **Use:** Storing raw binary payloads, Reed-Solomon shard bytes, and network buffers.
- **Advantage:** Cache-friendly contiguous memory layout ensures zero fragmentation and maximizes hardware memory bus throughput during Galois Field XOR operations.

### 3. Hashing Algorithms
- **Cryptographic Hashing (SHA-256):** Produces a 256-bit (32-byte) digest using OpenSSL's `EVP_sha256()` to verify absolute byte integrity per shard and per file.
- **Deterministic Partitioning Hashing (DJB2):** A high-speed, non-cryptographic hash function used to elect a single recovery coordinator among surviving peers:
  $$\text{Selected Peer Index} = \text{DJB2}(\text{file\_id}) \pmod{\text{Total Online Peers}}$$

```cpp
// DJB2 Hash implementation in peer.cpp for deterministic distributed consensus
uint32_t djb2_hash(const std::string& str) {
    uint32_t hash = 5381;
    for (unsigned char c : str) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash;
}
```

### 4. Workload & Capacity Ranking Algorithm
When selecting 3 distinct peers to hold the 3 shards ($D_0, D_1, P_0$) of a stripe, the `DynamicDistributor` executes a multi-factor greedy placement algorithm:
1. Filter out peers in `OFFLINE` or `SUSPECT` health states.
2. Calculate a Composite Workload Score:
   $$\text{Score}(P) = w_1 \cdot \text{active\_transfers} - w_2 \cdot \text{available\_storage\_bytes}$$
3. Sort candidate peers in ascending score order ($O(P \log P)$).
4. Select the top 3 distinct physical peers, rotating offsets per stripe to guarantee even cluster distribution.

---

## 2.3 Operating Systems & Systems Programming (OS)

### 1. Process Concurrency & Multi-Process Architecture
The project executes as completely isolated OS processes. On a single Windows machine, four separate instances of `peer.exe` run simultaneously, each bound to its own TCP port (e.g., `5701`, `5702`, `5703`, `5704`), its own UDP discovery socket, and its own filesystem folder (`storage/peer_A/`). The OS kernel handles process isolation, virtual memory paging, and socket multiplexing.

### 2. Multithreading and Synchronization
Each `peer.exe` process spawns multiple dedicated background worker threads:
- **`accept_thread_`:** Blocks on `accept()` to handle incoming client and peer TCP connections.
- **`beacon_listen_thread_`:** Blocks on `recvfrom()` for UDP peer discovery broadcasts.
- **`beacon_broadcast_thread_`:** Periodic timer thread emitting UDP beacons every 1000ms.
- **`monitor_thread_`:** Heartbeat checking, failure state transitions, and autonomous recovery coordinator.
- **`http_server` thread pool:** Handles concurrent browser HTTP requests.

All shared resources (such as `PeerRegistry` and SQLite access) are synchronized using `std::mutex`, `std::lock_guard`, and `std::unique_lock`.

```cpp
// Thread-safe peer health update using std::lock_guard
void PeerRegistry::update_heartbeat(const std::string& peer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = peers_.find(peer_id);
    if (it != peers_.end()) {
        it->second.last_heartbeat_time = std::chrono::steady_clock::now();
        it->second.health = PeerHealth::ONLINE;
        it->second.missed_heartbeats = 0;
    }
}
```

### 3. Atomic Filesystem Operations
To prevent data corruption if a machine loses power or crashes during a write, the engine strictly enforces **atomic two-phase writes**:
1. Incoming binary shards are written to a temporary staging file (`storage/peer_A/staging/<shard_id>.tmp`).
2. The SHA-256 hash of the written bytes is calculated and verified against the expected hash.
3. Only upon successful verification does the engine issue `std::filesystem::rename()`, which maps to the atomic Windows API `MoveFileExW(..., MOVEFILE_REPLACE_EXISTING)`.
4. If a failure occurs mid-transfer, the incomplete `.tmp` file is immediately deleted, guaranteeing that corrupt or partial shards never enter the storage catalogue.

---

## 2.4 Computer Networks

```
+-----------------------------------------------------------------------------------+
|                        P2P NETWORK PROTOCOL STACK                                 |
+-----------------------------------------------------------------------------------+
|  Application Layer     |  RFE P2P Protocol / HTTP/1.1 REST API                    |
|  Presentation Layer    |  UTF-8 JSON Headers + Raw Binary Payload Stream          |
|  Transport Layer       |  TCP (Reliable Data Transfer) / UDP (Discovery Broadcast)|
|  Internet Layer        |  IPv4 Addressing (LAN Subnet / Loopback 127.0.0.1)       |
|  Link Layer            |  Ethernet / 802.11 Wi-Fi                                 |
+-----------------------------------------------------------------------------------+
```

### 1. TCP vs. UDP Specialization
- **UDP (User Datagram Protocol):** Used exclusively for **peer discovery**. Nodes broadcast compact JSON discovery packets to `255.255.255.255` on port `5705`. Because UDP is connectionless and lightweight, new nodes can discover existing cluster members without prior knowledge of their IP addresses.
- **TCP (Transmission Control Protocol):** Used for all **metadata exchange, heartbeats, and binary shard transfers**. TCP guarantees in-order byte delivery, automatic packet retransmission, congestion control, and connection state management.

### 2. Custom Big-Endian Framing Protocol
TCP is a byte-stream protocol without built-in message boundaries. To prevent packet fragmentation bugs, all peer-to-peer TCP communication uses a strict 3-field framing protocol:

```text
+-----------------------+-----------------------+-----------------------------------+
| Header Length (4 B)   | JSON Header (N Bytes) | Binary Payload (M Bytes)          |
| [uint32_t Big-Endian] | [UTF-8 JSON string]   | [Raw Shard / File Bytes]          |
+-----------------------+-----------------------+-----------------------------------+
```
- **Field 1 (4 bytes):** 32-bit unsigned integer in network byte order (Big-Endian) indicating the exact length $N$ of the JSON header.
- **Field 2 ($N$ bytes):** Structured UTF-8 JSON object containing message metadata (e.g., `{"type": "PUSH_SHARD", "shard_id": "...", "payload_size": 65536}`).
- **Field 3 ($M$ bytes):** Raw binary data stream (zero JSON base64-encoding overhead).

---

## 2.5 Database Management Systems (DBMS)

### 1. Why SQLite on Every Peer Instead of a Central DB?
A centralized database server (such as a shared MySQL or PostgreSQL instance) introduces a fatal single point of failure and requires network connectivity for every local query. Instead, every peer hosts an embedded **SQLite 3.54.1** database engine directly inside its process address space (`storage/peer_<ID>/metadata.db`).

### 2. ACID Properties & WAL Mode
- **Atomicity:** All multi-row updates (e.g., registering a 20-stripe file manifest with 60 shard locations) execute inside `BEGIN TRANSACTION` and `COMMIT` blocks. If any insert fails, the transaction rolls back cleanly.
- **Consistency:** SQLite enforces relational foreign keys and `NOT NULL` constraints on file IDs, stripe indices, and shard locations.
- **Isolation:** SQLite is configured in **Write-Ahead Logging (WAL)** mode (`PRAGMA journal_mode=WAL;`). In WAL mode, read operations do not block write operations, and write operations do not block read operations.
- **Durability:** Committed transactions are flushed to disk logs, ensuring crash resilience.

---

## 2.6 Cryptography and Data Integrity

### 1. Cryptographic SHA-256 Digesting
The engine integrates OpenSSL 3 (`EVP_DigestInit_ex`, `EVP_DigestUpdate`, `EVP_DigestFinal_ex`) to calculate 256-bit SHA-256 cryptographic digests:
- **Per-Shard Verification:** Every individual shard carries its own SHA-256 hash. When a node receives a shard over TCP or reads it from disk, it recalculates the SHA-256 digest. If even a single bit has flipped due to disk rot or transmission noise, the shard is rejected immediately.
- **Whole-File Verification:** The file manifest records the SHA-256 digest of the original unchunked file. After all stripes are downloaded and reconstructed, the final assembled file is hashed. The file is only committed to the user's output directory if its hash matches the manifest identically.

### 2. Crucial Security Clarification
> [!IMPORTANT]
> **Reed-Solomon is an Erasure-Correcting Code, NOT Encryption:** Reed-Solomon transforms and adds mathematical redundancy to data so that missing fragments can be reconstructed. It does not obfuscate, scramble, or protect data confidentiality against eavesdroppers.
> 
> **SHA-256 Provides Integrity, NOT Authentication:** SHA-256 proves that a block of data has not been modified in transit. It does not authenticate the identity of the sending peer or prevent an adversary from forging packets unless paired with digital signatures (HMAC or public-key cryptography).

---

## 2.7 Software Engineering & Verification Methodologies

The project follows rigorous software engineering discipline:
- **Test-Driven Verification:** New non-trivial logic requires automated regression tests. The test suite expanded from Gate-1 `T01–T07`, to Gate-2 `T08–T10`, Phase-2 `T11–T15`, and Web Dashboard `T16`.
- **Fault Injection:** Automated tests programmatically kill peer processes mid-transfer, flip bytes in stored `.chunk` files on disk, and simulate network partitions to verify that failure recovery logic executes correctly under adverse conditions.
- **Zero-Warning Builds:** Configured with MSVC `/W4` and security linker hardening flags (`/DYNAMICBASE /NXCOMPAT /HIGHENTROPYVA`).

---

# 3. System Architecture and Mesh Topology

The architecture consists of completely symmetrical peer nodes communicating over a Local Area Network.

```text
               +-------------------------------------------------------+
               |               USER INTERFACES                         |
               |   [Windows Console CLI]       [Browser Web Dashboard] |
               |        (rfe.exe)               (http://127.0.0.1:8080)|
               +-------------------+-------------------+---------------+
                                   |                   |
                     TCP Commands  |                   | HTTP / REST API
                                   v                   v
+---------------------------------------------------------------------------------------+
|                                    PEER NODE (peer.exe)                               |
|                                                                                       |
|  +--------------------+   +---------------------+   +-------------------------------+ |
|  | Embedded HTTP/1.1  |   | Framed TCP Server   |   | UDP Discovery Beacon          | |
|  | Web Server & API   |   | (Port 5701..5704)   |   | (Port 5705 Broadcast)         | |
|  +---------+----------+   +----------+----------+   +---------------+---------------+ |
|            |                         |                              |                 |
|            +-------------------------+                              |                 |
|                                      v                              v                 |
|                        +---------------------------+   +----------------------------+ |
|                        | Decentralized Peer Engine |<--| Thread-Safe Peer Registry  | |
|                        +-------------+-------------+   +----------------------------+ |
|                                      |                                                |
|       +------------------------------+------------------------------+                 |
|       v                              v                              v                 |
|  +-----------------------+  +------------------------+  +---------------------------+ |
|  | Adaptive Chunker &    |  | Dynamic Distributor &  |  | Autonomous Recovery       | |
|  | Reed-Solomon RS(3,2)  |  | P2P Client Download    |  | Coordinator & Heartbeat   | |
|  +-----------+-----------+  +-----------+------------+  +-------------+-------------+ |
|              |                          |                             |               |
|              +--------------------------+-----------------------------+               |
|                                         v                                             |
|                     +---------------------------------------+                         |
|                     | Storage & Persistence Layer           |                         |
|                     |  - Embedded SQLite (metadata.db)      |                         |
|                     |  - Shard Store (storage/peer_A/chunks)|                         |
|                     +---------------------------------------+                         |
+---------------------------------------------------------------------------------------+
            ^                             ^                             ^
            | Direct TCP Shard Retrieval  | Heartbeat Gossip            | Manifest Push
            v                             v                             v
+-----------------------+     +-----------------------+     +-----------------------+
|  PEER B (Port 5702)   |     |  PEER C (Port 5703)   |     |  PEER D (Port 5704)   |
|  - SQLite DB          |     |  - SQLite DB          |     |  - SQLite DB          |
|  - Shard Store        |     |  - Shard Store        |     |  - Shard Store        |
+-----------------------+     +-----------------------+     +-----------------------+
```

---

# 4. Peer Discovery, Wire Protocol, and Network Lifecycle

### 4.1 Step-by-Step Peer Join Sequence
When a new node launches, it integrates into the decentralized mesh automatically:

```text
  New Peer (A)                                                  Surviving Mesh (B, C, D)
       |                                                                   |
       |--- 1. UDP Broadcast ("HELLO_BEACON", port 5705) ----------------->|
       |<-- 2. UDP Beacon Response (IP, TCP Port, Peer ID) ----------------|
       |                                                                   |
       |--- 3. TCP Connect to Discovered Peer B (e.g. 127.0.0.1:5702) ---->|
       |--- 4. P2P_HANDSHAKE {peer_id: "A", port: 5701, storage: 100GB} ->|
       |<-- 5. P2P_HANDSHAKE_ACK {status: "OK", peer_id: "B"} -------------|
       |                                                                   |
       |<-- 6. P2P_SYNC_PEERS {peers: [B, C, D]} --------------------------|
       |--- 7. P2P_METADATA_PULL_REQ {since_version: 0} ------------------>|
       |<-- 8. P2P_METADATA_PULL_RESP {manifests: [...]} ------------------|
       |                                                                   |
  [Active Mesh Participant: Periodic TCP Heartbeats every 1000ms]
```

1. **UDP Discovery:** `UDPBeacon` transmits a broadcast datagram to `255.255.255.255:5705` and listens for beacons from existing nodes.
2. **TCP Handshake:** Upon receiving a beacon, Peer A connects via TCP to the advertised IP and port, transmitting `P2P_HANDSHAKE`. Both nodes exchange node IDs, software versions, and available disk capacity.
3. **Membership Sync:** Peer B replies with `P2P_SYNC_PEERS`, informing Peer A of all other known peers in the network. Peer A connects to Peers C and D in parallel, forming a fully connected mesh.
4. **Metadata Catch-Up:** Peer A sends `P2P_METADATA_PULL_REQ` to request all file manifests committed while Peer A was offline, updating its local SQLite database.

---

# 5. Complete File Upload Workflow

When a user initiates an upload through either the CLI or Web Dashboard, the system executes a 15-step sequence:

```text
+---------------------------------------------------------------------------------------+
|                             15-STEP UPLOAD WORKFLOW                                   |
+---------------------------------------------------------------------------------------+
|  1. Input File Validation  --> Verify file exists, readable, and non-empty.          |
|  2. Size & Hash Scan       --> Stream file to compute full-file SHA-256 digest.       |
|  3. Unique ID Generation   --> Assign cryptographically unique File ID (e.g. F_B399B).|
|  4. Adaptive Chunking      --> Calculate optimal stripe size (64KB to 1MB).           |
|  5. File Slicing           --> Slice file into sequential stripes.                    |
|  6. Stripe Partitioning    --> Split each stripe into K=2 Data Shards (D0, D1).       |
|  7. Zero Padding           --> Pad final stripe shard to ensure equal shard lengths.  |
|  8. Reed-Solomon Encoding  --> Compute Parity Shard P0 = D0 (+) D1 over GF(2^8).      |
|  9. Shard Hashing          --> Compute individual SHA-256 digests for D0, D1, P0.     |
| 10. Peer Selection         --> Rank peers by capacity/workload; pick 3 distinct peers.|
| 11. TCP Shard Transmission --> Send PUSH_SHARD packets over TCP to chosen nodes.      |
| 12. Remote Hash Validation --> Remote nodes verify received shard SHA-256.           |
| 13. Atomic Shard Storage   --> Remote nodes rename .tmp -> .chunk in storage directory|
| 14. SQLite Manifest Insert --> Commit file, stripe, and shard rows in local SQLite DB.|
| 15. Broadcast Sync Push    --> Send P2P_METADATA_PUSH to all mesh peers.              |
+---------------------------------------------------------------------------------------+
```

---

# 6. Reed-Solomon RS(3,2) Erasure Coding Mathematics

### 6.1 Galois Field $\text{GF}(2^8)$ Arithmetic
Standard computer arithmetic cannot be used for erasure coding because operations like addition and multiplication can overflow fixed byte boundaries ($0..255$). Reed-Solomon operates over a finite **Galois Field $\text{GF}(2^8)$**:
- **Field Size:** Exactly 256 elements ($0$ to $255$), fitting perfectly in a standard 8-bit unsigned `uint8_t`.
- **Addition & Subtraction:** Implemented as bitwise XOR (`^`). Addition and subtraction are identical:
  $$A \oplus B = A \text{ \textasciicircum } B$$
- **Multiplication:** Implemented using precomputed log and anti-log (exponential) lookup tables based on the irreducible primitive polynomial:
  $$P(x) = x^8 + x^4 + x^3 + x^2 + 1 \quad (\text{Hex: } \mathtt{0x11D})$$

```cpp
// GF(256) multiplication using precomputed tables in gf256.cpp
uint8_t gf256_mul(uint8_t a, uint8_t b) {
    if (a == 0 || b == 0) return 0;
    return g_exp_table[g_log_table[a] + g_log_table[b]];
}
```

### 6.2 Systematic Vandermonde Matrix Encoding
In systematic Reed-Solomon, the original data shards appear unchanged in the encoded output. The generator matrix $G$ for $RS(3,2)$ consists of a $2 \times 2$ identity matrix for data shards and a row of ones for parity:

$$\begin{bmatrix} D_0 \\ D_1 \\ P_0 \end{bmatrix} = \begin{bmatrix} 1 & 0 \\ 0 & 1 \\ 1 & 1 \end{bmatrix} \begin{bmatrix} D_0 \\ D_1 \end{bmatrix}$$

This yields the concrete equations:
$$\text{Data Shard 0: } D_0 = D_0$$
$$\text{Data Shard 1: } D_1 = D_1$$
$$\text{Parity Shard 0: } P_0 = D_0 \oplus D_1$$

### 6.3 Reconstruction Proofs (All Failure Combinations)

| Surviving Pair | Missing Shard | Mathematical Reconstruction Formula | Verified in Code |
| :---: | :---: | :--- | :---: |
| **$D_0 + D_1$** | $P_0$ | Both data shards present. $P_0$ is reconstructed via $P_0 = D_0 \oplus D_1$. | `T09_RS`, `T14` |
| **$D_0 + P_0$** | $D_1$ | Since $P_0 = D_0 \oplus D_1$, XOR both sides with $D_0$:<br>$D_1 = P_0 \oplus D_0$. | `T09_RS`, `T14` |
| **$D_1 + P_0$** | $D_0$ | Since $P_0 = D_0 \oplus D_1$, XOR both sides with $D_1$:<br>$D_0 = P_0 \oplus D_1$. | `T09_RS`, `T14` |
| **Only 1 shard** | 2 shards | Underdetermined system of linear equations. Mathematically impossible to reconstruct. System reports `UNRECOVERABLE`. | `T05`, `T07`, `T14` |

---

# 7. Distributed Metadata Management and SQLite Schema

Every peer maintains an identical, normalized relational SQLite schema in `storage/peer_<ID>/metadata.db`.

```
+-----------------------------------------------------------------------------------+
|                        SQLITE METADATA RELATIONAL SCHEMA                          |
+-----------------------------------------------------------------------------------+
|  files (file_id PK, filename, original_size, sha256, version, status)             |
|    |                                                                              |
|    +--< stripes (file_id FK, stripe_index, stripe_size)                           |
|           |                                                                       |
|           +--< shards (shard_id PK, file_id FK, stripe_index, shard_type, sha256) |
|                  |                                                                |
|                  +--< shard_locations (shard_id FK, peer_id, location_version)    |
+-----------------------------------------------------------------------------------+
```

### Table Definitions:
1. **`files`:** Master record of all files in the distributed catalogue (`file_id`, `filename`, `original_size`, `sha256`, `total_stripes`, `version`, `status`).
2. **`stripes`:** Stripe ordering and chunk size metadata (`file_id`, `stripe_index`, `stripe_size`, `is_padded`, `original_bytes`).
3. **`shards`:** Individual shard identity and cryptographic digest (`shard_id`, `file_id`, `stripe_index`, `shard_type` [D0/D1/P0], `shard_size`, `sha256`).
4. **`shard_locations`:** Dynamic mapping of which physical peer currently stores which shard (`shard_id`, `peer_id`, `is_primary`, `updated_at`, `version`).
5. **`peers`:** Cluster membership and liveness registry (`peer_id`, `address`, `port`, `storage_capacity_bytes`, `health_status`, `last_heartbeat`).
6. **`recovery_records`:** Audit trail of all autonomous repairs (`recovery_id`, `shard_id`, `failed_peer_id`, `target_peer_id`, `rebuilder_peer_id`, `status`).

---

# 8. File Search, Download, and Reconstruction Workflow

To retrieve a file from the distributed network:
1. **Search Request:** The user queries any peer via CLI (`rfe search <query>`) or Web Dashboard (`GET /api/v1/search?q=...`).
2. **Catalogue Lookup:** The queried peer checks its local SQLite database. If metadata is missing or older than known cluster versions, it fetches the latest manifest from neighboring peers.
3. **Manifest Inspection:** The peer reads the list of stripes and the physical locations of all shards.
4. **Direct Parallel Shard Fetching:** For each stripe, the peer establishes direct TCP connections to the peers storing $D_0$ and $D_1$.
5. **Fallback to Parity:** If the peer holding $D_0$ is offline, unresponsive, or returns a shard whose SHA-256 fails verification, the engine connects to the peer holding parity shard $P_0$ and reconstructs $D_0$ on the fly:
   $$D_0 = D_1 \oplus P_0$$
6. **Padding Removal:** The engine strips trailing zero-padding bytes from the final stripe using the `original_bytes` field stored in the manifest.
7. **Full-File Hash Verification:** The assembled byte stream is hashed with SHA-256. If the digest matches the manifest, the file is atomically committed to the user's destination folder.

---

# 9. Autonomous Failure Detection and Durability Recovery

The system features fully autonomous, decentralized self-healing:

```text
[Peer B Active] ---> Misses 1 Heartbeat (800ms) ---> State: SUSPECT
                ---> Misses 2 Heartbeats (1500ms) -> State: OFFLINE
                                                            |
+-----------------------------------------------------------+
v
[Distributed Recovery Coordination]
1. Surviving peers (A, C, D) detect B is OFFLINE.
2. Query local SQLite for all shards assigned to Peer B.
3. Calculate deterministic recovery coordinator:
   Coordinator = DJB2(file_id) % online_peers.size()
4. Designated coordinator (e.g. Peer A) acquires distributed lease.
5. Coordinator retrieves surviving shards (e.g., D1 from C, P0 from D).
6. Coordinator reconstructs missing shard D0 via RS(3,2) in RAM.
7. Coordinator verifies reconstructed shard SHA-256 digest.
8. Coordinator selects replacement peer (Peer D) not already holding that stripe.
9. Transmits shard over TCP to Peer D; Peer D commits to disk.
10. Coordinator increments manifest version and broadcasts P2P_METADATA_PUSH.
11. Durability restored to 100% (AVAILABLE, 3/3 online shards per stripe).
```

### Stale Shard Cleanup on Peer Rejoin
When the failed Peer B powers back on and reconnects, its local database still lists the old shard locations. During anti-entropy sync:
1. Peer B receives the updated manifest (`version = 3`) from Peer A.
2. Peer B discovers that its previously held shards were already rebuilt and reassigned to Peer D.
3. Peer B automatically deletes its superseded `.chunk` files from its storage folder, preventing disk bloat and split-brain conflicts.

---

# 10. Embedded Web Dashboard and REST API Architecture

Every `peer.exe` binary embeds a custom multi-threaded Winsock HTTP/1.1 server. It serves a single-page application ([`web/`](file:///C:/Users/ariha/Documents/Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/web/)) and exposes a versioned REST API:

| Endpoint | HTTP Method | Description |
| :--- | :---: | :--- |
| `/api/v1/health` | `GET` | Node health, uptime, and software version. |
| `/api/v1/node` | `GET` | Local node ID, IP, port, and storage metrics. |
| `/api/v1/peers` | `GET` | Complete mesh membership table with live health badges. |
| `/api/v1/files` | `GET` | Distributed file catalogue with availability statuses. |
| `/api/v1/files/{file_id}` | `GET` | Detailed file manifest and interactive stripe/shard map. |
| `/api/v1/search?q={query}` | `GET` | Search catalogue by filename or File ID. |
| `/api/v1/uploads` | `POST` | Drag-and-drop file upload streaming directly to disk staging. |
| `/api/v1/downloads/{file_id}`| `GET` | On-demand file reconstruction and browser file download. |
| `/api/v1/transfers` | `GET` | Real-time active upload and download telemetry. |
| `/api/v1/recovery` | `GET` | Durability recovery event log and repair counts. |
| `/api/v1/storage` | `GET` | Storage usage, capacity limits, and stored shard list. |

### Web Security Protections:
- **Default Loopback Binding:** Defaults to `127.0.0.1:8080`, preventing unauthorized external network access.
- **CSRF Token Guard:** All mutating requests (`POST`) require the custom header `X-RFE-Request: 1`.
- **Origin Validation:** Cross-origin requests from foreign websites are strictly rejected.
- **Opt-In LAN Access & Token:** LAN access (`--ui-lan`) requires setting a bearer authentication token (`--ui-token <token>`).

---

# 11. Complete Source Code Walkthrough

```text
Peer-to-Peer-Decentralized-File-Transfer-and-Recovery-Engine/
├── CMakeLists.txt                     # Unified CMake build configuration
├── config.json                        # Default local cluster configuration
├── configs/                           # Preconfigured profiles for 4-peer LAN deployment
├── models/
│   ├── chunk.hpp                      # Shard and chunk data representations
│   ├── file_manifest.hpp              # Canonical FileManifest, Stripe, and Shard models
│   └── peer.hpp                       # PeerInfo, PeerHealth, and PeerWorkload models
├── engine/
│   ├── chunking/                      # ChunkManager and OpenSSL SHA-256 routines
│   ├── coding/                        # Galois Field GF(256) and Reed-Solomon RS(3,2)
│   ├── communication/                 # Framed TCPServer, TCPClient, UDPBeacon, HttpServer
│   ├── db/                            # DatabaseManager (Embedded SQLite 3 with WAL)
│   ├── distribution/                  # DynamicDistributor (Workload-aware 3-peer placement)
│   ├── recovery/                      # ClientDownload (Direct P2P retrieval & RS repair)
│   ├── registry/                      # Thread-safe PeerRegistry and health state machine
│   └── process/                       # Cross-platform process management
├── peer/
│   ├── peer.cpp                       # Core decentralized peer node daemon
│   └── peer.rc                        # Windows PE VersionInfo resource
├── cli/
│   ├── rfe.cpp                        # Native terminal CLI client (14 commands)
│   └── rfe.rc                         # Windows PE VersionInfo resource
├── controller/
│   ├── controller.cpp                 # Automated verification test harness (T01..T16)
│   └── controller.rc                  # Windows PE VersionInfo resource
├── web/                               # Embedded Web Dashboard static assets (HTML/CSS/JS)
├── scripts/                           # PowerShell automation, setup wizards, and firewalls
└── third_party/                       # Bundled SQLite 3.54.1, OpenSSL 3 headers/DLLs
```

---

# 12. Installation, Deployment, and User Guide

### 12.1 Windows Portable Package (`P2P_File_Engine_Windows_x64.zip`)
For end users, classmates, and evaluators who do not have Visual Studio or CMake installed:
1. Extract `P2P_File_Engine_Windows_x64.zip` to any folder (e.g. `C:\P2P_Engine\`).
2. Run the interactive setup wizard:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\setup_peer.ps1
   ```
3. Start the peer:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\start_peer.ps1
   ```
4. Open the Web Dashboard in your browser: `http://127.0.0.1:8080`.
5. Use the CLI client:
   ```powershell
   .\rfe.exe upload "D:\Files\demo.mp4"
   .\rfe.exe files
   .\rfe.exe search "demo"
   .\rfe.exe download "demo.mp4" --output "D:\Downloads\recovered.mp4"
   ```

### 12.2 Building from Source (`P2P_File_Engine_Source.zip`)
For developers and software engineers:
```powershell
# Extract source ZIP and enter repository root
cmake -S . -B build -DOPENSSL_ROOT_DIR="$PWD/third_party/openssl"
cmake --build build --config Release
.\build\Release\controller.exe test
```

---

# 13. Markdown and Project File-Type Reference

| File Extension | File Purpose | How to Open & Inspect |
| :---: | :--- | :--- |
| **`.md`** | **Markdown Documentation:** Human-readable text files with formatting. | Open in **VS Code** and press `Ctrl+Shift+V` for formatted preview. Do NOT execute in PowerShell. |
| **`.cpp` / `.hpp`** | **C++ Source & Headers:** Core compiled system logic. | Open in Visual Studio 2022 or VS Code for inspection. |
| **`.json`** | **Configuration Files:** Plain-text key-value structured data. | Open in Notepad, VS Code, or any text editor. |
| **`.ps1`** | **PowerShell Scripts:** Automation and setup scripts. | Execute via `powershell -ExecutionPolicy Bypass -File <script.ps1>`. |
| **`.exe`** | **Compiled Windows Binaries:** Native x64 machine code executables. | Execute from Command Prompt or PowerShell (`peer.exe`, `rfe.exe`). |
| **`.pdf`** | **Portable Document Format:** Publication document. | Open in Chrome, Edge, Adobe Acrobat, or any PDF viewer. |
| **`.zip`** | **Compressed Archive:** Bundled release packages. | Extract via Windows Explorer ("Extract All...") or `Expand-Archive`. |

---

# 14. Comprehensive Testing and Verification Report

### 14.1 Automated Test Suite (`controller.exe test`) — Result: PASS=16 FAIL=0

| Test ID | Test Name | Purpose & Verification Scope | Result |
| :---: | :--- | :--- | :---: |
| **T01** | Round-Robin Placement | Verifies chunk distribution across 3 nodes with balanced replication. | **PASS** |
| **T02** | Single Peer Failure Recovery | Simulates peer crash; verifies byte-identical file recovery from replica. | **PASS** |
| **T03** | Interrupted Transfer Retry | Injects network failure mid-transfer; verifies cleanup and successful retry. | **PASS** |
| **T04** | SHA-256 Mismatch Rejection | Injects byte corruption; verifies rejection and recovery from secondary source.| **PASS** |
| **T05** | Unrecoverable File Refusal | Kills all surviving sources; verifies download refuses with `UNRECOVERABLE`. | **PASS** |
| **T06** | Reconstruction Order | Verifies reassembly in strict sequential stripe order matching original. | **PASS** |
| **T07** | Two-Peer Failure Handling | Simulates 2-node failure; verifies surviving chunks rescued, rest unrecoverable.| **PASS** |
| **T08** | Dynamic Registration & Heartbeat| Verifies TCP registration, storage reporting, and `ONLINE` $\rightarrow$ `OFFLINE` timing. | **PASS** |
| **T09** | Reed-Solomon RS(3,2) Math | Verifies systematic encoding and reconstruction across D0+D1, D0+P0, D1+P0. | **PASS** |
| **T10** | Gate-2 End-to-End | Verifies on-demand RS(3,2) distribution, peer retrieval, and durability repair. | **PASS** |
| **T11** | Decentralized Peer Foundation | Verifies 4-peer UDP discovery, TCP handshake, SQLite DBs, and rejoin. | **PASS** |
| **T12** | Distributed Metadata Sync | Verifies manifest push, anti-entropy sync, idempotency, and any-peer search. | **PASS** |
| **T13** | Decentralized File Upload | Verifies 9 file types, RS(3,2) 3-distinct-peer placement, and SHA-256 integrity. | **PASS** |
| **T14** | Decentralized Download & RS | Verifies any-peer search, download, corrupted shard rejection, and reconstruction.| **PASS** |
| **T15** | Autonomous Durability Recovery| Verifies automatic failure detection, lease coordination, and 4th-peer rebuild. | **PASS** |
| **T16** | Web Dashboard E2E | Verifies HTTP server, UI assets, REST API v1, CSRF protection, and upload/download.| **PASS** |

### 14.2 Real-World 8-Scenario Multi-Process Audit (`run_real_world_e2e_audit.ps1`)
All 8 real-world user scenarios executed with 4 independent OS processes and passed 100%:
- **Scenario 1:** 4-Peer Decentralized Startup & Discovery — **PASS**
- **Scenario 2:** Real-User CLI Upload across 9 File Types (text, PDF, PNG, WAV, MP4, BIN, 2.5MB, odd padded, 0-byte) — **PASS**
- **Scenario 3:** Catalogue Search via Peer D by Name & File ID — **PASS**
- **Scenario 4:** Download All 9 Files via Peer D + Offline Uploader Test — **PASS**
- **Scenario 5:** Peer B Failure Detection & Autonomous RS(3,2) Recovery — **PASS**
- **Scenario 6:** Peer B Restart, Rejoin & Stale Shard Cleanup — **PASS**
- **Scenario 7:** Corrupted Shard Rejection & Multi-Peer Failure Handling — **PASS**
- **Scenario 8:** CLI Usability, Quoted Paths with Spaces & Diagnostics — **PASS**

---

# 15. Four-Laptop Demonstration Script & Faculty Evaluation Guide

```
+-----------------------------------------------------------------------------------+
|                           4-LAPTOP DEMONSTRATION SETUP                            |
+-----------------------------------------------------------------------------------+
|  Laptop 1 (Peer A) : 192.168.1.101:5701 | Storage: storage/peer_A | UI: Port 8080 |
|  Laptop 2 (Peer B) : 192.168.1.102:5702 | Storage: storage/peer_B | UI: Port 8081 |
|  Laptop 3 (Peer C) : 192.168.1.103:5703 | Storage: storage/peer_C | UI: Port 8082 |
|  Laptop 4 (Peer D) : 192.168.1.104:5704 | Storage: storage/peer_D | UI: Port 8083 |
+-----------------------------------------------------------------------------------+
```

### Demonstration Steps for Project Evaluators:
1. **Cluster Launch:** Start `start_peer.ps1` on all 4 laptops. Open Web Dashboards on each laptop.
2. **Mesh Discovery:** Show evaluators the "Peer Mesh" tab on Laptop 1; observe all 4 laptops listed as `ONLINE`.
3. **Upload via Laptop 1:** Drag and drop `sample_video.mp4` into Laptop 1's Web Dashboard. Point out the live speedometer and upload completion.
4. **Inspect Shard Distribution:** Click on the file in Laptop 1's catalogue. Show evaluators the Shard Map:
   - Data Shard 0 ($D_0$) placed on Laptop 1.
   - Data Shard 1 ($D_1$) placed on Laptop 2.
   - Parity Shard 0 ($P_0$) placed on Laptop 3.
5. **Search from Laptop 4:** On Laptop 4 (which held zero shards for this file), search `sample_video.mp4`. Show that the file is immediately visible with status `AVAILABLE` due to metadata synchronization.
6. **Kill Storage Laptop 2 (Failure Injection):** Abruptly close Laptop 2's lid or terminate its process (`stop_peer.ps1`).
7. **Observe Failure Detection:** On Laptop 4's dashboard, show Laptop 2 transitioning from `ONLINE` to `SUSPECT` (after 800ms) to `OFFLINE` (after 1500ms).
8. **Demonstrate Download Under Failure:** Immediately click "DOWNLOAD" on Laptop 4 while Laptop 2 is dead. Point out that the file downloads instantly. Show that Reed-Solomon automatically used $D_0$ (Laptop 1) and $P_0$ (Laptop 3) to reconstruct missing $D_1$ on the fly!
9. **Autonomous Self-Healing:** Point evaluators to the "Durability Monitor" tab. Show that Laptop 1 or 3 automatically coordinated a recovery lease, rebuilt the missing shard, and placed it onto Laptop 4!
10. **Rejoin & Stale Cleanup:** Power Laptop 2 back on. Show that it rejoins the cluster cleanly, downloads the updated manifest, and deletes its obsolete shard file.

---

# 16. Security Analysis and Engineering Limitations

To maintain academic and professional integrity, the system's operational boundaries are stated honestly:
- **LAN Scope:** Designed and optimized for Local Area Networks (LAN) and private Wi-Fi subnets. Does not implement NAT traversal (STUN/TURN/ICE) or WAN relaying.
- **Wi-Fi Client Isolation:** On public campus Wi-Fi networks where router AP isolation is enabled, peer-to-peer UDP broadcast packets may be blocked by the router. Users must specify seed peers via `--seed <ip:port>`.
- **Peer Trust Model:** Peers operate within a cooperative trust boundary. Cryptographic signatures (HMAC/public-key) are not implemented for inter-peer wire frames; a malicious compromised peer on the same LAN could spoof metadata.
- **Unencrypted LAN Frames:** Peer-to-peer binary shard transfers over TCP occur in plain binary without TLS encryption. Plain HTTP is used for local dashboard access.

---

# 17. Glossary of Technical Terms & Educational Outcomes

### Key Terms:
- **Peer / Servent:** An autonomous network node that acts simultaneously as both a client and a server.
- **Decentralization:** An architectural model where system control, coordination, and storage are distributed equally across all nodes without a central authority.
- **Stripe:** A horizontal slice of a file partitioned into $K$ data fragments and $M$ parity fragments.
- **Shard:** An individual encoded fragment of a stripe ($D_0, D_1, P_0$) stored on an independent peer.
- **Reed-Solomon RS(3,2):** A systematic linear error-correcting code over Galois Field $\text{GF}(2^8)$ where any 2 of 3 shards reconstruct original data.
- **Parity Shard:** A mathematically derived redundant shard ($P_0 = D_0 \oplus D_1$) used to recover lost data shards.
- **Write-Ahead Logging (WAL):** A database logging technique in SQLite providing high-concurrency read-write performance.
- **Anti-Entropy Sync:** A peer-to-peer gossip protocol where nodes periodically compare manifest version vectors to reconcile out-of-date records.

### What I Learned by Building This Project:
1. How to architect asynchronous, multi-threaded network servers from scratch using low-level socket APIs (Winsock2).
2. The mathematical beauty and practical implementation of finite field arithmetic and Reed-Solomon erasure coding.
3. How to design robust, self-healing distributed algorithms that handle node failures and network partitions deterministically.
4. How to integrate embedded relational databases (SQLite 3) with ACID guarantees inside native systems software.
5. The critical importance of Test-Driven Development (TDD) and multi-process fault injection in distributed systems engineering.

---

# 18. Official Artifacts and Cryptographic Checksums

All official release artifacts are stored in `dist/` and verified against `dist/SHA256SUMS.txt`:

```text
19a40c894af553621e0d3da58a44e69efed9b5ba13333437faa799f08c3cb347  P2P_File_Engine_Windows_x64.zip
c28e006fc85f55691d521d069b8cdb5603c8befc4f7a0f4924627c60aedb8ba8  P2P_File_Engine_Source.zip
63b8254687e5902c1bd17a4ee788e44c58c22165fa5efc5529624d0c9c369625  P2P_File_Engine_Complete_Project_Guide.pdf
```

---
*End of Complete Project Guide and Technical Learning Manual.*
