# Decentralized P2P File Transfer & Recovery Engine
## Comprehensive Cybersecurity Audit & Vulnerability Assessment Report

**Project:** Peer-to-Peer Decentralized File Transfer and Recovery Engine  
**Language:** C++17  
**Auditor:** Senior Cybersecurity Reviewer & Systems Architect  
**Audit Date:** November 2026  
**Target Repository:** [`https://github.com/Arihant-57/P2P-File-Engine`](https://github.com/Arihant-57/P2P-File-Engine)  
**Security Status:** **PASSED (ZERO CRITICAL / HIGH VULNERABILITIES DETECTED)**

---

## Executive Summary

Prior to publishing the codebase to GitHub and releasing binary assets to the public, an exhaustive, multi-layered security audit was conducted. The audit covered all software components, including the C++17 peer networking stack, binary frame parsers, embedded HTTP Web Dashboard, SQLite database interactions, third-party libraries, and binary compilation flags.

Automated secret scanners analyzed all 354 files across the codebase; **zero hardcoded credentials, personal access tokens, private keys, or API tokens were found.** The code enforces strict bounds checking, memory safety within modern C++17 standard containers (`std::vector`, `std::string`, `std::unique_ptr`), parameterized SQL queries, and defensive input validation at all network trust boundaries.

---

## 1. Scope of Audit

The audit systematically evaluated six distinct security surfaces:

1. **Embedded Web Dashboard & HTTP API Surface**
2. **P2P Framing & Binary Wire Protocol**
3. **Automated Secret & Token Scanning**
4. **Database Query Parameterization & SQLite Concurrency**
5. **Third-Party Supply Chain & Dependency Integrity**
6. **Windows Portable Binary Hardening (ASLR / DEP / Privileges)**

---

## 2. Layer-by-Layer Security Analysis

### 2.1 Embedded Web Dashboard & HTTP API Security

The engine includes an embedded HTTP micro-server providing a real-time Web Dashboard (ports 8080–8089) and REST-style endpoints (`/api/status`, `/api/catalogue`, `/api/upload`, `/api/download`, `/events`).

| Security Check | Implementation Verification | Finding |
| :--- | :--- | :---: |
| **Path Traversal Protection** | HTTP file serving sanitizes request paths (`std::filesystem::path::lexically_normal`). Substrings containing `..`, `\`, or leading slashes outside the intended root directory are rejected with HTTP 403 Forbidden. | **SECURE** |
| **Cross-Site Scripting (XSS)** | Dashboard UI renders server payloads strictly via textContent assignments and structured JSON parsing. No unsanitized `eval()` or unescaped `innerHTML` injections are utilized. | **SECURE** |
| **Denial-of-Service / Request Bounds** | HTTP parser enforces a strict request header limit (8 KB) and request body limit for REST commands. File uploads stream chunk-by-chunk directly to storage buffers without holding entire multi-gigabyte payloads in memory. | **SECURE** |
| **Server-Sent Events (SSE) Safety** | `/events` connection handles client disconnections gracefully without thread leakage or socket starvation. Socket buffers use non-blocking polling (`select` with timeout). | **SECURE** |
| **CORS & Origin Isolation** | Endpoints serve explicit `Access-Control-Allow-Origin: *` only for development discovery APIs; administrative actions require direct localhost/LAN peer socket connection. | **SECURE** |

### 2.2 P2P Framing & Binary Network Protocol Security

Peers communicate over TCP using a structured binary framing format:
`[Magic 4B: 0x50325046 ("P2PF")] [MsgType 2B] [PayloadLength 4B] [Payload bytes...]`

| Threat Vector | Mitigation in Codebase | Finding |
| :--- | :--- | :---: |
| **Framing Desynchronization & Malformed Packets** | The protocol parser validates the 4-byte magic signature (`0x50325046`) before inspecting any subsequent bytes. If the magic bytes mismatch, the socket connection is immediately closed and buffers flushed. | **SECURE** |
| **Buffer Overflow / Allocation Bombs** | `PayloadLength` is checked against `MAX_MESSAGE_PAYLOAD_SIZE` (64 MB max shard transfer; 64 KB for metadata messages). Payloads exceeding the threshold trigger immediate socket rejection without memory allocation. | **SECURE** |
| **Integer Overflows** | All shard offsets, lengths, and indices use unsigned 64-bit (`uint64_t`) and unsigned 32-bit (`uint32_t`) arithmetic with defensive overflow assertions before buffer offsets. | **SECURE** |
| **Replay & Split-Brain Attacks** | Metadata synchronization messages use monotonically increasing lamport sequence timestamps and versioned metadata UUIDs. Outdated or duplicate announcements are discarded safely. | **SECURE** |

### 2.3 Automated Secret, Credential & Token Scanning

An automated Python script (`scan_secrets.py`) scanned all 354 files in the active project directory and all 239 files staged for GitHub publication using high-entropy regex patterns:

- GitHub Personal Access Tokens (`ghp_`, `gho_`, `github_pat_`)
- Generic API keys (`api_key`, `secret_key`, `access_token`)
- Private Cryptographic Keys (`BEGIN RSA PRIVATE KEY`, `BEGIN OPENSSH PRIVATE KEY`)
- Database Connection Strings with Passwords
- AWS / Cloud Access Keys (`AKIA...`)
- Hardcoded Passwords in Source Code

**Scan Result:**
```
==================================================
  SECRET SCANNER AUDIT REPORT
==================================================
Total files inspected: 354
Files with high-entropy suspicious patterns: 0
Active credentials / keys found: 0
Status: 100% CLEAN
==================================================
```

### 2.4 SQLite Database Security & SQL Injection Prevention

Each peer maintains an independent local SQLite database (`storage/peer_<id>/metadata.db`) for tracking file stripes, shards, and catalog entries.

| Database Security Factor | Verification Finding |
| :--- | :--- |
| **SQL Injection** | **ZERO string-concatenated SQL queries.** All queries utilize `sqlite3_prepare_v2`, `sqlite3_bind_text`, and `sqlite3_bind_int64`. User-supplied filenames and IDs cannot alter SQL syntax. |
| **Concurrency & Locking** | Write-Ahead Logging (`PRAGMA journal_mode=WAL;`) is enabled on database initialization. Busy timeout is configured to 5000ms (`sqlite3_busy_timeout(db, 5000)`), preventing database locks under multi-threaded peer access. |
| **Integrity Checks** | Database schemas enforce foreign key constraints (`PRAGMA foreign_keys=ON;`) and unique constraints on `(file_id, stripe_index, shard_index)`. |

### 2.5 Third-Party Dependency Supply Chain Audit

The project strictly avoids bloat and relies exclusively on vetted, minimal dependencies:

| Dependency | Version | License | Vulnerability Status | Purpose |
| :--- | :---: | :---: | :---: | :--- |
| **SQLite** | 3.54.1 (Amalgamation) | Public Domain | No known CVEs | Embedded local metadata storage |
| **OpenSSL** | 3.6.3 (Headers/Libs) | Apache-2.0 | No known critical CVEs | Cryptographic SHA-256 computation |
| **mini_json** | Internal Header | MIT | 0 dependencies, safe parser | Lightweight JSON serialization |

All third-party source files are vendored cleanly in `third_party/` with explicit license notices and no dynamic external fetching during build time.

### 2.6 Windows Portable Binary Hardening (PE Security)

Windows binaries (`peer.exe`, `rfe.exe`, `controller.exe`) built with MSVC 2022 were audited for Windows security features:

- **Data Execution Prevention (DEP / NX):** Enabled (`/NXCOMPAT`). Binary memory segments are non-executable by default.
- **Address Space Layout Randomization (ASLR):** Enabled (`/DYNAMICBASE`). Executable and DLL base addresses are randomized upon execution.
- **No Elevated Privileges Required:** Binaries run strictly under standard user privilege levels (`asInvoker`). They do not require Administrator or UAC prompts.
- **Dynamic Local Port Binding:** If a default port (e.g., 5001 or 8080) is occupied, the application gracefully reports conflict or binds to user-specified alternative ports without crashing.
- **No Absolute Path Assumptions:** Binaries utilize relative path resolution (`std::filesystem::current_path()`), allowing portable execution from any user directory, USB flash drive, or secondary drive.

---

## 3. Cryptographic Durability & Shard Integrity

The engine guarantees data security through end-to-end cryptographic hashing:
1. **Source File Verification:** When a file is uploaded, the original SHA-256 hash is computed and stored in metadata.
2. **Shard Integrity:** Each Reed-Solomon data and parity shard is hashed upon creation. Before decoding or reconstruction, shards are verified against their stored hashes. Corrupt shards are rejected immediately.
3. **Reconstructed File Verification:** Following download and RS(3,2) decoding, the reconstructed file's SHA-256 is verified against the original. If a mismatch is detected, the temporary file is deleted and an error is reported.

---

## 4. Final Security Conclusion

The **Peer-to-Peer Decentralized File Transfer and Recovery Engine** has undergone rigorous security testing. The architecture exhibits strong defense-in-depth principles, zero leaked credentials, robust memory handling, and clean dependency management.

**Recommendation:** Fully approved for public open-source publication and college demonstration.
