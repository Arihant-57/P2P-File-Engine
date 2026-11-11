# Security Posture and Architectural Limitations

This document provides a transparent engineering assessment of the security controls implemented in the **Peer-to-Peer Decentralized File Transfer and Recovery Engine** as well as the known architectural boundaries and limitations of the current LAN prototype.

---

## 1. Implemented Security Controls

1. **Path Traversal & Identifier Sanitization:**
   - **Shard / Chunk IDs (`safe_chunk_id` in `peer/peer.cpp`):** Every incoming `STORE_CHUNK` and `GET_CHUNK` request strictly validates `chunk_id` to allow only `[A-Za-z0-9._-]` (1..128 chars) and rejects `..`, `/`, `\`, or drive colons, preventing directory traversal writes or reads outside `storage/peer_<ID>/chunks/`.
   - **Peer IDs (`safe_peer_id` in `peer/peer.cpp`):** Incoming `P2P_HANDSHAKE`, `P2P_HEARTBEAT`, and UDP beacon frames validate `peer_id` (`[A-Za-z0-9_-]`, 1..32 chars) before registering peers or constructing filesystem paths.
   - **Downloaded Filenames (`safe_filename_only` in `peer/peer.cpp` & `client_download.cpp`):** Reconstructed output filenames extracted from remote manifests are stripped to their basename (`fs::path::filename()`) and sanitized so a malicious manifest `original_name` (such as `../../Windows/evil.dll`) cannot write outside the target output directory.
   - **Loopback Restriction on `file_path` Upload Shortcut (`cli/rfe.cpp` & `peer/peer.cpp`):** The local zero-copy `file_path` shortcut in `P2P_UPLOAD_FILE` is only used when `rfe.exe` connects over loopback (`127.0.0.1` / `localhost`) and rejects any path containing `..`. Remote peers always transmit raw file bytes in the framed TCP payload.

2. **Parameterized SQLite Queries (SQL Injection Prevention):**
   - Every SQL statement in `engine/db/database_manager.cpp` uses `sqlite3_prepare_v2` with bound parameters (`?1`, `?2`, ..., `sqlite3_bind_text` / `sqlite3_bind_int64`). User-supplied filenames, search queries, and peer IDs are never concatenated into raw SQL strings.

3. **Bounded Wire Protocol Framing (Memory Exhaustion Prevention):**
   - `proto::recv_message` (`engine/communication/protocol.cpp`) enforces strict upper bounds on incoming frames before allocating memory:
     - `MAX_HEADER_SIZE = 4 MB` (`4 * 1024 * 1024` bytes)
     - `MAX_PAYLOAD_SIZE = 128 MB` (`128 * 1024 * 1024` bytes)
   - All sockets enforce non-blocking `select()` connect timeouts and `SO_RCVTIMEO` / `SO_SNDTIMEO` read/write timeouts so a stalled or misbehaving peer cannot hang a server thread indefinitely.

4. **End-to-End Cryptographic Data Integrity (SHA-256):**
   - Every shard (`STORE_CHUNK`, `GET_CHUNK`, RS(3,2) repair) and every reconstructed file is verified against its OpenSSL EVP SHA-256 digest before atomic `.tmp` → final file rename. Corrupted bytes in transit or on disk are rejected immediately and never exposed to the user.

---

## 2. Known Architectural Limitations & Future Work

1. **Same-LAN / Subnet Scope (No NAT Traversal or WAN Relay):**
   - The system is designed for computers on the same local area network (LAN / Wi-Fi subnet). It does not implement STUN/TURN/ICE NAT traversal, UPnP port forwarding, or WAN DHT routing.

2. **Unencrypted Plaintext TCP & Unauthenticated Peer Membership:**
   - All TCP control and shard transfer frames are transmitted in plaintext over the LAN without TLS encryption or mutual cryptographic peer authentication (mTLS / Ed25519 signatures). The system assumes a trusted institutional/campus LAN demo environment; on an untrusted network, a malicious node could spoof a `peer_id` or push forged manifest versions.

3. **RS(3,2) Fault Tolerance Bound (1 Peer Failure per Stripe):**
   - Reed-Solomon `RS(k=2, m=1)` encodes each stripe into 2 data shards and 1 parity shard across 3 distinct peers.
   - Each stripe can tolerate **1 simultaneous peer failure** (or 1 corrupted shard) with full recovery.
   - If **2 or more peers holding shards of the same stripe fail simultaneously** before autonomous recovery completes onto a 4th peer, fewer than $k=2$ shards survive and that stripe is deterministically marked `UNRECOVERABLE`.
   - Uploading a new file requires at least **3 distinct online peers** with sufficient free storage.

4. **Single-Payload Wire Ceiling for Remote Uploads (`128 MB`):**
   - While local loopback uploads stream directly from disk via `AdaptiveChunker::encode_file` without buffering the whole file in RAM, remote CLI uploads (`rfe upload` to a non-loopback `--peer <ip:port>`) transmit the file in a single `P2P_UPLOAD_FILE` frame bounded by `MAX_PAYLOAD_SIZE = 128 MB`. Multi-gigabyte remote uploads would require multi-frame chunked streaming from `rfe.exe`.

5. **Physical 4-Laptop Hardware Verification Scope:**
   - All multi-process networking, UDP broadcast/unicast discovery, TCP handshakes, RS(3,2) shard transfers, autonomous failure recovery, and extracted portable ZIP execution have been verified using 4 independent OS processes (`peer.exe`) and real Winsock TCP/UDP sockets on Windows x64. Physical execution across 4 separate hardware laptops requires the user's LAN environment and Windows Firewall rules (`configure_lan_firewall.ps1`).
