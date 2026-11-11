# User Guide (`USER_GUIDE.md`)

**Project:** Peer-to-Peer Decentralized File Transfer and Recovery Engine  
**Executables:** `peer.exe` (Decentralized Peer Daemon), `rfe.exe` (User CLI), `controller.exe` (Automated Verification Runner)

---

## 1. Running a Decentralized Peer Node (`peer.exe`)

Every participating machine runs `peer.exe`. It acts simultaneously as a TCP server, TCP client, UDP discovery broadcaster/listener, SQLite metadata node, and autonomous `RS(3,2)` recovery participant.

### Option A — Start Using a Per-Peer JSON Configuration File
```powershell
.\build\Release\peer.exe --config configs\peer_A.json
```
*(Or via the PowerShell wrapper: `.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_A.json`)*

### Option B — Start Using Command-Line Arguments
```powershell
.\build\Release\peer.exe <PEER_ID> <BIND_IP> <TCP_PORT> p2p <UDP_DISCOVERY_PORT> [--max-storage-mb <MB>] [--seed <IP:PORT>]
```
Example:
```powershell
.\build\Release\peer.exe A 192.168.1.101 5001 p2p 5005 --max-storage-mb 10240 --seed 192.168.1.102:5002
```

---

## 2. User Command-Line Interface (`rfe.exe`)

All `rfe.exe` commands accept `--peer <ip:port>` to target **any** online peer on the LAN, or you can bind a default session peer once using `rfe connect <ip:port>`.

### 2.1 Connect Default Session to Any Peer
```powershell
.\build\Release\rfe.exe connect 192.168.1.101:5001
```
Saves the active peer endpoint in `metadata/session.json` so subsequent `rfe` commands on that laptop do not require `--peer`.

### 2.2 Inspect Peer Membership & Health (`peers`)
```powershell
.\build\Release\rfe.exe peers --peer 192.168.1.101:5001
```
Displays every known peer (`A`, `B`, `C`, `D`), its `IP:Port`, health state (`ONLINE`, `SUSPECT`, `OFFLINE`), available storage capacity, and active transfer load.

### 2.3 Upload a File Through Any Peer (`upload`)
```powershell
.\build\Release\rfe.exe upload "D:\Files\lecture_notes.pdf" --peer 192.168.1.101:5001
```
- Computes original size and full-file SHA-256.
- Sends the file to the target peer (`192.168.1.101:5001`).
- Adaptively chunks the file (`64 KB` / `256 KB` / `1 MB`), encodes each stripe with systematic `RS(3,2)` (`D0`, `D1`, `P0`), distributes the 3 shards of each stripe across **3 distinct online peers**, and synchronizes the `AVAILABLE` manifest across all peers' SQLite databases.

### 2.4 List All Files in the Distributed Catalogue (`files`)
```powershell
.\build\Release\rfe.exe files --peer 192.168.1.104:5004
```
Lists all files known to the peer network along with `File ID`, `Filename`, `Size`, `Live Status` (`AVAILABLE`, `DEGRADED`, `UNRECOVERABLE`), `Shards Online` ratio (`3/3`, `6/6`, etc.), `Hosting Peers`, and `SHA-256`.

### 2.5 Search for a File by Filename or File ID (`search`)
```powershell
.\build\Release\rfe.exe search lecture_notes.pdf --peer 192.168.1.104:5004
```
Performs a case-insensitive substring or exact File ID search on the contacted peer's SQLite catalogue (automatically syncing any missing metadata from online peers first) and displays the per-shard storage map (`<shard_id>@<peer_id>`).

### 2.6 View Detailed File Manifest & Stripe/Shard Map (`info`)
```powershell
.\build\Release\rfe.exe info <FILE_ID> --peer 192.168.1.104:5004
```
Displays the full manifest header, coding parameters, original uploader peer ID, and every shard's stripe index, type (`DATA` / `PARITY`), unpadded byte length, active storage peer, and SHA-256 prefix.

### 2.7 Download and Reconstruct a File Through Any Peer (`download`)
```powershell
.\build\Release\rfe.exe download <FILE_ID_OR_FILENAME> --peer 192.168.1.104:5004 --output "D:\Downloads\lecture_notes.pdf"
```
- Resolves the manifest and peer endpoints from the contacted peer.
- Connects directly over TCP (`GET_CHUNK`) to the storage peers holding each stripe's shards.
- Verifies each shard's SHA-256 and reconstructs any missing or corrupted shard via `RS(3,2)` erasure decoding.
- Verifies the reconstructed file's full SHA-256 against the manifest before committing the output file to disk.

### 2.8 Trigger or Inspect Autonomous Durability Repair (`repair`)
```powershell
.\build\Release\rfe.exe repair --peer 192.168.1.101:5001
```
Triggers an immediate durability scan on the target peer and prints the synchronized `recovery_records` audit table (`recovery_id`, `failed_peer_id`, `rebuilder_peer_id`, `target_peer_id`, `status`). Note that peers also run this scan automatically in the background whenever any peer transitions to `OFFLINE`.

### 2.9 Cluster & Transfer Status (`status`, `transfers`)
```powershell
.\build\Release\rfe.exe status --peer 192.168.1.101:5001
.\build\Release\rfe.exe transfers --peer 192.168.1.101:5001
```
Displays cluster-wide online/offline peer counts, aggregate free storage, catalogue health counts, and active transfer slots.
