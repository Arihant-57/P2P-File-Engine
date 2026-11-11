# Four-Laptop Physical LAN Demonstration Guide (`FOUR_LAPTOP_DEMO_GUIDE.md`)

This step-by-step runbook is designed for the live **4-physical-laptop college project demonstration**.

---

## Pre-Demo Setup Summary

| Laptop | Peer ID | Example LAN IP | TCP Port | UDP Discovery Port | Config File |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Laptop 1** | `A` | `192.168.1.101` | `5001` | `5005` | `configs\peer_A.json` |
| **Laptop 2** | `B` | `192.168.1.102` | `5002` | `5005` | `configs\peer_B.json` |
| **Laptop 3** | `C` | `192.168.1.103` | `5003` | `5005` | `configs\peer_C.json` |
| **Laptop 4** | `D` | `192.168.1.104` | `5004` | `5005` | `configs\peer_D.json` |

*(Replace `192.168.1.101–104` with the actual IPv4 addresses of the 4 laptops on your demonstration Wi-Fi/LAN).*

---

## 15-Step Live Demonstration Procedure

### Step 1 — Start Four Peers on Four Physical Laptops
Open a PowerShell terminal in the project root on each laptop and start its peer daemon:
- **Laptop 1:** `.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_A.json`
- **Laptop 2:** `.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_B.json`
- **Laptop 3:** `.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_C.json`
- **Laptop 4:** `.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_D.json`

**Expected Observation:** Each terminal prints:
`[peer <ID>] ONLINE at <IP>:<PORT> storage=storage/peer_<ID>/chunks db=storage/peer_<ID>/metadata.db`

---

### Step 2 — Verify Automatic Peer Discovery
Wait 2 seconds, then on **Laptop 1** (or any laptop) open a second terminal and run:
```powershell
.\build\Release\rfe.exe peers --peer 192.168.1.101:5001
```
**Expected Observation:** All 4 peers (`A`, `B`, `C`, `D`) appear in the peer membership table with their respective LAN IP addresses and ports.

---

### Step 3 — Verify Direct TCP Connectivity
Query the peer table directly from **Laptop 4 (`Peer D`)**:
```powershell
.\build\Release\rfe.exe connect 192.168.1.104:5004
.\build\Release\rfe.exe peers --peer 192.168.1.104:5004
```
**Expected Observation:** `rfe connect` reports connection latency in milliseconds and `rfe peers` confirms `Peer D` has completed direct TCP handshakes with `A`, `B`, and `C`.

---

### Step 4 — Verify Heartbeat Monitoring
Run `status` on **Laptop 2 (`Peer B`)**:
```powershell
.\build\Release\rfe.exe status --peer 192.168.1.102:5002
```
**Expected Observation:** All 4 peers show health state `ONLINE`, `0` missed heartbeats, and live available storage in GB.

---

### Step 5 — Upload a File Through Peer A (Laptop 1)
On **Laptop 1**, prepare or choose a test file (e.g., `demo_report.pdf` or a 200 KB binary/image file) and compute its original SHA-256:
```powershell
Get-FileHash .\demo_report.pdf -Algorithm SHA256
.\build\Release\rfe.exe upload .\demo_report.pdf --peer 192.168.1.101:5001
```
**Expected Observation:**
- Output displays `DECENTRALIZED UPLOAD SUCCESS`, the generated `File ID`, original size, full `SHA-256`, `RS(3,2)` stripe/shard counts, and the `SHARD DISTRIBUTION ACROSS PEERS` log showing each stripe's 3 shards (`D0`, `D1`, `P0`) placed on **3 distinct peers**.

---

### Step 6 — Verify Shard Distribution Across Different Peers
On each laptop, list the local shard directory:
```powershell
Get-ChildItem .\storage\peer_*\chunks\*.chunk
```
**Expected Observation:** Physical `.chunk` binary files (`65,536` bytes each) are distributed across the laptops' local `chunks/` directories, and no single laptop holds more than 1 shard of the same stripe.

---

### Step 7 — Search for the File Through Peer D (Laptop 4)
On **Laptop 4**, search the distributed catalogue without contacting Laptop 1 directly:
```powershell
.\build\Release\rfe.exe search demo_report.pdf --peer 192.168.1.104:5004
```
**Expected Observation:** `Peer D` returns the matching `File ID`, `Filename`, `File Size`, `SHA-256`, `Availability: AVAILABLE (Reconstructable: YES)`, `Storage Peers`, and the complete `Shard Map` (`<shard_id>@<peer_id>`).

---

### Step 8 — Download the File Through Peer D (Laptop 4)
On **Laptop 4**, download and reconstruct the file:
```powershell
.\build\Release\rfe.exe download demo_report.pdf --peer 192.168.1.104:5004 --output .\output\downloaded_on_D.pdf
```
**Expected Observation:** Output displays `DECENTRALIZED DOWNLOAD COMPLETE`, `Integrity Check: VERIFIED`, and the per-shard retrieval log showing shards fetched directly over TCP from the hosting laptops.

---

### Step 9 — Compare SHA-256 Between Laptop 1 and Laptop 4
On **Laptop 4**, run:
```powershell
Get-FileHash .\output\downloaded_on_D.pdf -Algorithm SHA256
```
**Expected Observation:** The SHA-256 hash on Laptop 4 matches the original file's SHA-256 on Laptop 1 byte-for-byte.

---

### Step 10 — Shut Down One Storage Peer (e.g., Laptop 2 / Peer B)
On **Laptop 2 (`Peer B`)**, press `Ctrl+C` in the `peer.exe` terminal (or run `.\scripts\stop_peer_lan.ps1`).
- Also test shutting down **Laptop 1 (`Peer A`, the original uploader)** after an upload to demonstrate that downloading on Laptop 4 still succeeds when the original uploader is offline!

---

### Step 11 — Verify Autonomous Failure Detection
Wait ~1.5–2 seconds and watch the `peer.exe` consoles on Laptops 1, 3, and 4, or run on **Laptop 4**:
```powershell
.\build\Release\rfe.exe peers --peer 192.168.1.104:5004
```
**Expected Observation:**
- Surviving peers log `HEALTH CHANGE: Peer B ... -> SUSPECT` followed by `-> OFFLINE`.
- `rfe peers` shows `Peer B` in state `OFFLINE` while `A`, `C`, `D` remain `ONLINE`.

---

### Step 12 — Verify Autonomous Reconstructed Shard Storage on the 4th Peer
Within 1–2 seconds of `Peer B` reaching `OFFLINE`, check the `peer.exe` console on the surviving laptops and run on **Laptop 4**:
```powershell
.\build\Release\rfe.exe search demo_report.pdf --peer 192.168.1.104:5004
.\build\Release\rfe.exe repair --peer 192.168.1.104:5004
```
**Expected Observation:**
- Console logs `[P2P-Recovery] Rebuilt <shard_id> (from failed peer B) -> stored on Peer <replacement_peer>`.
- `rfe search` shows `Availability: AVAILABLE` (`Online Shards: 6/6` or `3/3`) with `Peer B`'s shards now hosted on the remaining 3 online peers (`A`, `C`, `D`)!
- `rfe repair` displays the `COMPLETED` recovery records in SQLite.
- Downloading the file again on Laptop 4 succeeds with verified SHA-256.

---

### Step 13 — Restart the Failed Peer (Laptop 2 / Peer B)
On **Laptop 2**, restart `Peer B`:
```powershell
.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_B.json
```
**Expected Observation:** `Peer B` comes back `ONLINE` and automatically performs TCP handshakes and anti-entropy metadata sync with `A`, `C`, and `D`.

---

### Step 14 — Verify Metadata Reconciliation and Stale Shard Cleanup on Rejoined Peer
On **Laptop 2 (`Peer B`)**, inspect its catalogue and local `chunks/` folder:
```powershell
.\build\Release\rfe.exe info <FILE_ID> --peer 192.168.1.102:5002
Get-ChildItem .\storage\peer_B\chunks\*.chunk
```
**Expected Observation:**
- `Peer B`'s SQLite database has updated to the newer manifest version (`version > 2`).
- The shards that were replaced while `Peer B` was offline are marked `STALE` for `Peer B` and `STORED` on their new replacement peer, and `Peer B` has automatically deleted its obsolete duplicate `.chunk` files from `storage\peer_B\chunks\`.

---

### Step 15 — Confirm Zero Coordinator Process Is Running
On all 4 laptops, run:
```powershell
Get-Process peer, controller -ErrorAction SilentlyContinue
```
**Expected Observation:** Only `peer` is running on each laptop; no `controller` or central coordinator process exists anywhere on the network.
