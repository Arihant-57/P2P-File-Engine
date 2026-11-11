# Four-Laptop Windows LAN Deployment Guide (`LAN_DEPLOYMENT_GUIDE.md`)

This guide explains how to deploy and run the **Peer-to-Peer Decentralized File Transfer and Recovery Engine** across **four physical Windows laptops** connected to the same Local Area Network (Wi-Fi router, mobile hotspot, or Ethernet switch) without any central coordinator or central database.

---

## 1. Network Topology Example

Each laptop runs one independent instance of `peer.exe` and has `rfe.exe` available for user commands:

| Machine | Peer ID | Example LAN IP | TCP Port | UDP Discovery Port | Local SQLite DB | Local Shard Dir | Config File |
| :--- | :---: | :---: | :---: | :---: | :--- | :--- | :--- |
| **Laptop 1** | `A` | `192.168.1.101` | `5001` | `5005` | `storage/peer_A/metadata.db` | `storage/peer_A/chunks` | `configs/peer_A.json` |
| **Laptop 2** | `B` | `192.168.1.102` | `5002` | `5005` | `storage/peer_B/metadata.db` | `storage/peer_B/chunks` | `configs/peer_B.json` |
| **Laptop 3** | `C` | `192.168.1.103` | `5003` | `5005` | `storage/peer_C/metadata.db` | `storage/peer_C/chunks` | `configs/peer_C.json` |
| **Laptop 4** | `D` | `192.168.1.104` | `5004` | `5005` | `storage/peer_D/metadata.db` | `storage/peer_D/chunks` | `configs/peer_D.json` |

> **Important:** The laptops do **not** need to use `192.168.1.101–104`. Use each laptop's actual IPv4 address on your LAN (e.g., `10.x.x.x`, `172.20.x.x`, or `192.168.x.x`). Since each laptop has its own unique IP address, all 4 laptops can either use distinct TCP ports (`5001`–`5004`) or the same TCP port (e.g., `5001`).

---

## 2. Step-by-Step Preparation on Each Laptop

### Step 2.1 — Build or Copy the Release Binaries
Either build the project on each laptop:
```powershell
cmake -S . -B build -DOPENSSL_ROOT_DIR="$PWD/third_party/openssl"
cmake --build build --config Release
```
**OR** build once on Laptop 1 and copy the project folder (including `build\Release\`, `configs\`, and `scripts\`) to Laptops 2, 3, and 4.

### Required Files in `build\Release\`
Verify that the following files exist in `build\Release\` on every laptop (CMake copies the OpenSSL DLL automatically during build):
- `peer.exe` — Decentralized peer daemon
- `rfe.exe` — User CLI tool
- `controller.exe` — Automated test runner
- `libcrypto-3-x64.dll` — OpenSSL 3 SHA-256 runtime library

---

### Step 2.2 — Find Each Laptop's LAN IPv4 Address
On each laptop, open PowerShell and run:
```powershell
Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*" } | Select-Object InterfaceAlias, IPAddress
```
Record the 4 IPv4 addresses:
- Laptop 1 (`Peer A`): `<IP_A>` (e.g. `192.168.1.101`)
- Laptop 2 (`Peer B`): `<IP_B>` (e.g. `192.168.1.102`)
- Laptop 3 (`Peer C`): `<IP_C>` (e.g. `192.168.1.103`)
- Laptop 4 (`Peer D`): `<IP_D>` (e.g. `192.168.1.104`)

---

### Step 2.3 — Configure Windows Defender Firewall on Each Laptop
1. Ensure the Wi-Fi / Ethernet network profile on each Windows laptop is set to **Private** (Settings $\rightarrow$ Network & Internet $\rightarrow$ Properties $\rightarrow$ Network profile type: **Private**).
2. Open PowerShell **as Administrator** on each laptop and run:
   ```powershell
   .\scripts\configure_lan_firewall.ps1 -PortRange "5001-5010"
   ```
   *(Or manually run:)*
   ```powershell
   New-NetFirewallRule -DisplayName "RFE Decentralized P2P TCP" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5001-5010 -Profile Private,Domain
   New-NetFirewallRule -DisplayName "RFE Decentralized P2P UDP" -Direction Inbound -Action Allow -Protocol UDP -LocalPort 5001-5010 -Profile Private,Domain
   ```

---

### Step 2.4 — Update Per-Peer Configuration Files (`configs/peer_A.json` .. `peer_D.json`)
Edit `configs/peer_A.json` on Laptop 1 (and correspondingly `peer_B.json`, `peer_C.json`, `peer_D.json` on Laptops 2–4) with the actual LAN IPs recorded in Step 2.2:

```json
{
  "peer_id": "A",
  "address": "192.168.1.101",
  "port": 5001,
  "mode": "p2p",
  "discovery_port": 5005,
  "max_storage_mb": 10240,
  "seed_peers": [
    { "address": "192.168.1.102", "port": 5002 },
    { "address": "192.168.1.103", "port": 5003 },
    { "address": "192.168.1.104", "port": 5004 }
  ]
}
```
> **Why `seed_peers` is included:** On standard home/lab routers, UDP broadcast (`255.255.255.255:5005` and `<subnet>.255:5005`) discovers all peers automatically with zero seed configuration. On university Wi-Fi or mobile hotspots that block broadcast packets (AP client isolation / multicast filtering), `seed_peers` sends direct unicast UDP probes to the listed IPs so discovery still completes automatically.

---

## 3. Starting and Stopping Peers on the 4 Laptops

### Starting Peers (Using Configuration Files)
```powershell
# On Laptop 1:
.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_A.json

# On Laptop 2:
.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_B.json

# On Laptop 3:
.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_C.json

# On Laptop 4:
.\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_D.json
```

### Starting Peers (Directly via `peer.exe` CLI Without Editing JSON)
```powershell
# On Laptop 1:
.\build\Release\peer.exe A 192.168.1.101 5001 p2p 5005 --max-storage-mb 10240 --seed 192.168.1.102:5002 --seed 192.168.1.103:5003 --seed 192.168.1.104:5004
```

### Stopping a Peer
Press `Ctrl+C` in the terminal running `peer.exe`, or run from another PowerShell window on that laptop:
```powershell
.\scripts\stop_peer_lan.ps1
```

---

## 4. Storage Directory & SQLite Database Initialization

When `peer.exe` starts on a laptop (e.g., Peer `A`), it automatically initializes:
- `storage\peer_A\chunks\` — Local directory storing binary `RS(3,2)` shard files (`<shard_id>.chunk`).
- `storage\peer_A\staging\` — Temporary upload/encoding workspace (automatically cleaned up after each upload and on startup).
- `storage\peer_A\metadata.db` — Embedded SQLite 3.54.1 database in `WAL` mode (`metadata.db-wal`, `metadata.db-shm`).

To perform a completely clean reset on a laptop before a fresh demo:
```powershell
.\scripts\stop_peer_lan.ps1
Remove-Item -Recurse -Force storage\peer_* -ErrorAction SilentlyContinue
```

---

## 5. Troubleshooting Guide

| Symptom | Cause | Solution |
| :--- | :--- | :--- |
| Peers on different laptops stay at 1 peer (`self` only) in `rfe peers` | Windows Firewall blocking inbound TCP/UDP `5001–5005` or network profile set to `Public`. | Switch Wi-Fi profile to **Private** and run `.\scripts\configure_lan_firewall.ps1` as Administrator (or add `-FirewallProfile Any`). |
| `ping 192.168.1.102` fails between laptops | Wi-Fi router / campus hotspot has **AP Client Isolation** enabled. | Connect all laptops to a personal mobile hotspot (with client isolation off) or an unmanaged Ethernet/Wi-Fi router. |
| UDP broadcast blocked by router, but TCP works | Router drops `255.255.255.255` UDP broadcast frames. | Pass `--seed <other_ip:port>` or populate `seed_peers` in `configs/peer_X.json` so `UDPBeacon` uses direct unicast UDP packets. |
| `peer.exe` fails to bind `<IP>:5001` | The IP in `configs/peer_X.json` does not belong to any local network adapter on that laptop, or another `peer.exe` is already bound to `5001`. | Run `.\scripts\stop_peer_lan.ps1` and verify the laptop's IPv4 address with `ipconfig`. |
| Upload fails with `requires at least 3 ONLINE eligible peers` | Fewer than 3 laptops are currently `ONLINE` (remember `RS(3,2)` places 3 shards per stripe on 3 distinct peers). | Ensure at least 3 peers appear as `ONLINE` in `.\build\Release\rfe.exe peers --peer <ip:port>`. |
