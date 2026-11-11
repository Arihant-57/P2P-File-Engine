# Portable Release Guide (Windows x64)

This guide explains how to extract, configure, and run the **Windows x64 Portable Release Package** (`dist/P2P_File_Engine_Windows_x64.zip`) on one laptop (local multi-peer demo) or across 4 separate Windows laptops on a LAN/Wi-Fi network **without installing Visual Studio, CMake, SQLite, or OpenSSL**.

---

## 1. System Requirements

- **Operating System:** Windows 10 or Windows 11 (64-bit / x64).
- **Runtime Libraries:** Standard Windows Visual C++ 2015–2022 Redistributable (x64), which is pre-installed on standard Windows 10/11 systems. OpenSSL (`libcrypto-3-x64.dll`) is bundled directly next to `peer.exe` and `rfe.exe`, and SQLite 3 is statically linked into both executables.
- **Network:** Local loopback (`127.0.0.1`) for single-PC testing, or a shared LAN / Wi-Fi hotspot (`Private` network profile recommended) for multi-laptop operation.

---

## 2. Extracting the Portable Package

1. Copy `P2P_File_Engine_Windows_x64.zip` to any folder on your computer (for example, `C:\P2P_File_Engine\`).
2. Right-click `P2P_File_Engine_Windows_x64.zip` → **Extract All...** (or run `Expand-Archive .\P2P_File_Engine_Windows_x64.zip -DestinationPath C:\P2P_File_Engine`).
3. Open **PowerShell** inside the extracted folder.

### Package Directory Layout

```text
P2P_File_Engine_Windows_x64/
├── peer.exe                      # Decentralized P2P node daemon
├── rfe.exe                       # Native command-line client (14 operations)
├── libcrypto-3-x64.dll           # Bundled OpenSSL 3 x64 cryptographic runtime
├── web/                          # Embedded Web Dashboard static assets (HTML/CSS/JS)
├── setup_peer.ps1                # Interactive peer configuration wizard (UI port/token support)
├── start_peer.ps1                # Starts peer.exe using your configured profile
├── stop_peer.ps1                 # Stops running peer.exe processes cleanly
├── configure_lan_firewall.ps1    # Optional Windows Firewall rule helper (Run as Admin)
├── START_HERE.txt                # Quick-reference cheat sheet
├── THIRD_PARTY_NOTICES.txt       # SQLite & OpenSSL license notices
├── configs/
│   ├── peer_A.json               # Pre-configured profile for Peer A (port 5701)
│   ├── peer_B.json               # Pre-configured profile for Peer B (port 5702)
│   ├── peer_C.json               # Pre-configured profile for Peer C (port 5703)
│   └── peer_D.json               # Pre-configured profile for Peer D (port 5704)
├── docs/                         # User Guide, CLI Guide, Web Guide, Architecture & Demo Guides
├── storage/                      # Created automatically per peer (chunks + metadata.db)
└── output/                       # Default download destination folder
```

---

## 3. Option A — Interactive Setup Wizard (`setup_peer.ps1`)

To configure a laptop interactively:

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_peer.ps1
```

The wizard prompts for:
1. **Peer ID** (`A`, `B`, `C`, `D`, or custom identifier)
2. **Bind/Advertise IPv4 Address** (auto-detects your LAN IPv4 address)
3. **TCP Listen Port** (defaults to `5701` for `A`, `5702` for `B`, `5703` for `C`, `5704` for `D`)
4. **UDP Discovery Port** (default `5705`)
5. **Storage Directory** (default `storage/peer_<ID>`)
6. **Max Storage Contribution in MB** (`0` = unlimited / disk free space)
7. **Optional Seed Peer `<ip:port>`** (useful on Wi-Fi hotspots that block UDP broadcast)

It saves your configuration to `configs/peer_<ID>.json` and `configs/active_peer.json`.

Then launch your peer:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_peer.ps1
```

To stop your peer at any time:

```powershell
powershell -ExecutionPolicy Bypass -File .\stop_peer.ps1
```

---

## 4. Option B — Running a 4-Peer Cluster on a Single Windows PC

You can run all 4 peers simultaneously in the background from the extracted portable folder:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_peer.ps1 -PeerId A -Background
powershell -ExecutionPolicy Bypass -File .\start_peer.ps1 -PeerId B -Background
powershell -ExecutionPolicy Bypass -File .\start_peer.ps1 -PeerId C -Background
powershell -ExecutionPolicy Bypass -File .\start_peer.ps1 -PeerId D -Background
```

Verify the 4-peer mesh:

```powershell
.\rfe.exe peers --peer 127.0.0.1:5701
.\rfe.exe status --peer 127.0.0.1:5704
```

Upload, search, and download a file:

```powershell
.\rfe.exe upload .\START_HERE.txt --peer 127.0.0.1:5701
.\rfe.exe search START_HERE.txt --peer 127.0.0.1:5704
.\rfe.exe download START_HERE.txt --peer 127.0.0.1:5704 --output .\output\restored_START_HERE.txt
```

Stop all 4 background peers when done:

```powershell
powershell -ExecutionPolicy Bypass -File .\stop_peer.ps1
```

---

## 5. Option C — Running Across 4 Separate Laptops on a LAN

1. Extract `P2P_File_Engine_Windows_x64.zip` on each of the 4 laptops (`Laptop 1`..`Laptop 4`).
2. On each laptop, open an **Administrator PowerShell** once and run:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\configure_lan_firewall.ps1
   ```
3. Run `.\setup_peer.ps1` on each laptop (assigning Peer IDs `A`, `B`, `C`, `D` and each laptop's actual LAN IPv4 address, e.g. `192.168.1.101`..`192.168.1.104`).
4. Run `.\start_peer.ps1` on each laptop.
5. See `docs/FOUR_LAPTOP_DEMO_GUIDE.md` for the step-by-step live demonstration script.

---

## 6. Accessing the Web Dashboard

Every running `peer.exe` automatically serves its embedded local Web Dashboard.
1. Open your web browser:
   - For Peer A: `http://127.0.0.1:8080` (or the configured `--ui-port`)
   - For Peer B: `http://127.0.0.1:8081`
   - For Peer C: `http://127.0.0.1:8082`
   - For Peer D: `http://127.0.0.1:8083`
2. You can view real-time node health, live peer mesh discovery, catalogue search, upload files directly via the browser drag-and-drop studio, initiate streaming downloads, inspect durability recovery logs, and view storage telemetry.
3. If LAN access was enabled via `--ui-lan`, peers on the LAN can access the dashboard at `http://<laptop-ip>:<ui-port>` (and enter the optional `--ui-token` if configured).

---

## 7. Package Verification & SHA-256 Checksums

Verify the authenticity of your release packages using PowerShell:

```powershell
Get-FileHash -Path dist\P2P_File_Engine_Windows_x64.zip -Algorithm SHA256
Get-FileHash -Path dist\P2P_File_Engine_Source.zip -Algorithm SHA256
```

Official release hashes in `dist/SHA256SUMS.txt`:
```text
19a40c894af553621e0d3da58a44e69efed9b5ba13333437faa799f08c3cb347  P2P_File_Engine_Windows_x64.zip
c28e006fc85f55691d521d069b8cdb5603c8befc4f7a0f4924627c60aedb8ba8  P2P_File_Engine_Source.zip
63b8254687e5902c1bd17a4ee788e44c58c22165fa5efc5529624d0c9c369625  P2P_File_Engine_Complete_Project_Guide.pdf
```
Both packages have been verified using automated regression test suites (`PASS=16 FAIL=0`) and isolated extracted runtime tests.
