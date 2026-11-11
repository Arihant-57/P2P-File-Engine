# ==============================================================================
# run_real_world_e2e_audit.ps1
# Independent Real-World User E2E Verification Suite (Section 3: Scenarios 1-8)
# ==============================================================================
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$PeerExe = Join-Path $RepoRoot "build\Release\peer.exe"
$RfeExe  = Join-Path $RepoRoot "build\Release\rfe.exe"

if (-not (Test-Path $PeerExe) -or -not (Test-Path $RfeExe)) {
    throw "Release binaries not found in build\Release\. Build Release first."
}

# Clean up any leftover peer.exe processes and E2E test directories
Get-Process peer -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 400

$TestRoot = Join-Path $RepoRoot "build\e2e_audit_workspace"
if (Test-Path $TestRoot) {
    Remove-Item -Recurse -Force $TestRoot
}
$InputDir    = Join-Path $TestRoot "inputs"
$DownloadDir = Join-Path $TestRoot "downloads"
$SpacedDir   = Join-Path $TestRoot "Folder With Spaces"
New-Item -ItemType Directory -Force -Path $InputDir, $DownloadDir, $SpacedDir | Out-Null

foreach ($id in @("E2E_A", "E2E_B", "E2E_C", "E2E_D")) {
    $st = Join-Path $RepoRoot "storage\peer_$id"
    if (Test-Path $st) { Remove-Item -Recurse -Force $st }
}

function Write-Step([string]$title) {
    Write-Host ""
    Write-Host "==========================================================================" -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan
}

function Assert-True([bool]$cond, [string]$msg) {
    if (-not $cond) {
        throw "ASSERTION FAILED: $msg"
    }
    Write-Host "  [PASS] $msg" -ForegroundColor Green
}

function Invoke-Rfe([string[]]$RfeArgs) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = (& $RfeExe @RfeArgs 2>&1) | Out-String
        return [PSCustomObject]@{
            ExitCode = $LASTEXITCODE
            Output   = $out
        }
    } finally {
        $ErrorActionPreference = $prev
    }
}

$PeerProcs = @{}
function Start-AuditPeer([string]$id, [int]$port) {
    $p = Start-Process -FilePath $PeerExe -ArgumentList @($id, "127.0.0.1", "$port", "p2p", "6305") -WorkingDirectory $RepoRoot -PassThru -WindowStyle Hidden
    $PeerProcs[$id] = $p
}

function Stop-AuditPeer([string]$id) {
    if ($PeerProcs.ContainsKey($id) -and $null -ne $PeerProcs[$id]) {
        Stop-Process -Id $PeerProcs[$id].Id -Force -ErrorAction SilentlyContinue
        $PeerProcs.Remove($id)
    }
}

try {
    # ==========================================================================
    # SCENARIO 1: Network Startup (4 Independent Peer Processes, No Coordinator)
    # ==========================================================================
    Write-Step "SCENARIO 1: 4-Peer Decentralized Network Startup & Discovery"
    Start-AuditPeer "E2E_A" 6301
    Start-AuditPeer "E2E_B" 6302
    Start-AuditPeer "E2E_C" 6303
    Start-AuditPeer "E2E_D" 6304

    # Wait up to 6 seconds for UDP discovery & TCP handshake convergence
    $converged = $false
    for ($i = 0; $i -lt 25; $i++) {
        Start-Sleep -Milliseconds 300
        $peersOut = (& $RfeExe peers --peer 127.0.0.1:6304 2>&1) | Out-String
        if ($peersOut -match "E2E_A" -and $peersOut -match "E2E_B" -and $peersOut -match "E2E_C" -and $peersOut -match "E2E_D" -and ($peersOut -split "ONLINE").Count -ge 5) {
            $converged = $true
            break
        }
    }
    Assert-True $converged "All 4 independent peers (E2E_A..E2E_D) discovered each other via UDP & TCP handshake without any coordinator"

    foreach ($id in @("E2E_A", "E2E_B", "E2E_C", "E2E_D")) {
        $dbFile = Join-Path $RepoRoot "storage\peer_$id\metadata.db"
        Assert-True ((Test-Path $dbFile) -and ((Get-Item $dbFile).Length -gt 0)) "Peer $id initialized isolated SQLite database ($dbFile)"
    }

    $statusOut = (& $RfeExe status --peer 127.0.0.1:6304 2>&1) | Out-String
    Assert-True ($statusOut -match "HEALTHY" -and $statusOut -match "4 ONLINE") "rfe status via Peer D reports HEALTHY cluster (4 ONLINE)"

    # ==========================================================================
    # SCENARIO 2: Upload via CLI Across All 9 Real-World File Types
    # ==========================================================================
    Write-Step "SCENARIO 2: Real-User CLI File Uploads (9 File Types)"

    $rng = [System.Random]::new(20261111)

    # 1. Small text file
    $f1 = Join-Path $InputDir "notes.txt"
    [System.IO.File]::WriteAllText($f1, ("Talent Hub & P2P Decentralized File Engine Test Notes`r`n" * 25))

    # 2. PDF file
    $f2 = Join-Path $InputDir "architecture_spec.pdf"
    $pdfBytes = New-Object byte[] 49152
    $rng.NextBytes($pdfBytes)
    $pdfHeader = [System.Text.Encoding]::ASCII.GetBytes("%PDF-1.7`n1 0 obj << /Type /Catalog >> endobj`n")
    [Array]::Copy($pdfHeader, 0, $pdfBytes, 0, $pdfHeader.Length)
    [System.IO.File]::WriteAllBytes($f2, $pdfBytes)

    # 3. PNG image file
    $f3 = Join-Path $InputDir "campus_diagram.png"
    $pngBytes = New-Object byte[] 65536
    $rng.NextBytes($pngBytes)
    $pngSig = [byte[]]@(0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A)
    [Array]::Copy($pngSig, 0, $pngBytes, 0, $pngSig.Length)
    [System.IO.File]::WriteAllBytes($f3, $pngBytes)

    # 4. Audio WAV file
    $f4 = Join-Path $InputDir "voice_sample.wav"
    $wavBytes = New-Object byte[] 153600
    $rng.NextBytes($wavBytes)
    $wavSig = [System.Text.Encoding]::ASCII.GetBytes("RIFF____WAVEfmt ")
    [Array]::Copy($wavSig, 0, $wavBytes, 0, $wavSig.Length)
    [System.IO.File]::WriteAllBytes($f4, $wavBytes)

    # 5. Video MP4 file
    $f5 = Join-Path $InputDir "demo_clip.mp4"
    $mp4Bytes = New-Object byte[] 360448
    $rng.NextBytes($mp4Bytes)
    $mp4Sig = [byte[]]@(0x00,0x00,0x00,0x18,0x66,0x74,0x79,0x70,0x69,0x73,0x6F,0x6D)
    [Array]::Copy($mp4Sig, 0, $mp4Bytes, 0, $mp4Sig.Length)
    [System.IO.File]::WriteAllBytes($f5, $mp4Bytes)

    # 6. Arbitrary binary file
    $f6 = Join-Path $InputDir "firmware_dump.bin"
    $binBytes = New-Object byte[] 204800
    $rng.NextBytes($binBytes)
    [System.IO.File]::WriteAllBytes($f6, $binBytes)

    # 7. Large multi-megabyte file (2.5 MB = 2,621,440 bytes -> 20 stripes)
    $f7 = Join-Path $InputDir "dataset_archive.bin"
    $largeBytes = New-Object byte[] 2621440
    $rng.NextBytes($largeBytes)
    [System.IO.File]::WriteAllBytes($f7, $largeBytes)

    # 8. Final-stripe padded file (131,079 bytes -> 1 full stripe + 7-byte padded stripe)
    $f8 = Join-Path $InputDir "odd_padded.bin"
    $oddBytes = New-Object byte[] 131079
    $rng.NextBytes($oddBytes)
    [System.IO.File]::WriteAllBytes($f8, $oddBytes)

    # 9. Empty file (0 bytes)
    $f9 = Join-Path $InputDir "empty_zero.txt"
    [System.IO.File]::WriteAllBytes($f9, [byte[]]@())

    $TestFiles = @($f1, $f2, $f3, $f4, $f5, $f6, $f7, $f8, $f9)
    $UploadedMap = @{}

    $idx = 0
    foreach ($tf in $TestFiles) {
        $idx++
        $entryPort = 6301 + ($idx % 4)
        $fname = Split-Path -Leaf $tf
        $origHash = (Get-FileHash -Path $tf -Algorithm SHA256).Hash.ToLower()
        $upOut = (& $RfeExe upload $tf --peer "127.0.0.1:$entryPort" 2>&1) | Out-String
        Assert-True ($LASTEXITCODE -eq 0 -and $upOut -match "DECENTRALIZED UPLOAD SUCCESS") "Uploaded $fname ($( (Get-Item $tf).Length ) bytes) via 127.0.0.1:$entryPort"
        if ($upOut -match "File ID:\s+(F_[0-9A-F]+)") {
            $fid = $Matches[1]
            $UploadedMap[$fname] = @{
                FileId   = $fid
                Path     = $tf
                Size     = (Get-Item $tf).Length
                Sha256   = $origHash
            }
        } else {
            throw "Could not parse File ID from upload output for $fname"
        }
    }

    # ==========================================================================
    # SCENARIO 3: Search via Another Peer (Peer D: 127.0.0.1:6304)
    # ==========================================================================
    Write-Step "SCENARIO 3: Catalogue Search via Peer D (By Filename & File ID)"
    $searchByName = (& $RfeExe search "demo_clip.mp4" --peer 127.0.0.1:6304 2>&1) | Out-String
    Assert-True ($LASTEXITCODE -eq 0 -and $searchByName -match $UploadedMap["demo_clip.mp4"].FileId -and $searchByName -match "AVAILABLE") "Search by filename 'demo_clip.mp4' on Peer D returned matching File ID & AVAILABLE status"

    $targetFid = $UploadedMap["dataset_archive.bin"].FileId
    $searchById = (& $RfeExe search $targetFid --peer 127.0.0.1:6304 2>&1) | Out-String
    Assert-True ($LASTEXITCODE -eq 0 -and $searchById -match "dataset_archive\.bin" -and $searchById -match "Reconstructable: YES") "Search by File ID '$targetFid' on Peer D returned full metadata & Reconstructable: YES"

    $infoOut = (& $RfeExe info $targetFid --peer 127.0.0.1:6304 2>&1) | Out-String
    Assert-True ($LASTEXITCODE -eq 0 -and $infoOut -match "FILE MANIFEST INFO" -and $infoOut -match "RS\(3,2\)") "rfe info '$targetFid' on Peer D displayed complete stripe/shard map"

    # ==========================================================================
    # SCENARIO 4: Download via Peer D & Verify Offline Uploader Resilience
    # ==========================================================================
    Write-Step "SCENARIO 4: Download All 9 Files via Peer D + Offline Uploader Test"
    foreach ($fname in $UploadedMap.Keys) {
        $meta = $UploadedMap[$fname]
        $outFile = Join-Path $DownloadDir $fname
        $dlOut = (& $RfeExe download $meta.FileId --peer 127.0.0.1:6304 --output $outFile 2>&1) | Out-String
        Assert-True ($LASTEXITCODE -eq 0 -and (Test-Path $outFile)) "Downloaded $fname ($($meta.FileId)) via Peer D"
        $dlHash = (Get-FileHash -Path $outFile -Algorithm SHA256).Hash.ToLower()
        $dlSize = (Get-Item $outFile).Length
        Assert-True ($dlSize -eq $meta.Size -and $dlHash -eq $meta.Sha256) "SHA-256 & byte size verified for $fname ($dlSize bytes, sha256=$($dlHash.Substring(0,16))...)"
    }

    # Now stop Peer B (6302, the original entry/uploader peer for demo_clip.mp4, notes.txt, and empty_zero.txt)
    # and immediately verify Peer D (6304) can still download demo_clip.mp4 while its uploader Peer B is OFFLINE!
    $bChunksDir = Join-Path $RepoRoot "storage\peer_E2E_B\chunks"
    $chunksBeforeStop = @(Get-ChildItem -Path $bChunksDir -Filter "*.chunk" -ErrorAction SilentlyContinue).Count
    Stop-AuditPeer "E2E_B"
    Start-Sleep -Milliseconds 300

    $offlineOutFile = Join-Path $DownloadDir "offline_uploader_demo_clip.mp4"
    $dlOfflineOut = (& $RfeExe download $UploadedMap["demo_clip.mp4"].FileId --peer 127.0.0.1:6304 --output $offlineOutFile 2>&1) | Out-String
    $offHash = (Get-FileHash -Path $offlineOutFile -Algorithm SHA256).Hash.ToLower()
    Assert-True ($LASTEXITCODE -eq 0 -and $offHash -eq $UploadedMap["demo_clip.mp4"].Sha256) "Downloaded & RS(3,2)-reconstructed demo_clip.mp4 via Peer D while original uploader Peer B was OFFLINE"

    # ==========================================================================
    # SCENARIO 5: Peer Failure & Autonomous Durability Recovery (No Manual Repair)
    # ==========================================================================
    Write-Step "SCENARIO 5: Peer B Failure Detection & Autonomous RS(3,2) Recovery"

    # Wait for heartbeat monitor to mark E2E_B OFFLINE and autonomous recovery to heal all 9 files (including 20-stripe 2.5MB file)
    $recovered = $false
    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep -Milliseconds 400
        $filesOut = (& $RfeExe files --peer 127.0.0.1:6304 2>&1) | Out-String
        $stOut    = (& $RfeExe status --peer 127.0.0.1:6304 2>&1) | Out-String
        if ($stOut -match "1 OFFLINE" -and $filesOut -notmatch "DEGRADED" -and $filesOut -match "AVAILABLE") {
            $recovered = $true
            break
        }
    }
    Assert-True $recovered "Peer B detected OFFLINE and autonomous recovery restored all 9 files to AVAILABLE (3/3 online shards/stripe) without manual repair command"

    # Verify downloaded 2.5 MB file after autonomous repair is byte-identical
    $postRepairOut = Join-Path $DownloadDir "post_repair_dataset.bin"
    $dlPostRepair = (& $RfeExe download $UploadedMap["dataset_archive.bin"].FileId --peer 127.0.0.1:6304 --output $postRepairOut 2>&1) | Out-String
    $prHash = (Get-FileHash -Path $postRepairOut -Algorithm SHA256).Hash.ToLower()
    Assert-True ($LASTEXITCODE -eq 0 -and $prHash -eq $UploadedMap["dataset_archive.bin"].Sha256) "2.5 MB dataset_archive.bin downloaded after autonomous repair matches original SHA-256"

    # ==========================================================================
    # SCENARIO 6: Peer Restart, Metadata Sync & Stale Shard Cleanup
    # ==========================================================================
    Write-Step "SCENARIO 6: Peer B Restart, Rejoin & Stale Shard Cleanup"
    Start-AuditPeer "E2E_B" 6302
    $rejoined = $false
    $chunksAfterRejoin = $chunksBeforeStop
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 400
        $stOut = (& $RfeExe status --peer 127.0.0.1:6304 2>&1) | Out-String
        $chunksAfterRejoin = @(Get-ChildItem -Path $bChunksDir -Filter "*.chunk" -ErrorAction SilentlyContinue).Count
        if ($stOut -match "HEALTHY" -and $chunksAfterRejoin -eq 0) {
            $rejoined = $true
            break
        }
    }
    Assert-True $rejoined "Restarted Peer B rejoined cluster (HEALTHY) and purged all $chunksBeforeStop superseded stale .chunk files"

    # ==========================================================================
    # SCENARIO 7: Corrupted Shard Rejection & Multiple Peer Failures
    # ==========================================================================
    Write-Step "SCENARIO 7: Corrupted Shard Rejection & Multi-Peer Failure Handling"

    # 7a. Corrupt one stored shard of firmware_dump.bin on disk and verify download rejects it and heals via RS(3,2)
    $fwFid = $UploadedMap["firmware_dump.bin"].FileId
    $corruptedFile = $null
    $origShardBytes = $null
    foreach ($id in @("E2E_A", "E2E_B", "E2E_C", "E2E_D")) {
        $cand = Join-Path $RepoRoot "storage\peer_$id\chunks\$fwFid-S000-D0.chunk"
        if (Test-Path $cand) {
            $corruptedFile = $cand
            $origShardBytes = [System.IO.File]::ReadAllBytes($cand)
            $badBytes = [byte[]]$origShardBytes.Clone()
            $badBytes[0] = $badBytes[0] -bxor 0xFF
            $badBytes[1] = $badBytes[1] -bxor 0xFF
            [System.IO.File]::WriteAllBytes($cand, $badBytes)
            break
        }
    }
    Assert-True ($null -ne $corruptedFile) "Injected 2-byte corruption into $corruptedFile"

    $corruptDlPath = Join-Path $DownloadDir "corrupt_test_firmware.bin"
    $corruptDlOut = (& $RfeExe download $fwFid --peer 127.0.0.1:6304 --output $corruptDlPath 2>&1) | Out-String
    $corruptDlHash = (Get-FileHash -Path $corruptDlPath -Algorithm SHA256).Hash.ToLower()
    Assert-True ($LASTEXITCODE -eq 0 -and $corruptDlOut -match "Corrupted Rejected:\s+[1-9]" -and $corruptDlHash -eq $UploadedMap["firmware_dump.bin"].Sha256) "Corrupted shard detected & rejected on download; RS(3,2) parity reconstructed byte-identical firmware_dump.bin"

    # Restore valid shard bytes before multi-failure test
    [System.IO.File]::WriteAllBytes($corruptedFile, $origShardBytes)

    # 7b. Stop TWO active storage peers (E2E_A and E2E_C) while E2E_B holds 0 shards -> only E2E_D has 1 shard/stripe (<2 required)
    Stop-AuditPeer "E2E_A"
    Stop-AuditPeer "E2E_C"
    Start-Sleep -Milliseconds 1800

    $unrecDlPath = Join-Path $DownloadDir "should_not_exist.bin"
    $unrecRes = Invoke-Rfe @("download", $fwFid, "--peer", "127.0.0.1:6304", "--output", $unrecDlPath)
    Assert-True ($unrecRes.ExitCode -ne 0 -and $unrecRes.Output -match "UNRECOVERABLE" -and -not (Test-Path $unrecDlPath)) "When 2 storage peers in a stripe are offline (<2 shards surviving), download fails with UNRECOVERABLE and writes no partial file"

    # 7c. With only 2 peers (E2E_B and E2E_D) online (<3 peers required for RS(3,2) upload), verify upload is rejected
    $underMinRes = Invoke-Rfe @("upload", $f1, "--peer", "127.0.0.1:6304")
    Assert-True ($underMinRes.ExitCode -ne 0 -and ($underMinRes.Output -match "need at least 3" -or $underMinRes.Output -match "RS\(3,2\) requires 3 distinct peers")) "Upload cleanly rejected when fewer than 3 online peers are available"

    # Restart E2E_A and E2E_C for Scenario 8 and wait until 4 ONLINE
    Start-AuditPeer "E2E_A" 6301
    Start-AuditPeer "E2E_C" 6303
    for ($i = 0; $i -lt 25; $i++) {
        Start-Sleep -Milliseconds 300
        $st8 = Invoke-Rfe @("status", "--peer", "127.0.0.1:6304")
        if ($st8.Output -match "4 ONLINE") { break }
    }

    # ==========================================================================
    # SCENARIO 8: CLI Usability (Spaces, Relative Paths, Invalid Args, Diagnostics)
    # ==========================================================================
    Write-Step "SCENARIO 8: CLI Usability & Edge-Case Validation"

    # 8a. Quoted path with spaces
    $spacedFile = Join-Path $SpacedDir "My Project Report Final.txt"
    [System.IO.File]::WriteAllText($spacedFile, "File inside a directory and filename with spaces!`r`n")
    $spUpRes = Invoke-Rfe @("upload", $spacedFile, "--peer", "127.0.0.1:6301")
    Assert-True ($spUpRes.ExitCode -eq 0 -and $spUpRes.Output -match "DECENTRALIZED UPLOAD SUCCESS") "Uploaded file from path containing spaces"

    $spacedOutFile = Join-Path $SpacedDir "Restored Report With Spaces.txt"
    $spDlRes = Invoke-Rfe @("download", "My Project Report Final.txt", "--peer", "127.0.0.1:6304", "--output", $spacedOutFile)
    Assert-True ($spDlRes.ExitCode -eq 0 -and (Test-Path $spacedOutFile)) "Downloaded file to output path containing spaces"

    # 8b. Relative path resolution from a non-root working directory
    Push-Location $SpacedDir
    try {
        $relDlRes = Invoke-Rfe @("download", "notes.txt", "--peer", "127.0.0.1:6304", "--output", ".\relative_notes.txt")
        Assert-True ($relDlRes.ExitCode -eq 0 -and (Test-Path (Join-Path $SpacedDir "relative_notes.txt"))) "Relative --output path resolved against user's current shell directory"
    } finally {
        Pop-Location
    }

    # 8c. Invalid command
    $badCmdRes = Invoke-Rfe @("invalid_command")
    Assert-True ($badCmdRes.ExitCode -ne 0 -and $badCmdRes.Output -match "unknown command") "Invalid command rejected with clear message and usage help"

    # 8d. Missing arguments
    $missUpRes = Invoke-Rfe @("upload")
    Assert-True ($missUpRes.ExitCode -ne 0 -and $missUpRes.Output -match "usage: rfe upload") "Missing upload argument reported cleanly"

    $missPeerRes = Invoke-Rfe @("peers", "--peer")
    Assert-True ($missPeerRes.ExitCode -ne 0 -and $missPeerRes.Output -match "missing <ip:port>") "Missing --peer value reported cleanly"

    # 8e. Invalid --peer port
    $badPeerPortRes = Invoke-Rfe @("peers", "--peer", "127.0.0.1:99999")
    Assert-True ($badPeerPortRes.ExitCode -ne 0 -and $badPeerPortRes.Output -match "invalid --peer endpoint") "Out-of-range --peer port rejected cleanly"

    # 8f. Non-existent File ID
    $noFileRes = Invoke-Rfe @("download", "F_DOESNOTEXIST", "--peer", "127.0.0.1:6301")
    Assert-True ($noFileRes.ExitCode -ne 0 -and $noFileRes.Output -match "not found") "Non-existent File ID download rejected cleanly"

    # 8g. Unreachable peer
    $unreachRes = Invoke-Rfe @("status", "--peer", "127.0.0.1:6399")
    Assert-True ($unreachRes.ExitCode -ne 0 -and $unreachRes.Output -match "OFFLINE or unreachable") "Unreachable peer endpoint reported cleanly without hang"

    # 8h. Transfers workload command
    $xfersRes = Invoke-Rfe @("transfers", "--peer", "127.0.0.1:6301")
    Assert-True ($xfersRes.ExitCode -eq 0 -and $xfersRes.Output -match "ACTIVE WORKLOAD & TRANSFERS") "rfe transfers command displayed live peer workload table"

    Write-Host ""
    Write-Host "==========================================================================" -ForegroundColor Green
    Write-Host "  ALL 8 REAL-WORLD END-TO-END SCENARIOS PASSED (100% VERIFIED)            " -ForegroundColor Green
    Write-Host "==========================================================================" -ForegroundColor Green
} finally {
    foreach ($id in @("E2E_A", "E2E_B", "E2E_C", "E2E_D")) {
        Stop-AuditPeer $id
    }
    Get-Process peer -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 300
    if (Test-Path $TestRoot) {
        Remove-Item -Recurse -Force $TestRoot -ErrorAction SilentlyContinue
    }
    foreach ($id in @("E2E_A", "E2E_B", "E2E_C", "E2E_D")) {
        $st = Join-Path $RepoRoot "storage\peer_$id"
        if (Test-Path $st) { Remove-Item -Recurse -Force $st -ErrorAction SilentlyContinue }
    }
}
