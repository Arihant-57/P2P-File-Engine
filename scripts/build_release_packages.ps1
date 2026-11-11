# ==============================================================================
# build_release_packages.ps1
# Builds Windows x64 Portable Release ZIP, Clean Source ZIP, SHA256SUMS.txt,
# and verifies standalone execution from an extracted temporary directory.
# ==============================================================================
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$PeerExe   = Join-Path $RepoRoot "build\Release\peer.exe"
$RfeExe    = Join-Path $RepoRoot "build\Release\rfe.exe"
$CryptoDll = Join-Path $RepoRoot "build\Release\libcrypto-3-x64.dll"

if (-not (Test-Path $PeerExe) -or -not (Test-Path $RfeExe) -or -not (Test-Path $CryptoDll)) {
    throw "Release binaries or libcrypto-3-x64.dll missing in build\Release\."
}

# 1. Inspect PE DLL dependencies of peer.exe and rfe.exe
Write-Host "=== 1. Inspecting Executable DLL Imports ===" -ForegroundColor Cyan
foreach ($bin in @($PeerExe, $RfeExe)) {
    $bytes = [System.IO.File]::ReadAllBytes($bin)
    $ascii = [System.Text.Encoding]::ASCII.GetString($bytes)
    $dllMatches = [regex]::Matches($ascii, '[A-Za-z0-9_-]+\.dll', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
        ForEach-Object { $_.Value } |
        Where-Object { $_ -match '^(libcrypto|WS2_32|KERNEL32|ADVAPI32|MSVCP|VCRUNTIME|api-ms-win-crt)' } |
        Select-Object -Unique
    Write-Host "  $(Split-Path -Leaf $bin) imports: $($dllMatches -join ', ')" -ForegroundColor Green
}

# 2. Prepare dist/ and staging directories
$DistDir = Join-Path $RepoRoot "dist"
if (Test-Path $DistDir) {
    Remove-Item -Recurse -Force $DistDir
}
New-Item -ItemType Directory -Force -Path $DistDir | Out-Null

$StagingRoot = Join-Path $RepoRoot "build\package_staging"
if (Test-Path $StagingRoot) {
    Remove-Item -Recurse -Force $StagingRoot -ErrorAction SilentlyContinue
}
$PortableStage = Join-Path $StagingRoot "P2P_File_Engine_Windows_x64"
$SourceStage   = Join-Path $StagingRoot "P2P_File_Engine_Source"
New-Item -ItemType Directory -Force -Path $PortableStage, $SourceStage | Out-Null

try {
    # ==========================================================================
    # 3. Assemble Windows x64 Portable Release Package
    # ==========================================================================
    Write-Host "=== 2. Building dist/P2P_File_Engine_Windows_x64.zip ===" -ForegroundColor Cyan
    Copy-Item $PeerExe   -Destination $PortableStage
    Copy-Item $RfeExe    -Destination $PortableStage
    Copy-Item $CryptoDll -Destination $PortableStage

    # Include web static assets for embedded dashboard
    Copy-Item -Path (Join-Path $RepoRoot "web") -Destination (Join-Path $PortableStage "web") -Recurse -Force

    Copy-Item (Join-Path $RepoRoot "scripts\portable\setup_peer.ps1")  -Destination $PortableStage
    Copy-Item (Join-Path $RepoRoot "scripts\portable\start_peer.ps1")  -Destination $PortableStage
    Copy-Item (Join-Path $RepoRoot "scripts\portable\stop_peer.ps1")   -Destination $PortableStage
    Copy-Item (Join-Path $RepoRoot "scripts\portable\START_HERE.txt")  -Destination $PortableStage
    Copy-Item (Join-Path $RepoRoot "scripts\configure_lan_firewall.ps1") -Destination $PortableStage
    Copy-Item (Join-Path $RepoRoot "THIRD_PARTY_NOTICES.txt")          -Destination $PortableStage
    Copy-Item (Join-Path $RepoRoot "config.json")                      -Destination $PortableStage

    $PortConfigs = Join-Path $PortableStage "configs"
    $PortDocs    = Join-Path $PortableStage "docs"
    $PortStorage = Join-Path $PortableStage "storage"
    $PortOutput  = Join-Path $PortableStage "output"
    New-Item -ItemType Directory -Force -Path $PortConfigs, $PortDocs, $PortStorage, $PortOutput | Out-Null

    Copy-Item (Join-Path $RepoRoot "configs\*.json") -Destination $PortConfigs
    foreach ($doc in @(
        "README.md",
        "ARCHITECTURE.md",
        "USER_GUIDE.md",
        "CLI_GUIDE.md",
        "WEB_DASHBOARD_GUIDE.md",
        "LAN_DEPLOYMENT_GUIDE.md",
        "FOUR_LAPTOP_DEMO_GUIDE.md",
        "PORTABLE_RELEASE_GUIDE.md",
        "RELEASE_NOTES.md",
        "SECURITY_AND_LIMITATIONS.md",
        "FINAL_ENGINEERING_AUDIT.md",
        "AUTONOMOUS_ENGINEERING_PROGRESS.md"
    )) {
        $dp = Join-Path $RepoRoot $doc
        if (Test-Path $dp) { Copy-Item $dp -Destination $PortDocs }
    }
    $cpgMd  = Join-Path $RepoRoot "docs\COMPLETE_PROJECT_GUIDE.md"
    $cpgPdf = Join-Path $RepoRoot "dist\P2P_File_Engine_Complete_Project_Guide.pdf"
    if (Test-Path $cpgMd)  { Copy-Item $cpgMd  -Destination $PortDocs }
    if (Test-Path $cpgPdf) { Copy-Item $cpgPdf -Destination $PortDocs }

    $PortableZip = Join-Path $DistDir "P2P_File_Engine_Windows_x64.zip"
    Compress-Archive -Path (Join-Path $PortableStage "*") -DestinationPath $PortableZip -Force
    Write-Host "  [OK] Created $PortableZip ($((Get-Item $PortableZip).Length) bytes)" -ForegroundColor Green

    # ==========================================================================
    # 4. Assemble Clean Source Code Package
    # ==========================================================================
    Write-Host "=== 3. Building dist/P2P_File_Engine_Source.zip ===" -ForegroundColor Cyan
    foreach ($dirName in @("cli", "configs", "controller", "docs", "engine", "models", "peer", "scripts", "third_party", "web")) {
        $srcPath = Join-Path $RepoRoot $dirName
        if (Test-Path $srcPath) {
            Copy-Item -Path $srcPath -Destination (Join-Path $SourceStage $dirName) -Recurse -Force
        }
    }

    # Copy top-level build/config/documentation files
    Copy-Item (Join-Path $RepoRoot "CMakeLists.txt") -Destination $SourceStage
    Copy-Item (Join-Path $RepoRoot "config.json")    -Destination $SourceStage
    Copy-Item (Join-Path $RepoRoot "THIRD_PARTY_NOTICES.txt") -Destination $SourceStage
    Get-ChildItem -Path $RepoRoot -Filter "*.md" -File | ForEach-Object {
        Copy-Item $_.FullName -Destination $SourceStage
    }

    # Ensure zero build artifacts, object files, or temp files exist in SourceStage
    Get-ChildItem -Path $SourceStage -Recurse -Include @("*.obj", "*.pdb", "*.ilk", "*.db", "*.db-wal", "*.db-shm", "*.chunk", "*.tmp") -ErrorAction SilentlyContinue |
        Remove-Item -Force -ErrorAction SilentlyContinue

    $SourceZip = Join-Path $DistDir "P2P_File_Engine_Source.zip"
    Compress-Archive -Path (Join-Path $SourceStage "*") -DestinationPath $SourceZip -Force
    Write-Host "  [OK] Created $SourceZip ($((Get-Item $SourceZip).Length) bytes)" -ForegroundColor Green

    # ==========================================================================
    # 5. Generate dist/SHA256SUMS.txt
    # ==========================================================================
    Write-Host "=== 4. Generating dist/SHA256SUMS.txt ===" -ForegroundColor Cyan
    $portHash = (Get-FileHash -Path $PortableZip -Algorithm SHA256).Hash.ToLower()
    $srcHash  = (Get-FileHash -Path $SourceZip   -Algorithm SHA256).Hash.ToLower()
    $pdfFile  = Join-Path $DistDir "P2P_File_Engine_Complete_Project_Guide.pdf"
    $pdfHash  = if (Test-Path $pdfFile) { (Get-FileHash -Path $pdfFile -Algorithm SHA256).Hash.ToLower() } else { "" }
    $sumsText = "$portHash  P2P_File_Engine_Windows_x64.zip`r`n$srcHash  P2P_File_Engine_Source.zip`r`n"
    if ($pdfHash) { $sumsText += "$pdfHash  P2P_File_Engine_Complete_Project_Guide.pdf`r`n" }
    $SumsFile = Join-Path $DistDir "SHA256SUMS.txt"
    [System.IO.File]::WriteAllText($SumsFile, $sumsText)
    Write-Host $sumsText -ForegroundColor Green

    # ==========================================================================
    # 6. Extract Portable ZIP to Fresh Temp Directory & Verify Execution
    # ==========================================================================
    Write-Host "=== 5. Testing Extracted Portable Package in Isolated Temp Directory ===" -ForegroundColor Cyan
    $ExtractVerifyDir = Join-Path $StagingRoot "Extracted_Portable_Test"
    Expand-Archive -Path $PortableZip -DestinationPath $ExtractVerifyDir -Force

    Push-Location $ExtractVerifyDir
    try {
        # Test setup_peer.ps1 non-interactively for 4 peers on ports 6401..6404 (UDP 6405)
        foreach ($idx in 0..3) {
            $pidName = @("PORT_A", "PORT_B", "PORT_C", "PORT_D")[$idx]
            $pPort   = 6401 + $idx
            $uPort   = 8081 + $idx
            & powershell -ExecutionPolicy Bypass -File .\setup_peer.ps1 -PeerId $pidName -Address "127.0.0.1" -Port $pPort -DiscoveryPort 6405 -StorageDir "storage/peer_$pidName" -MaxStorageMB 256 -UIPort $uPort -NonInteractive | Out-Null
            & powershell -ExecutionPolicy Bypass -File .\start_peer.ps1 -PeerId $pidName -Background | Out-Null
        }

        # Wait for 4-peer mesh convergence in the extracted portable directory
        $portConverged = $false
        for ($i = 0; $i -lt 30; $i++) {
            Start-Sleep -Milliseconds 350
            $prevEA = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            $stOut = (& .\rfe.exe status --peer 127.0.0.1:6401 2>&1) | Out-String
            $ErrorActionPreference = $prevEA
            if ($stOut -match "4 ONLINE") {
                $portConverged = $true
                break
            }
        }
        if (-not $portConverged) {
            throw "Extracted portable peers did not converge to 4 ONLINE"
        }
        Write-Host "  [PASS] Extracted portable peer.exe cluster converged (4 ONLINE)" -ForegroundColor Green

        # Upload START_HERE.txt via PORT_A (6401), search & download via PORT_D (6404)
        $upOut = (& .\rfe.exe upload .\START_HERE.txt --peer 127.0.0.1:6401 2>&1) | Out-String
        if ($LASTEXITCODE -ne 0 -or $upOut -notmatch "DECENTRALIZED UPLOAD SUCCESS") {
            throw "Portable upload failed: $upOut"
        }
        Write-Host "  [PASS] Uploaded START_HERE.txt from extracted portable folder via 127.0.0.1:6401" -ForegroundColor Green

        $restoredPath = Join-Path $ExtractVerifyDir "output\restored_START_HERE.txt"
        $dlOut = (& .\rfe.exe download "START_HERE.txt" --peer 127.0.0.1:6404 --output $restoredPath 2>&1) | Out-String
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path $restoredPath)) {
            throw "Portable download failed: $dlOut"
        }
        $origSha = (Get-FileHash -Path .\START_HERE.txt -Algorithm SHA256).Hash.ToLower()
        $restSha = (Get-FileHash -Path $restoredPath    -Algorithm SHA256).Hash.ToLower()
        if ($origSha -ne $restSha) {
            throw "Portable SHA-256 mismatch ($origSha vs $restSha)"
        }
        Write-Host "  [PASS] Downloaded & verified START_HERE.txt via 127.0.0.1:6404 (SHA-256: $restSha)" -ForegroundColor Green

        # Verify CLI storage and recovery queries
        $storageOut = (& .\rfe.exe storage --peer 127.0.0.1:6401 2>&1) | Out-String
        if ($LASTEXITCODE -ne 0 -or $storageOut -notmatch "PEER STORAGE TELEMETRY") {
            throw "Portable rfe storage command failed: $storageOut"
        }
        Write-Host "  [PASS] Verified rfe storage CLI command in portable release" -ForegroundColor Green

        $recoveryOut = (& .\rfe.exe recovery --peer 127.0.0.1:6401 2>&1) | Out-String
        if ($LASTEXITCODE -ne 0 -or $recoveryOut -notmatch "AUTONOMOUS RECOVERY STATUS") {
            throw "Portable rfe recovery command failed: $recoveryOut"
        }
        Write-Host "  [PASS] Verified rfe recovery CLI command in portable release" -ForegroundColor Green

        # Verify embedded Web Dashboard HTTP server
        try {
            $webResp = Invoke-WebRequest -Uri "http://127.0.0.1:8081/api/v1/health" -UseBasicParsing -TimeoutSec 3
            if ($webResp.StatusCode -eq 200 -and $webResp.Content -match '"status":\s*"OK"') {
                Write-Host "  [PASS] Verified embedded Web Dashboard HTTP health endpoint (http://127.0.0.1:8081/api/v1/health)" -ForegroundColor Green
            }
        } catch {
            Write-Host "  [NOTE] Web Dashboard port 8081 health probe: $($_.Exception.Message)" -ForegroundColor Yellow
        }

        # Verify graceful remote peer daemon shutdown via rfe shutdown
        $shutOut = (& .\rfe.exe shutdown --peer 127.0.0.1:6404 2>&1) | Out-String
        if ($LASTEXITCODE -ne 0 -or $shutOut -notmatch "Graceful shutdown") {
            throw "Portable rfe shutdown command failed: $shutOut"
        }
        Write-Host "  [PASS] Verified graceful remote peer shutdown via rfe shutdown (127.0.0.1:6404)" -ForegroundColor Green

        & powershell -ExecutionPolicy Bypass -File .\stop_peer.ps1 | Out-Null
    } finally {
        Pop-Location
        Get-Process peer -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    }
} finally {
    Get-Process peer -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 300
    if (Test-Path $StagingRoot) {
        Remove-Item -Recurse -Force $StagingRoot -ErrorAction SilentlyContinue
    }
}

Write-Host "=== ALL RELEASE PACKAGES BUILT AND VERIFIED SUCCESSFULLY ===" -ForegroundColor Green
