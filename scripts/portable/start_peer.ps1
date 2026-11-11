# ==============================================================================
# start_peer.ps1 - Launch Decentralized Peer Node (Portable Windows x64)
# ==============================================================================
param(
    [string]$Config = "",
    [string]$PeerId = "",
    [switch]$Background = $false
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (Test-Path (Join-Path $ScriptDir "peer.exe")) {
    $RootDir = $ScriptDir
    $PeerExe = Join-Path $ScriptDir "peer.exe"
} elseif (Test-Path (Join-Path $ScriptDir "..\..\build\Release\peer.exe")) {
    $RootDir = (Resolve-Path (Join-Path $ScriptDir "..\..")).Path
    $PeerExe = Join-Path $RootDir "build\Release\peer.exe"
} elseif (Test-Path (Join-Path $ScriptDir "..\build\Release\peer.exe")) {
    $RootDir = (Resolve-Path (Join-Path $ScriptDir "..")).Path
    $PeerExe = Join-Path $RootDir "build\Release\peer.exe"
} else {
    throw "peer.exe not found. Ensure you are running from the portable release folder or have built the project."
}

if ([string]::IsNullOrWhiteSpace($Config)) {
    if (-not [string]::IsNullOrWhiteSpace($PeerId)) {
        $Config = Join-Path $RootDir "configs\peer_$PeerId.json"
    } elseif (Test-Path (Join-Path $RootDir "configs\active_peer.json")) {
        $Config = Join-Path $RootDir "configs\active_peer.json"
    } else {
        $Config = Join-Path $RootDir "configs\peer_A.json"
    }
}

if (-not (Test-Path $Config)) {
    throw "Configuration file not found: $Config. Run .\setup_peer.ps1 first."
}

Push-Location $RootDir
try {
    if ($Background) {
        $proc = Start-Process -FilePath $PeerExe -ArgumentList @("--config", "`"$Config`"") -WorkingDirectory $RootDir -PassThru -WindowStyle Hidden
        Write-Host "[OK] Started peer.exe in background (PID=$($proc.Id)) using config: $Config" -ForegroundColor Green
    } else {
        Write-Host "[INFO] Launching peer.exe with config: $Config (Press Ctrl+C to stop)..." -ForegroundColor Cyan
        & $PeerExe --config $Config
    }
} finally {
    Pop-Location
}
