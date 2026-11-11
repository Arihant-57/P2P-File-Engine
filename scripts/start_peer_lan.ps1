<#
.SYNOPSIS
    Starts a Decentralized Peer node (peer.exe) on a Windows laptop for LAN operation.

.EXAMPLE
    # Start using a per-peer JSON configuration file:
    .\scripts\start_peer_lan.ps1 -ConfigFile configs\peer_A.json

.EXAMPLE
    # Start with explicit Peer ID, LAN IP, TCP Port, UDP Discovery Port, and Storage Quota:
    .\scripts\start_peer_lan.ps1 -PeerId A -Address 192.168.1.101 -Port 5001 -DiscoveryPort 5005 -MaxStorageMB 10240 -Seeds @("192.168.1.102:5002","192.168.1.103:5003","192.168.1.104:5004")
#>
param(
    [string]$ConfigFile = "",
    [string]$PeerId = "A",
    [string]$Address = "",
    [int]$Port = 5001,
    [int]$DiscoveryPort = 5005,
    [int]$MaxStorageMB = 10240,
    [string[]]$Seeds = @()
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$PeerExe = Join-Path $ProjectRoot "build\Release\peer.exe"
if (-not (Test-Path $PeerExe)) {
    Write-Error "peer.exe not found at $PeerExe. Run 'cmake --build build --config Release' first."
    exit 1
}

if ($ConfigFile -ne "") {
    if (-not (Test-Path $ConfigFile)) {
        Write-Error "Configuration file not found: $ConfigFile"
        exit 1
    }
    Write-Host "[RFE-LAN] Starting decentralized peer from config: $ConfigFile" -ForegroundColor Cyan
    & $PeerExe --config $ConfigFile
    exit $LASTEXITCODE
}

if ($Address -eq "") {
    $detected = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.254.*" -and $_.PrefixOrigin -ne "WellKnown" } |
        Select-Object -First 1
    if ($null -ne $detected) {
        $Address = $detected.IPAddress
    } else {
        $Address = "127.0.0.1"
    }
}

$argList = @($PeerId, $Address, "$Port", "p2p", "$DiscoveryPort", "--max-storage-mb", "$MaxStorageMB")
foreach ($s in $Seeds) {
    if ($s -ne "") {
        $argList += "--seed"
        $argList += $s
    }
}

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  STARTING DECENTRALIZED PEER NODE" -ForegroundColor Cyan
Write-Host "  Peer ID:        $PeerId"
Write-Host "  LAN Address:    $Address`:$Port"
Write-Host "  UDP Discovery:  Port $DiscoveryPort"
Write-Host "  Storage Quota:  $MaxStorageMB MB"
Write-Host "  Storage Dir:    storage\peer_$PeerId\chunks"
Write-Host "  SQLite DB:      storage\peer_$PeerId\metadata.db"
Write-Host "=================================================================" -ForegroundColor Cyan

& $PeerExe @argList
