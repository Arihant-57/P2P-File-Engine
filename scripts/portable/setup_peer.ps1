# ==============================================================================
# setup_peer.ps1 - Interactive Peer Configuration Wizard (Portable Windows x64)
# ==============================================================================
param(
    [string]$PeerId = "",
    [string]$Address = "",
    [int]$Port = 0,
    [int]$DiscoveryPort = 0,
    [string]$StorageDir = "",
    [int]$MaxStorageMB = -1,
    [string]$SeedPeer = "",
    [int]$UIPort = 0,
    [switch]$NoUI = $false,
    [switch]$UILan = $false,
    [string]$UIToken = "",
    [switch]$NonInteractive = $false
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ((Split-Path -Leaf $ScriptDir) -eq "portable" -or (Split-Path -Leaf $ScriptDir) -eq "scripts") {
    $RootDir = Split-Path -Parent $ScriptDir
    if ((Split-Path -Leaf $RootDir) -eq "scripts") {
        $RootDir = Split-Path -Parent $RootDir
    }
} else {
    $RootDir = $ScriptDir
}

Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "  P2P DECENTRALIZED FILE TRANSFER & RECOVERY ENGINE - PEER SETUP WIZARD   " -ForegroundColor Cyan
Write-Host "==========================================================================" -ForegroundColor Cyan

# Detect local LAN IPv4 address as a helpful default
$DetectedIP = "127.0.0.1"
try {
    $NetIP = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.IPAddress -ne "127.0.0.1" -and $_.IPAddress -notlike "169.254.*" -and $_.PrefixOrigin -ne "WellKnown" } |
        Select-Object -First 1
    if ($NetIP -and $NetIP.IPAddress) {
        $DetectedIP = $NetIP.IPAddress
    }
} catch {}

if (-not $NonInteractive) {
    if ([string]::IsNullOrWhiteSpace($PeerId)) {
        $inputVal = Read-Host "1. Enter Peer ID (A, B, C, D or custom) [default: A]"
        $PeerId = if ([string]::IsNullOrWhiteSpace($inputVal)) { "A" } else { $inputVal.Trim() }
    }
    if ([string]::IsNullOrWhiteSpace($Address)) {
        $inputVal = Read-Host "2. Enter Bind/Advertise IPv4 Address [default: $DetectedIP]"
        $Address = if ([string]::IsNullOrWhiteSpace($inputVal)) { $DetectedIP } else { $inputVal.Trim() }
    }
    if ($Port -le 0) {
        $defaultPort = switch ($PeerId.ToUpper()) {
            "A" { 5701 }
            "B" { 5702 }
            "C" { 5703 }
            "D" { 5704 }
            default { 5701 }
        }
        $inputVal = Read-Host "3. Enter TCP Listen Port (1..65535) [default: $defaultPort]"
        $Port = if ([string]::IsNullOrWhiteSpace($inputVal)) { $defaultPort } else { [int]$inputVal }
    }
    if ($DiscoveryPort -le 0) {
        $inputVal = Read-Host "4. Enter UDP Discovery Port (1..65535) [default: 5705]"
        $DiscoveryPort = if ([string]::IsNullOrWhiteSpace($inputVal)) { 5705 } else { [int]$inputVal }
    }
    if ([string]::IsNullOrWhiteSpace($StorageDir)) {
        $defaultStorage = "storage/peer_$PeerId"
        $inputVal = Read-Host "5. Enter Peer Storage Directory [default: $defaultStorage]"
        $StorageDir = if ([string]::IsNullOrWhiteSpace($inputVal)) { $defaultStorage } else { $inputVal.Trim() }
    }
    if ($MaxStorageMB -lt 0) {
        $inputVal = Read-Host "6. Enter Max Storage Contribution in MB (0 = unlimited) [default: 0]"
        $MaxStorageMB = if ([string]::IsNullOrWhiteSpace($inputVal)) { 0 } else { [int]$inputVal }
    }
    if ([string]::IsNullOrWhiteSpace($SeedPeer)) {
        $inputVal = Read-Host "7. Enter Optional Seed Peer <ip:port> (press Enter to skip)"
        $SeedPeer = if ([string]::IsNullOrWhiteSpace($inputVal)) { "" } else { $inputVal.Trim() }
    }
    if ($UIPort -le 0 -and (-not $NoUI)) {
        $defaultUIPort = 8080 + ($Port - 5701)
        if ($defaultUIPort -lt 8080 -or $defaultUIPort -gt 8999) { $defaultUIPort = 8080 + ($Port % 100) }
        $inputVal = Read-Host "8. Enter Web Dashboard Port [default: $defaultUIPort]"
        $UIPort = if ([string]::IsNullOrWhiteSpace($inputVal)) { $defaultUIPort } else { [int]$inputVal }
    }
    if (-not $UILan -and (-not $NoUI)) {
        $inputVal = Read-Host "9. Allow LAN access to Web Dashboard? (y/N) [default: N]"
        if ($inputVal -match '^[Yy]') {
            $UILan = $true
            if ([string]::IsNullOrWhiteSpace($UIToken)) {
                $tokenInput = Read-Host "10. Enter optional Web Dashboard auth token (press Enter for none)"
                $UIToken = if ([string]::IsNullOrWhiteSpace($tokenInput)) { "" } else { $tokenInput.Trim() }
            }
        }
    }
} else {
    if ([string]::IsNullOrWhiteSpace($PeerId)) { $PeerId = "A" }
    if ([string]::IsNullOrWhiteSpace($Address)) { $Address = $DetectedIP }
    if ($Port -le 0) { $Port = 5701 }
    if ($DiscoveryPort -le 0) { $DiscoveryPort = 5705 }
    if ([string]::IsNullOrWhiteSpace($StorageDir)) { $StorageDir = "storage/peer_$PeerId" }
    if ($MaxStorageMB -lt 0) { $MaxStorageMB = 0 }
    if ($UIPort -le 0) {
        $UIPort = 8080 + ($Port - 5701)
        if ($UIPort -lt 8080 -or $UIPort -gt 8999) { $UIPort = 8080 + ($Port % 100) }
    }
}

if ($PeerId -notmatch '^[A-Za-z0-9_-]{1,32}$') {
    throw "Invalid Peer ID '$PeerId'. Use 1..32 alphanumeric, underscore, or hyphen characters."
}
if ($Port -lt 1 -or $Port -gt 65535) {
    throw "Invalid TCP Port '$Port'. Must be between 1 and 65535."
}
if ($DiscoveryPort -lt 1 -or $DiscoveryPort -gt 65535) {
    throw "Invalid UDP Discovery Port '$DiscoveryPort'. Must be between 1 and 65535."
}
if ($UIPort -lt 0 -or $UIPort -gt 65535) {
    throw "Invalid UI Port '$UIPort'. Must be between 0 and 65535."
}

$ConfigsDir = Join-Path $RootDir "configs"
New-Item -ItemType Directory -Force -Path $ConfigsDir | Out-Null

$SeedList = @()
if (-not [string]::IsNullOrWhiteSpace($SeedPeer)) {
    $SeedList = @($SeedPeer.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
}

$ConfigObj = [ordered]@{
    peer_id        = $PeerId
    address        = $Address
    port           = $Port
    mode           = "p2p"
    discovery_port = $DiscoveryPort
    storage_dir    = ($StorageDir -replace '\\', '/')
    max_storage_mb = $MaxStorageMB
    seed_peers     = $SeedList
    ui_enabled     = (-not $NoUI)
    ui_port        = $UIPort
    ui_lan         = [bool]$UILan
    ui_token       = $UIToken
}

$PeerConfigPath = Join-Path $ConfigsDir "peer_$PeerId.json"
$ActiveConfigPath = Join-Path $ConfigsDir "active_peer.json"

$Utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$JsonText = $ConfigObj | ConvertTo-Json -Depth 4
[System.IO.File]::WriteAllText($PeerConfigPath, $JsonText, $Utf8NoBom)
[System.IO.File]::WriteAllText($ActiveConfigPath, $JsonText, $Utf8NoBom)

# Also save default CLI target session so rfe.exe connects to this peer automatically
$MetadataDir = Join-Path $RootDir "metadata"
New-Item -ItemType Directory -Force -Path $MetadataDir | Out-Null
$SessionObj = [ordered]@{
    address = if ($Address -eq "0.0.0.0") { "127.0.0.1" } else { $Address }
    port    = $Port
}
[System.IO.File]::WriteAllText((Join-Path $MetadataDir "session.json"), ($SessionObj | ConvertTo-Json), $Utf8NoBom)

Write-Host ""
Write-Host "[OK] Peer configuration written to:" -ForegroundColor Green
Write-Host "     - $PeerConfigPath"
Write-Host "     - $ActiveConfigPath"
Write-Host "[OK] CLI session configured for $($SessionObj.address):$Port" -ForegroundColor Green
Write-Host "Next step: Run .\start_peer.ps1 to launch Peer $PeerId." -ForegroundColor Yellow
