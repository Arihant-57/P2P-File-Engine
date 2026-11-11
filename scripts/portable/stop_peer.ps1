# ==============================================================================
# stop_peer.ps1 - Cleanly Stop Running Peer Processes (Portable Windows x64)
# ==============================================================================
param(
    [string]$PeerId = ""
)

$ErrorActionPreference = "SilentlyContinue"
$procs = Get-CimInstance Win32_Process -Filter "Name = 'peer.exe'"

if (-not $procs) {
    Write-Host "[INFO] No running peer.exe processes found." -ForegroundColor Yellow
    exit 0
}

$stopped = 0
foreach ($p in $procs) {
    if ([string]::IsNullOrWhiteSpace($PeerId) -or
        ($p.CommandLine -match "peer_$PeerId\.json") -or
        ($p.CommandLine -match "\s+$PeerId\s+")) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "[OK] Stopped peer.exe (PID=$($p.ProcessId)): $($p.CommandLine)" -ForegroundColor Green
        $stopped++
    }
}

if ($stopped -eq 0) {
    Write-Host "[INFO] No peer.exe process matched PeerId='$PeerId'." -ForegroundColor Yellow
}
