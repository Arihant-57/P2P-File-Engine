<#
.SYNOPSIS
    Stops running peer.exe processes on the local Windows machine.
#>
param(
    [switch]$Force
)

$procs = Get-Process -Name "peer" -ErrorAction SilentlyContinue
if ($null -eq $procs) {
    Write-Host "[RFE-LAN] No running peer.exe processes found on this machine." -ForegroundColor Yellow
    exit 0
}

foreach ($p in $procs) {
    Write-Host "[RFE-LAN] Stopping peer.exe (PID: $($p.Id))..." -ForegroundColor Cyan
    Stop-Process -Id $p.Id -Force
}
Write-Host "[RFE-LAN] All local peer.exe processes stopped." -ForegroundColor Green
