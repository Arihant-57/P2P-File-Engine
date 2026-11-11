<#
.SYNOPSIS
    Configures Windows Defender Firewall inbound rules for the Decentralized P2P Engine on a trusted LAN.
    Run this script in an Administrator PowerShell prompt on each participating laptop.
#>
param(
    [string]$PortRange = "5001-5010"
)

$ErrorActionPreference = "Stop"

Write-Host "[RFE-Firewall] Adding Windows Defender Firewall inbound rules for TCP & UDP ports $PortRange (Private/Domain LAN)..." -ForegroundColor Cyan

New-NetFirewallRule -DisplayName "RFE Decentralized P2P TCP ($PortRange)" `
    -Direction Inbound -Action Allow -Protocol TCP -LocalPort $PortRange `
    -Profile Private,Domain -ErrorAction SilentlyContinue | Out-Null

New-NetFirewallRule -DisplayName "RFE Decentralized P2P UDP Discovery ($PortRange)" `
    -Direction Inbound -Action Allow -Protocol UDP -LocalPort $PortRange `
    -Profile Private,Domain -ErrorAction SilentlyContinue | Out-Null

Write-Host "[RFE-Firewall] Firewall rules configured for Private/Domain LAN profiles on ports $PortRange." -ForegroundColor Green
