# verify_source_zip.ps1
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$ZipPath = Join-Path $RepoRoot "dist\P2P_File_Engine_Source.zip"
$TestDir = Join-Path $RepoRoot "build\test_source_zip"

if (Test-Path $TestDir) { Remove-Item -Recurse -Force $TestDir }
New-Item -ItemType Directory -Force -Path $TestDir | Out-Null

try {
    Write-Host "Extracting $ZipPath..." -ForegroundColor Cyan
    Expand-Archive -Path $ZipPath -DestinationPath $TestDir -Force

    Push-Location $TestDir
    try {
        Write-Host "Configuring CMake from extracted Source ZIP..." -ForegroundColor Cyan
        & cmake -S . -B build -DOPENSSL_ROOT_DIR="$RepoRoot\third_party\openssl"
        if ($LASTEXITCODE -ne 0) { throw "CMake configure failed on extracted source ZIP" }

        Write-Host "Building peer executable from extracted Source ZIP..." -ForegroundColor Cyan
        & cmake --build build --config Release --target peer
        if ($LASTEXITCODE -ne 0) { throw "CMake build failed on extracted source ZIP" }

        $peerBin = Join-Path $TestDir "build\Release\peer.exe"
        if (-not (Test-Path $peerBin)) { throw "peer.exe was not created in extracted build" }

        Write-Host "SUCCESS: Extracted source ZIP is independently buildable! (peer.exe size: $((Get-Item $peerBin).Length) bytes)" -ForegroundColor Green
    } finally {
        Pop-Location
    }
} finally {
    Remove-Item -Recurse -Force $TestDir -ErrorAction SilentlyContinue
}
