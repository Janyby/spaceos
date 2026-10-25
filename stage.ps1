# Safe staged install of SpaceOS release binaries.
# 1. Builds into target-agent-staging (never touches the live release dir).
# 2. Refuses to replace any EXE that is currently locked by a running process.
# 3. Copies staging -> release only when every target is free.
# Never kills processes; use stop_spaceos.ps1 first (elevated if needed).
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\stage.ps1
param(
    [string]$StagingDir = "C:\spaceos\target-agent-staging",
    [string]$ReleaseDir = "C:\spaceos\target\release"
)
$ErrorActionPreference = "Stop"
Set-Location C:\spaceos

$exes = @(
    @("space-core", "space-core.exe"),
    @("browser-provider", "browser-provider.exe"),
    @("filesystem-provider", "filesystem-provider.exe"),
    @("space-bridge", "space-bridge.exe"),
    @("space-bridge", "spaceos-mcp.exe")
)

Write-Host "[1/3] Staging build..."
$pkgs = @("space-core", "browser-provider", "filesystem-provider", "space-bridge")
$buildArgs = @("build", "--release", "--target-dir", $StagingDir)
foreach ($p in $pkgs) { $buildArgs += @("-p", $p) }
& cargo $buildArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] staging build failed" -ForegroundColor Red
    exit 1
}
Write-Host "[PASS] staging build ok: $StagingDir" -ForegroundColor Green

Write-Host "[2/3] Checking release EXEs are not locked..."
$blocked = $false
foreach ($pair in $exes) {
    $dst = Join-Path $ReleaseDir $pair[1]
    if (-not (Test-Path $dst)) {
        Write-Host ("[INFO] new file (no existing to replace): " + $pair[1])
        continue
    }
    try {
        $fs = [System.IO.File]::Open($dst, "Open", "ReadWrite", "None")
        $fs.Close()
        Write-Host ("[PASS] free: " + $pair[1]) -ForegroundColor Green
    } catch {
        Write-Host ("[FAIL] locked by a running process, refusing: " + $dst) -ForegroundColor Red
        $blocked = $true
    }
}
if ($blocked) {
    Write-Host "[FAIL] stop the running stack first (stop_spaceos.ps1, elevated if the processes are elevated)" -ForegroundColor Red
    exit 1
}

Write-Host "[3/3] Installing staged binaries..."
foreach ($pair in $exes) {
    $src = Join-Path (Join-Path $StagingDir "release") $pair[1]
    $dst = Join-Path $ReleaseDir $pair[1]
    if (-not (Test-Path $src)) {
        Write-Host ("[FAIL] staging artifact missing: " + $src) -ForegroundColor Red
        exit 1
    }
    Copy-Item $src $dst -Force
    Write-Host ("[PASS] installed: " + $pair[1]) -ForegroundColor Green
}
Write-Host "[PASS] staged install complete. Verify with verify_spaceos.ps1" -ForegroundColor Green
exit 0
