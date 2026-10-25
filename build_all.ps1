# Build all SpaceOS release binaries.
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\build_all.ps1
$ErrorActionPreference = "Stop"
Set-Location C:\spaceos
$pkgs = @("space-core", "browser-provider", "filesystem-provider", "space-bridge")
$failed = $false
foreach ($p in $pkgs) {
    Write-Host ("Building release: " + $p)
    cargo build --release -p $p
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("[FAIL] build failed: " + $p) -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host ("[PASS] built: " + $p) -ForegroundColor Green
    }
}
$exes = @("space-core.exe", "browser-provider.exe", "filesystem-provider.exe", "space-bridge.exe")
foreach ($e in $exes) {
    $path = "C:\spaceos\target\release\" + $e
    if (Test-Path $path) {
        Write-Host ("[PASS] found: " + $path) -ForegroundColor Green
    } else {
        Write-Host ("[FAIL] missing: " + $path) -ForegroundColor Red
        $failed = $true
    }
}
if ($failed) { exit 1 }
Write-Host "[PASS] all release binaries ready" -ForegroundColor Green
exit 0
