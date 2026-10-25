# Test filesystem-provider.exe: needs Core pipe. Root comes from SPACEOS_ROOT env.
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\test_filesystem_provider.ps1
param([int]$TimeoutSeconds = 20, [string]$WatchPath = "C:\spaceos\data")
$ErrorActionPreference = "Stop"
$exe = "C:\spaceos\target\release\filesystem-provider.exe"
if (-not (Test-Path $exe)) { Write-Host ("[FAIL] missing: " + $exe) -ForegroundColor Red; exit 1 }
if (-not (Test-Path "\\.\pipe\spaceos-core")) { Write-Host "[FAIL] core pipe missing; start space-core first" -ForegroundColor Red; exit 1 }
if (-not (Test-Path $WatchPath)) { New-Item -ItemType Directory -Path $WatchPath -Force | Out-Null }
$env:SPACEOS_PROVIDER_ID = "filesystem"
$env:SPACEOS_ROOT = $WatchPath
try {
    $proc = Start-Process -FilePath $exe -PassThru -WindowStyle Hidden -WorkingDirectory "C:\spaceos" -RedirectStandardOutput "C:\spaceos\logs\test-fs.out.log" -RedirectStandardError "C:\spaceos\logs\test-fs.err.log"
} finally {
    Remove-Item Env:\SPACEOS_PROVIDER_ID -ErrorAction SilentlyContinue
    Remove-Item Env:\SPACEOS_ROOT -ErrorAction SilentlyContinue
}
if (-not $proc) { Write-Host "[FAIL] Start-Process returned nothing" -ForegroundColor Red; exit 1 }
Write-Host ("filesystem-provider PID: " + $proc.Id)
Start-Sleep 8
if ($proc.HasExited) {
    Write-Host ("[FAIL] exited early with code " + $proc.ExitCode + "; see logs/test-fs.err.log") -ForegroundColor Red
    exit 1
}
Write-Host "[PASS] filesystem-provider alive after 8s" -ForegroundColor Green
Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
Write-Host "[PASS] test_filesystem_provider done" -ForegroundColor Green
exit 0
