# Test browser-provider.exe: needs Core pipe and Chrome CDP on 9222.
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\test_browser_provider.ps1
param([int]$TimeoutSeconds = 30)
$ErrorActionPreference = "Stop"
$exe = "C:\spaceos\target\release\browser-provider.exe"
if (-not (Test-Path $exe)) { Write-Host ("[FAIL] missing: " + $exe) -ForegroundColor Red; exit 1 }
if (-not (Test-Path "\\.\pipe\spaceos-core")) { Write-Host "[FAIL] core pipe missing; start space-core first" -ForegroundColor Red; exit 1 }
try {
    $v = Invoke-RestMethod -Uri "http://127.0.0.1:9222/json/version" -TimeoutSec 3 -ErrorAction Stop
    Write-Host ("CDP OK: " + $v.Browser)
} catch {
    Write-Host "[FAIL] CDP not reachable at 127.0.0.1:9222" -ForegroundColor Red
    exit 1
}
$env:SPACEOS_PROVIDER_ID = "browser"
try {
    $proc = Start-Process -FilePath $exe -PassThru -WindowStyle Hidden -WorkingDirectory "C:\spaceos" -RedirectStandardOutput "C:\spaceos\logs\test-browser.out.log" -RedirectStandardError "C:\spaceos\logs\test-browser.err.log"
} finally {
    Remove-Item Env:\SPACEOS_PROVIDER_ID -ErrorAction SilentlyContinue
}
if (-not $proc) { Write-Host "[FAIL] Start-Process returned nothing" -ForegroundColor Red; exit 1 }
Write-Host ("browser-provider PID: " + $proc.Id)
Start-Sleep 8
if ($proc.HasExited) {
    Write-Host ("[FAIL] exited early with code " + $proc.ExitCode + "; see logs/test-browser.err.log") -ForegroundColor Red
    exit 1
}
Write-Host "[PASS] browser-provider alive after 8s" -ForegroundColor Green
Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
Write-Host "[PASS] test_browser_provider done" -ForegroundColor Green
exit 0
