# Stop SpaceOS runtime processes owned by this user.
# Stops ONLY SpaceOS executables; never touches Chrome, OpenCode or
# anything else. Fails openly when a process cannot be signalled
# (e.g. it runs elevated in another session).
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\stop_spaceos.ps1
$ErrorActionPreference = "Continue"
$names = @(
    "space-core",
    "space-bridge",
    "browser-provider",
    "filesystem-provider",
    "spaceos-mcp",
    "spaceos",
    "agent-demo"
)
$failed = $false
foreach ($name in $names) {
    $procs = Get-Process -Name $name -ErrorAction SilentlyContinue
    if (-not $procs) {
        Write-Host ("[SKIP] " + $name + ": not running")
        continue
    }
    foreach ($p in $procs) {
        try {
            Stop-Process -Id $p.Id -Force -ErrorAction Stop
            Write-Host ("[PASS] stopped " + $name + " PID=" + $p.Id) -ForegroundColor Green
        } catch {
            Write-Host ("[FAIL] cannot stop " + $name + " PID=" + $p.Id + ": " + $_.Exception.Message) -ForegroundColor Red
            $failed = $true
        }
    }
}
if ($failed) {
    Write-Host "[FAIL] some processes could not be stopped (likely elevated: rerun from an elevated shell)" -ForegroundColor Red
    exit 1
}
Write-Host "[PASS] SpaceOS runtime stopped" -ForegroundColor Green
exit 0
