# Sequential SpaceOS checks: unit tests plus pipe-isolated integration suites.
# Pipe suites serialize via the global IPC lock file; they need the global
# named pipes FREE (stop any running SpaceOS stack first).
# Full live-stack checks live in verify_spaceos.ps1.
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\run_tests.ps1
$ErrorActionPreference = "Stop"
Set-Location C:\spaceos
$failed = $false
# Integration suites spawn real binaries (spaceos.exe, agent-demo.exe,
# spaceos-mcp.exe) from target/debug: build them FIRST, otherwise
# cargo test -p <one-crate> leaves stale binaries from days ago
# (observed: product_slice failed on a spaceos.exe from 11.10).
Write-Host "=== build dependent binaries (spaceos, agent-demo, mcp) ==="
cargo build -p runtime -p agent-demo -p space-bridge
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] dependent binaries" -ForegroundColor Red
    exit 1
} else {
    Write-Host "[PASS] dependent binaries" -ForegroundColor Green
}
$suites = @(
    @("space-core lib", "cargo test -p space-core --lib"),
    @("browser-provider lib", "cargo test -p browser-provider --lib"),
    @("space-bridge lib", "cargo test -p space-bridge --lib"),
    @("filesystem-provider lib", "cargo test -p filesystem-provider --lib"),
    @("bridge vertical_slice (Core flow)", "cargo test -p space-bridge --test vertical_slice"),
    @("bridge mcp_stdio (protocol)", "cargo test -p space-bridge --test mcp_stdio"),
    @("bridge mcp_e2e (MCP via Core)", "cargo test -p space-bridge --test mcp_e2e"),
    @("filesystem roots_gate", "cargo test -p filesystem-provider --test roots_gate"),
    @("filesystem scan_search", "cargo test -p filesystem-provider --test scan_search"),
    @("filesystem e2e_identity", "cargo test -p filesystem-provider --test e2e_identity"),
    @("filesystem v17_action_semantics", "cargo test -p filesystem-provider --test v17_action_semantics"),
    @("filesystem performer_identity", "cargo test -p filesystem-provider --test performer_identity"),
    @("filesystem product_slice", "cargo test -p filesystem-provider --test product_slice"),
    @("bridge mission_workflow", "cargo test -p space-bridge --test mission_workflow"),
    @("core g7_restart", "cargo test -p space-core --test g7_core_restart"),
    @("bridge mcp_client", "cargo test -p space-bridge --test mcp_client"),
    @("bridge load_path", "cargo test -p space-bridge --test load_path"),
    @("bridge static_ui (UI assets)", "cargo test -p space-bridge --test static_ui")
)
foreach ($s in $suites) {
    Write-Host ("=== " + $s[0] + " ===")
    Invoke-Expression $s[1]
    if ($LASTEXITCODE -ne 0) {
        Write-Host ("[FAIL] " + $s[0]) -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host ("[PASS] " + $s[0]) -ForegroundColor Green
    }
}
if ($failed) { exit 1 }
Write-Host "[PASS] run_tests done" -ForegroundColor Green
exit 0
