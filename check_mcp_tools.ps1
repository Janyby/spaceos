# Live MCP tool-call check: drives the real spaceos-mcp.exe over stdio
# (exactly how OpenCode spawns it) through STATE-CHANGING tools, then
# proves each call landed in the shared Core history with actor
# `space-bridge-mcp` and a queryable lifecycle. Needs a live stack
# (launch.ps1); starts nothing itself.
# Usage: powershell -ExecutionPolicy Bypass -File C:\spaceos\check_mcp_tools.ps1
# Log: C:\spaceos\logs\check_mcp_tools.log
param(
    [string]$WorkDir = "C:\spaceos",
    [string]$LogDir = "C:\spaceos\logs",
    [string]$ProbeDir = "C:\spaceos\data",
    [string]$McpExe = "C:\spaceos\target\release\spaceos-mcp.exe"
)
$ErrorActionPreference = "Stop"
Set-Location $WorkDir
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
$LogFile = Join-Path $LogDir "check_mcp_tools.log"
if (Test-Path $LogFile) { Remove-Item $LogFile -Force }
function Log($m) { $l = "[" + (Get-Date -Format "HH:mm:ss") + "] " + $m; Write-Host $l; Add-Content -Path $LogFile -Value $l }
$failed = $false
function Stage($n, $ok, $d = "") {
    if ($ok) { Log ("PASS | " + $n + " | " + $d) } else { Log ("FAIL | " + $n + " | " + $d); $script:failed = $true }
}

if (-not (Test-Path $McpExe)) { Log "FAIL | mcp binary | missing $McpExe"; exit 1 }
# Preflight: this script drives a live stack but starts nothing itself.
# Fail fast with a usable message instead of a cryptic `open pipe` isError.
$stackUp = (Test-Path "\\.\pipe\spaceos-core")
if ($stackUp) {
    try {
        $hb = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/health" -TimeoutSec 3 -ErrorAction Stop
        $stackUp = ($hb.status -eq "ok")
    } catch { $stackUp = $false }
}
if (-not $stackUp) {
    Log "FAIL | mcp live stack | Core pipe or bridge health missing; start the stack first (launch.ps1 -Shell or verify_spaceos.ps1 -KeepRunning)"
    Log "RESULT: FAIL"
    exit 1
}
$probeFile = Join-Path $ProbeDir ("mcp-probe-" + (Get-Date -Format "HHmmss") + ".txt")
try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $McpExe
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WorkingDirectory = $WorkDir
    $mp = [System.Diagnostics.Process]::Start($psi)
    $sw = $mp.StandardInput
    $sr = $mp.StandardOutput
    function Rpc($id, $method, $params) {
        $req = (@{jsonrpc = "2.0"; id = $id; method = $method; params = $params } | ConvertTo-Json -Compress -Depth 8)
        $sw.WriteLine($req); $sw.Flush()
        $line = $sr.ReadLine()
        if (-not $line) { throw "empty response" }
        return ($line | ConvertFrom-Json)
    }
    function Tool($id, $name, $toolArgs) {
        $r = Rpc $id "tools/call" @{name = $name; arguments = $toolArgs }
        if ($r.error) { throw ("tool error: " + ($r.error | ConvertTo-Json -Compress)) }
        if ($r.result.isError) { throw ("isError: " + $r.result.content[0].text) }
        return ($r.result.content[0].text | ConvertFrom-Json)
    }
    $init = Rpc 1 "initialize" @{protocolVersion = "2024-11-05"; capabilities = @{}; clientInfo = @{} }
    Stage "mcp initialize" ($init.result.serverInfo.name -eq "spaceos") "protocol 2024-11-05"

    # 0. Status, tabs, roots (read-only surface first).
    $sv = Tool 10 "spaceos_status" @{}
    Stage "mcp spaceos_status" ($null -ne $sv.core) "core ok"
    $tb = Tool 11 "browser_tabs" @{}
    Stage "mcp browser_tabs" ($tb.status -eq "Completed") "real tabs via CDP"
    $rt = Tool 12 "filesystem_roots" @{}
    Stage "mcp filesystem_roots" ($rt.result.mode -eq "restricted") ("mode=" + $rt.result.mode)

    # 1. Filesystem create -> read -> write through MCP (shared Core flow).
    $c = Tool 2 "filesystem_create_file" @{path = $probeFile; content = "mcp probe v1" }
    Stage "mcp create" ($c.status -eq "Completed") ("action=" + $c.action_id)
    $createId = $c.action_id
    $rd = Tool 3 "filesystem_read_file" @{path = $probeFile }
    $rdOk = ($rd.status -eq "Completed") -and ($rd.detail.content -eq "mcp probe v1")
    Stage "mcp read" $rdOk ("content verified, size=" + $rd.detail.size)
    $wr = Tool 4 "filesystem_write_file" @{path = $probeFile; content = "mcp probe v2"; expected_modified = $rd.detail.modified; expected_size = $rd.detail.size }
    $diskOk = ($wr.status -eq "Completed") -and ((Get-Content $probeFile -Raw).Trim() -eq "mcp probe v2")
    Stage "mcp write+conflict-stamps" $diskOk "disk verified"
    # Stale stamp must be refused loudly.
    $stale = Tool 5 "filesystem_write_file" @{path = $probeFile; content = "stale"; expected_modified = $rd.detail.modified; expected_size = $rd.detail.size }
    Stage "mcp stale write refused" (($stale.status -eq "Failed") -and ($stale.detail.error -like "conflict:*")) "conflict machine-readable"

    # 2. Browser content through MCP.
    $bc = Tool 6 "browser_get_content" @{max_chars = 500 }
    Stage "mcp browser_get_content" (($bc.status -eq "Completed") -and $bc.excerpt) ("chars=" + $bc.length)

    # 2b. Search finds the probe (retry: the index follows the write);
    # forbidden path refused loudly.
    $seHit = $false
    $seCount = 0
    for ($i = 0; $i -lt 6 -and -not $seHit; $i++) {
        if ($i -gt 0) { Start-Sleep 2 }
        $se = Tool 13 "filesystem_search" @{q = "mcp-probe-" }
        $seCount = $se.count
        foreach ($it in $se.items) { if ($it.path -like "*mcp-probe-*") { $seHit = $true } }
    }
    Stage "mcp filesystem_search" $seHit ("matches=" + $seCount)
    $evilDir = Join-Path ([System.IO.Path]::GetTempPath()) "spaceos-forbidden"
    New-Item -ItemType Directory -Path $evilDir -Force | Out-Null
    $evil = Join-Path $evilDir "evil.txt"
    if (Test-Path $evil) { Remove-Item $evil -Force }
    $fb = Tool 14 "filesystem_create_file" @{path = $evil; content = "x" }
    $fbOk = ($fb.status -eq "Failed") -and (-not (Test-Path $evil))
    Stage "mcp forbidden refused" $fbOk "outside allowed roots, nothing written"

    # 3. Every MCP call visible in shared history with the MCP actor.
    $h = Tool 7 "spaceos_actions" @{limit = 50 }
    $rows = $h.actions
    $mine = @($rows | Where-Object { $_.actor -eq "space-bridge-mcp" -and ($_.action_id -eq $createId) })
    Stage "mcp history actor" ($mine.Count -ge 1) ("actor=space-bridge-mcp action=" + $createId)

    # 4. Lifecycle queryable per action id.
    $s = Tool 8 "spaceos_action_status" @{action_id = $createId }
    Stage "mcp action lifecycle" ($s.status -eq "Completed") ("events=" + $s.event_count)

    try { $mp.Kill() } catch { }
    $mp.WaitForExit(3000) | Out-Null
}
catch {
    Stage "mcp live session" $false $_.Exception.Message
    try { $mp.Kill() } catch { }
}
finally {
    if (Test-Path $probeFile) { Remove-Item $probeFile -Force -ErrorAction SilentlyContinue }
}
if ($failed) { Log "RESULT: FAIL"; exit 1 }
Log "RESULT: PASS"
exit 0
