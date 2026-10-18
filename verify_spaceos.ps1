# SpaceOS smoke test: executables, CDP, full stack, HTTP API, file
# scenario, MCP smoke. Leaves nothing behind unless -KeepRunning.
# Usage (normal Windows PowerShell, run from C:\spaceos):
#   powershell -ExecutionPolicy Bypass -File C:\spaceos\verify_spaceos.ps1 [-KeepRunning]
# Log: C:\spaceos\logs\verify_spaceos.log
param(
    [string]$WorkDir = "C:\spaceos",
    [string]$LogDir = "C:\spaceos\logs",
    [string]$ChromePath = "C:\spaceos\chrome\win64-155.0.8059.39\chrome-win64\chrome.exe",
    [string]$ChromeProfile = "C:\spaceos\chrome-profile",
    [int]$CdpPort = 9222,
    [string]$WatchPath = "C:\spaceos\data",
    [switch]$SkipBrowser,
    [switch]$NoCleanup,
    [switch]$KeepRunning
)

$LogFile = Join-Path $LogDir "verify_spaceos.log"
$stages = @()
$started = @()
$ownChrome = $false
$chromeProc = $null

function Log {
    param($msg)
    $line = "[" + (Get-Date -Format "HH:mm:ss") + "] " + $msg
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

function Stage {
    param($name, $status, $detail = "")
    $script:stages += [pscustomobject]@{ Stage = $name; Status = $status; Detail = $detail }
    Log ($status + " | " + $name + " | " + $detail)
}

function Wait-True {
    param([ScriptBlock]$Cond, [int]$Timeout = 30)
    for ($i = 0; $i -lt $Timeout; $i++) {
        try { if (& $Cond) { return $true } } catch { }
        Start-Sleep 1
    }
    return $false
}

function Start-Comp {
    param($Name, $Exe, $EnvTable, $ReadyCheck, $ReadyTimeout = 30)
    if (-not (Test-Path $Exe)) {
        Stage $Name "FAIL" ("missing " + $Exe)
        return $null
    }
    $outLog = Join-Path $LogDir ("verify-" + $Name + ".out.log")
    $errLog = Join-Path $LogDir ("verify-" + $Name + ".err.log")
    $saved = @{}
    if ($EnvTable) {
        foreach ($k in $EnvTable.Keys) {
            $saved[$k] = [System.Environment]::GetEnvironmentVariable($k)
            [System.Environment]::SetEnvironmentVariable($k, $EnvTable[$k])
        }
    }
    try {
        $sp = @{ FilePath = $Exe; PassThru = $true; WindowStyle = "Hidden"; WorkingDirectory = $WorkDir; RedirectStandardOutput = $outLog; RedirectStandardError = $errLog }
        $proc = Start-Process @sp
    } finally {
        foreach ($k in $saved.Keys) { [System.Environment]::SetEnvironmentVariable($k, $saved[$k]) }
    }
    if (-not $proc) { Stage $Name "FAIL" "Start-Process returned nothing"; return $null }
    Start-Sleep -Milliseconds 500
    if ($proc.HasExited) {
        Stage $Name "FAIL" ("exited immediately, code " + $proc.ExitCode + "; see " + $errLog)
        return $null
    }
    if ($ReadyCheck) {
        $ok = Wait-True -Cond $ReadyCheck -Timeout $ReadyTimeout
        if (-not $ok) {
            $state = "still running"
            if ($proc.HasExited) { $state = "exit code " + $proc.ExitCode }
            Stage $Name "FAIL" ("not ready in " + $ReadyTimeout + "s (" + $state + "); see " + $errLog)
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            return $null
        }
        if ($proc.HasExited) {
            Stage $Name "FAIL" ("ready then exited, code " + $proc.ExitCode)
            return $null
        }
    }
    Log ("started " + $Name + " PID=" + $proc.Id)
    return $proc
}

# ---- init ----
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
if (Test-Path $LogFile) { Remove-Item $LogFile -Force }
Log "SpaceOS verify started"
if (-not (Test-Path $WatchPath)) { New-Item -ItemType Directory -Path $WatchPath -Force | Out-Null }

# ---- 1. executables ----
$exes = @("space-core.exe", "space-bridge.exe", "browser-provider.exe", "filesystem-provider.exe")
$allExes = $true
foreach ($e in $exes) {
    $p = Join-Path (Join-Path $WorkDir "target\release") $e
    if (Test-Path $p) { Stage ("exe " + $e) "PASS" $p }
    else { Stage ("exe " + $e) "FAIL" ("missing " + $p); $allExes = $false }
}
if (-not $allExes) { Stage "verify" "FAIL" "missing executables, aborting"; exit 1 }

# ---- 2. CDP (start our own Chromium if none answers) ----
$cdpOk = $false
$cdpErr = ""
try {
    $v = Invoke-RestMethod -Uri "http://127.0.0.1:$CdpPort/json/version" -TimeoutSec 3 -ErrorAction Stop
    $cdpOk = $true
    Stage "cdp version" "PASS" $v.Browser
} catch {
    $cdpErr = $_.Exception.Message
}
if ((-not $cdpOk) -and (-not $SkipBrowser)) {
    if (-not (Test-Path $ChromePath)) {
        Stage "cdp version" "FAIL" $cdpErr
        Stage "cdp" "FAIL" "Chrome CDP not responding and no Chromium at $ChromePath; start Chrome with --remote-debugging-port=$CdpPort or use -SkipBrowser"
    } else {
        # A same-profile Chromium without CDP would swallow the new
        # process (Chrome forwards to the running instance and exits):
        # stop ONLY PIDs whose command line pins our profile, never a
        # foreign browser.
        try {
            $procs = Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction Stop
            foreach ($p in $procs) {
                if ($p.CommandLine -and $p.CommandLine -like ("*" + $ChromeProfile + "*")) {
                    Log ("stopping stale same-profile Chromium PID " + $p.ProcessId)
                    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
                }
            }
            Start-Sleep 2
        } catch { }
        if (-not (Test-Path $ChromeProfile)) { New-Item -ItemType Directory -Path $ChromeProfile -Force | Out-Null }
        $chromeArgs = @(
            "--remote-debugging-port=$CdpPort",
            "--user-data-dir=$ChromeProfile",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "about:blank"
        )
        Log "starting own Chromium for verify (CDP $CdpPort, own profile)..."
        $script:chromeProc = Start-Process -FilePath $ChromePath -ArgumentList $chromeArgs -PassThru -WorkingDirectory $WorkDir
        $cdpUp = Wait-True -Cond { try { $null -ne (Invoke-RestMethod -Uri "http://127.0.0.1:$CdpPort/json/version" -TimeoutSec 2 -ErrorAction Stop) } catch { $false } } -Timeout 30
        if ($cdpUp) {
            $script:ownChrome = $true
            $cdpOk = $true
            try {
                $v = Invoke-RestMethod -Uri "http://127.0.0.1:$CdpPort/json/version" -TimeoutSec 3 -ErrorAction Stop
                Stage "cdp version" "PASS" ($v.Browser + " (verify-started)")
            } catch {
                Stage "cdp version" "PASS" "verify-started Chromium on $CdpPort"
            }
        } else {
            if ($script:chromeProc -and -not $script:chromeProc.HasExited) { Stop-Process -Id $script:chromeProc.Id -Force -ErrorAction SilentlyContinue }
            $script:chromeProc = $null
            Stage "cdp version" "FAIL" $cdpErr
            Stage "cdp" "FAIL" "verify-started Chromium did not answer CDP on $CdpPort within 30s"
        }
    }
}

# ---- 3. start stack ----
$core = Start-Comp -Name "space-core" -Exe "C:\spaceos\target\release\space-core.exe" -ReadyTimeout 20 -ReadyCheck { Test-Path "\\.\pipe\spaceos-core" }
if (-not $core) { Stage "verify" "FAIL" "core failed"; exit 1 }
$started += $core
Stage "core pipe" "PASS" "\\.\pipe\spaceos-core"

$bridge = Start-Comp -Name "space-bridge" -Exe "C:\spaceos\target\release\space-bridge.exe" -ReadyTimeout 15 -ReadyCheck {
    try { $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/health" -TimeoutSec 2 -ErrorAction Stop; $r.status -eq "ok" } catch { $false }
}
if (-not $bridge) { Stage "verify" "FAIL" "bridge failed"; exit 1 }
$started += $bridge
Stage "bridge health" "PASS" "http://127.0.0.1:8765/api/health"

if (-not $SkipBrowser) {
    $bp = Start-Comp -Name "browser-provider" -Exe "C:\spaceos\target\release\browser-provider.exe" -EnvTable @{ SPACEOS_PROVIDER_ID = "browser"; SPACEOS_BROWSER_CDP_BASE = "http://127.0.0.1:$CdpPort" } -ReadyTimeout 30 -ReadyCheck {
        try {
            $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/providers" -TimeoutSec 2 -ErrorAction Stop
            $hit = $false
            foreach ($p in $r.providers) { if ($p.id -eq "browser") { $hit = $true } }
            $hit
        } catch { $false }
    }
    if ($bp) { $started += $bp; Stage "browser-provider registered" "PASS" "id=browser" }
} else {
    Stage "browser-provider registered" "NOT RUN" "-SkipBrowser"
}

$fp = Start-Comp -Name "filesystem-provider" -Exe "C:\spaceos\target\release\filesystem-provider.exe" -EnvTable @{ SPACEOS_PROVIDER_ID = "filesystem"; SPACEOS_ROOT = $WatchPath; SPACEOS_ALLOWED_ROOTS = $WatchPath } -ReadyTimeout 30 -ReadyCheck {
    try {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/providers" -TimeoutSec 2 -ErrorAction Stop
        $hit = $false
        foreach ($p in $r.providers) { if ($p.id -eq "filesystem") { $hit = $true } }
        $hit
    } catch { $false }
}
if ($fp) { $started += $fp; Stage "filesystem-provider registered" "PASS" "id=filesystem" }

# ---- 4. HTTP API checks ----
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/components" -TimeoutSec 5 -ErrorAction Stop
    Stage "GET /api/components" "PASS" "reachable"
} catch { Stage "GET /api/components" "FAIL" $_.Exception.Message }

try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/providers" -TimeoutSec 5 -ErrorAction Stop
    $ids = @()
    foreach ($p in $r.providers) { $ids += $p.id }
    Stage "GET /api/providers" "PASS" ("ids=" + ($ids -join ","))
} catch { Stage "GET /api/providers" "FAIL" $_.Exception.Message }

try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:8765/" -TimeoutSec 5 -ErrorAction Stop -UseBasicParsing
    if ($r.StatusCode -ne 200) { throw ("HTTP " + $r.StatusCode) }
    if ($r.Content -notlike '*SpaceOS World*') { throw "root does not serve the World page" }
    Stage "GET / world" "PASS" ("HTTP 200, World entry, bytes=" + $r.RawContentLength)
} catch { Stage "GET / world" "FAIL" $_.Exception.Message }

try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:8765/index.html" -TimeoutSec 5 -ErrorAction Stop -UseBasicParsing
    if ($r.StatusCode -ne 200) { throw ("HTTP " + $r.StatusCode) }
    $missing = @()
    foreach ($t in @("workspace", "files", "browser", "agent", "tasks", "activity", "system")) {
        if ($r.Content -notlike ('*data-tab="' + $t + '"*')) { $missing += $t }
    }
    if ($missing.Count -gt 0) { throw ("shell tabs missing: " + ($missing -join ",")) }
    Stage "GET /index.html shell" "PASS" ("HTTP 200, 7 tabs, bytes=" + $r.RawContentLength)
} catch { Stage "GET /index.html shell" "FAIL" $_.Exception.Message }

# ---- 5. browser tabs + navigate ----
if ($SkipBrowser) {
    Stage "GET /api/browser/tabs" "NOT RUN" "-SkipBrowser"
    Stage "POST /api/browser/navigate" "NOT RUN" "-SkipBrowser"
} else {
    try {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/tabs" -TimeoutSec 15 -ErrorAction Stop
        $tabs = $r.result.result.detail.tabs
        if ($null -eq $tabs) { $tabs = $r.result.result.tabs }
        if ($null -eq $tabs) { $tabs = $r.result.tabs }
        if ($null -eq $tabs) { throw "no tabs array in response" }
        $n = 0
        if ($tabs) { $n = $tabs.Count }
        if ($n -eq 0) { throw "tabs array empty" }
        Stage "GET /api/browser/tabs" "PASS" ("tabs=" + $n)
    } catch { Stage "GET /api/browser/tabs" "FAIL" $_.Exception.Message }

    try {
        $body = '{"url":"https://example.com"}'
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/navigate" -Method POST -Body $body -ContentType "application/json" -TimeoutSec 25 -ErrorAction Stop
        $inner = $r.result.result
        if ($null -eq $inner) { $inner = $r.result }
        $status = $inner.status
        if ($status -ne "Completed") { throw ("action status=" + $status + " detail=" + ($inner | ConvertTo-Json -Depth 4 -Compress)) }
        Stage "POST /api/browser/navigate" "PASS" ("status=Completed")
        Start-Sleep 3
        $t = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/tabs" -TimeoutSec 15 -ErrorAction Stop
        $tabs2 = $t.result.result.detail.tabs
        if ($null -eq $tabs2) { $tabs2 = $t.result.result.tabs }
        if ($null -eq $tabs2) { $tabs2 = $t.result.tabs }
        $found = $false
        foreach ($tab in $tabs2) { if ($tab.url -like "*example.com*") { $found = $true } }
        if ($found) { Stage "navigate effect" "PASS" "example.com present in tabs" }
        else { Stage "navigate effect" "FAIL" "example.com not found in tabs" }
        $navId = $r.result.result.action_id
        if ($navId) {
            $a = Invoke-RestMethod -Uri ("http://127.0.0.1:8765/api/actions/" + $navId) -TimeoutSec 5 -ErrorAction Stop
            $types = @()
            foreach ($ev in $a.events) { $types += $ev.event_type }
            if (($types -contains "ActionRequested") -and ($types -contains "ActionCompleted")) {
                Stage "navigate lifecycle" "PASS" ("Requested+Completed correlation=" + $navId)
            } else {
                Stage "navigate lifecycle" "FAIL" ("events: " + ($types -join ","))
            }
        } else {
            Stage "navigate lifecycle" "FAIL" "no action_id in navigate response"
        }
    } catch { Stage "POST /api/browser/navigate" "FAIL" $_.Exception.Message }

    # ---- 5b. tab management: new tab -> visible -> activate -> close ----
    # New tabs (never retargets), explicit close; provider polls CDP on
    # a ~5s tick, so visibility assertions poll instead of sleeping once.
    try {
        $nb = '{"url":"https://example.com/"}'
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/new-tab" -Method POST -Body $nb -ContentType "application/json" -TimeoutSec 25 -ErrorAction Stop
        $ni = $r.result.result
        if ($null -eq $ni) { $ni = $r.result }
        if ($ni.status -ne "Completed") { throw ("new-tab status=" + $ni.status) }
        $newId = $ni.detail.target_id
        if (-not $newId) { throw "new-tab: empty target_id" }
        Stage "POST /api/browser/new-tab" "PASS" ("target=" + $newId)
        $seen = $false
        for ($i = 0; $i -lt 8 -and -not $seen; $i++) {
            if ($i -gt 0) { Start-Sleep 3 }
            $t = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/tabs" -TimeoutSec 15 -ErrorAction Stop
            $tl = $t.result.result.detail.tabs
            if ($null -eq $tl) { $tl = $t.result.result.tabs }
            foreach ($tab in $tl) { if ($tab.target_id -eq $newId) { $seen = $true } }
        }
        if ($seen) { Stage "new-tab visible" "PASS" "provider attached the tab" }
        else { Stage "new-tab visible" "FAIL" "target $newId never listed" }
        $ab = '{"target_id":"' + $newId + '"}'
        $a = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/activate" -Method POST -Body $ab -ContentType "application/json" -TimeoutSec 25 -ErrorAction Stop
        $ai = $a.result.result
        if ($null -eq $ai) { $ai = $a.result }
        if ($ai.status -ne "Completed") { throw ("activate status=" + $ai.status) }
        Stage "POST /api/browser/activate" "PASS" "target focused"
        $c = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/close" -Method POST -Body $ab -ContentType "application/json" -TimeoutSec 25 -ErrorAction Stop
        $ci = $c.result.result
        if ($null -eq $ci) { $ci = $c.result }
        if ($ci.status -ne "Completed") { throw ("close status=" + $ci.status) }
        $gone = $false
        for ($i = 0; $i -lt 8 -and -not $gone; $i++) {
            if ($i -gt 0) { Start-Sleep 3 }
            $t = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/tabs" -TimeoutSec 15 -ErrorAction Stop
            $tl = $t.result.result.detail.tabs
            if ($null -eq $tl) { $tl = $t.result.result.tabs }
            $gone = $true
            foreach ($tab in $tl) { if ($tab.target_id -eq $newId) { $gone = $false } }
        }
        if ($gone) { Stage "POST /api/browser/close" "PASS" "tab gone after explicit close" }
        else { Stage "POST /api/browser/close" "FAIL" "target $newId still listed" }
    } catch { Stage "browser tab management" "FAIL" $_.Exception.Message }
}

# ---- 6. filesystem scan (small watched dir; full C:\spaceos exceeds bridge 10s IPC timeout) ----
try {
    $esc = $WatchPath.Replace("\", "\\")
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/filesystem/scan" -Method POST -Body ('{"path":"' + $esc + '"}') -ContentType "application/json" -TimeoutSec 60 -ErrorAction Stop
    if ($r.result.ok -ne $true) { throw ("scan ok=false: " + ($r.result | ConvertTo-Json -Depth 4 -Compress)) }
    $scanStatus = $r.result.result.status
    $scanId = $r.result.result.action_id
    if ($scanStatus -ne "Started") { throw ("expected Started, got " + $scanStatus) }
    if (-not $scanId) { throw "scan response has no action_id" }
    Stage "POST /api/filesystem/scan" "PASS" ("status=Started action_id=" + $scanId)
    $done = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep 1
        try {
            $a = Invoke-RestMethod -Uri ("http://127.0.0.1:8765/api/actions/" + $scanId) -TimeoutSec 5 -ErrorAction Stop
            foreach ($ev in $a.events) {
                if ($ev.event_type -eq "ActionCompleted") { $done = $true; break }
                if ($ev.event_type -eq "ActionFailed" -or $ev.event_type -eq "ActionDenied") { throw ("terminal: " + $ev.event_type) }
            }
            if ($done) { break }
        } catch {
            if ($_.Exception.Message -like "terminal:*") { throw $_ }
        }
    }
    if ($done) { Stage "scan completion event" "PASS" ("ActionCompleted correlation=" + $scanId) }
    else { Stage "scan completion event" "FAIL" "no ActionCompleted within 60s" }
} catch { Stage "POST /api/filesystem/scan" "FAIL" $_.Exception.Message }

# ---- 7. file scenario: create -> read -> write -> search -> disk ----
$helloPath = Join-Path $WatchPath "hello.txt"
if (Test-Path $helloPath) { Remove-Item $helloPath -Force; Log "setup: removed stale hello.txt" }
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/filesystem/create" -Method POST -Body (@{path=$helloPath; content="Hello from SpaceOS"} | ConvertTo-Json -Compress) -ContentType "application/json" -TimeoutSec 20 -ErrorAction Stop
    if ($r.result.result.status -ne "Completed") { throw ("create status=" + $r.result.result.status) }
    Stage "file create hello.txt" "PASS" ("action=" + $r.result.result.action_id)
    $createId = $r.result.result.action_id
} catch { Stage "file create hello.txt" "FAIL" $_.Exception.Message; $createId = $null }

try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/filesystem/read" -Method POST -Body (@{path=$helloPath} | ConvertTo-Json -Compress) -ContentType "application/json" -TimeoutSec 20 -ErrorAction Stop
    $content = $r.result.result.detail.content
    if ($content -ne "Hello from SpaceOS") { throw ("content mismatch: " + $content) }
    Stage "file read hello.txt" "PASS" "content verified"
} catch { Stage "file read hello.txt" "FAIL" $_.Exception.Message }

try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/filesystem/write" -Method POST -Body (@{path=$helloPath; content="Hello from SpaceOS, edited"} | ConvertTo-Json -Compress) -ContentType "application/json" -TimeoutSec 20 -ErrorAction Stop
    if ($r.result.result.status -ne "Completed") { throw ("write status=" + $r.result.result.status) }
    $disk = [System.IO.File]::ReadAllText($helloPath)
    if ($disk -ne "Hello from SpaceOS, edited") { throw ("disk mismatch: " + $disk) }
    Stage "file write hello.txt" "PASS" "disk content verified"
} catch { Stage "file write hello.txt" "FAIL" $_.Exception.Message }

try {
    $found = $false
    $tries = 0
    for ($tries = 0; $tries -lt 6; $tries++) {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/filesystem/search" -Method POST -Body '{"q":"hello.txt"}' -ContentType "application/json" -TimeoutSec 20 -ErrorAction Stop
        foreach ($it in $r.items) { if ($it.path -like "*hello.txt") { $found = $true } }
        if ($found) { break }
        Start-Sleep 2
    }
    if ($found) { Stage "file search hello.txt" "PASS" ("matches=" + $r.count) }
    else { Stage "file search hello.txt" "FAIL" "not indexed after watcher window" }
} catch { Stage "file search hello.txt" "FAIL" $_.Exception.Message }

# ---- 8. forbidden path refused (live roots gate) ----
try {
    $evilDir = Join-Path ([System.IO.Path]::GetTempPath()) "spaceos-forbidden"
    New-Item -ItemType Directory -Path $evilDir -Force | Out-Null
    $evil = Join-Path $evilDir "evil.txt"
    if (Test-Path $evil) { Remove-Item $evil -Force }
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/filesystem/create" -Method POST -Body (@{path=$evil; content="x"} | ConvertTo-Json -Compress) -ContentType "application/json" -TimeoutSec 20 -ErrorAction Stop
    if ($r.result.result.status -eq "Failed" -and (Test-Path $evil) -eq $false) {
        Stage "forbidden path refused" "PASS" "outside allowed roots, nothing written"
    } else {
        Stage "forbidden path refused" "FAIL" (($r.result.result | ConvertTo-Json -Compress) + " exists=" + (Test-Path $evil))
    }
} catch { Stage "forbidden path refused" "FAIL" $_.Exception.Message }

# ---- 9. MCP smoke over stdio (real binary, protocol + shared Core flow) ----
try {
    $mcpExe = Join-Path (Join-Path $WorkDir "target\release") "spaceos-mcp.exe"
    if (-not (Test-Path $mcpExe)) { throw "spaceos-mcp.exe missing" }
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $mcpExe
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.WorkingDirectory = $WorkDir
    $mp = [System.Diagnostics.Process]::Start($psi)
    $sw = $mp.StandardInput
    $sr = $mp.StandardOutput
    function McpRpc($id, $method, $params) {
        $req = (@{jsonrpc="2.0"; id=$id; method=$method; params=$params} | ConvertTo-Json -Compress -Depth 6)
        $sw.WriteLine($req)
        $sw.Flush()
        $line = $sr.ReadLine()
        if (-not $line) { throw "mcp: empty response" }
        return ($line | ConvertFrom-Json)
    }
    $init = McpRpc 9001 "initialize" @{protocolVersion="2024-11-05"; capabilities=@{}; clientInfo=@{}}
    if ($init.result.serverInfo.name -ne "spaceos") { throw "bad serverInfo" }
    Stage "mcp initialize" "PASS" "spaceos protocol 2024-11-05"
    $list = McpRpc 9002 "tools/list" @{}
    if ($list.result.tools.Count -ne 20) { throw ("tools=" + $list.result.tools.Count) }
    Stage "mcp tools/list" "PASS" "20 tools"
    $st = McpRpc 9003 "tools/call" @{name="spaceos_status"; arguments=@{}}
    if ($st.result.isError) { throw "spaceos_status isError" }
    Stage "mcp spaceos_status" "PASS" "via MCP"
    $nav = McpRpc 9004 "tools/call" @{name="browser_navigate"; arguments=@{url="https://example.com"}}
    $navText = $nav.result.content[0].text | ConvertFrom-Json
    if ($navText.status -ne "Completed") { throw ("navigate status=" + $navText.status) }
    $mcpActionId = $navText.action_id
    Stage "mcp browser_navigate" "PASS" ("action=" + $mcpActionId)
    $hist = McpRpc 9005 "tools/call" @{name="spaceos_actions"; arguments=@{limit=50}}
    $histText = $hist.result.content[0].text | ConvertFrom-Json
    $seen = $false
    foreach ($row in $histText.actions) { if ($row.action_id -eq $mcpActionId -and $row.actor -eq "space-bridge-mcp") { $seen = $true } }
    if ($seen) { Stage "mcp action in shared history" "PASS" ("actor=space-bridge-mcp action=" + $mcpActionId) }
    else { Stage "mcp action in shared history" "FAIL" "not found" }
    $content = McpRpc 9006 "tools/call" @{name="browser_get_content"; arguments=@{max_chars=500}}
    if ($content.result.isError) { throw "browser_get_content isError" }
    $contentText = $content.result.content[0].text | ConvertFrom-Json
    if ($contentText.status -ne "Completed") { throw ("content status=" + $contentText.status) }
    if (-not $contentText.excerpt) { throw "content excerpt empty" }
    Stage "mcp browser_get_content" "PASS" ("chars=" + $contentText.length)
    try { $mp.Kill() } catch { }
    $mp.WaitForExit(3000) | Out-Null
} catch { Stage "mcp smoke" "FAIL" $_.Exception.Message }

# ---- 9b. browser content over HTTP (same Core action as MCP tool) ----
try {
    $cb = '{"max_chars":500}'
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/browser/content" -Method POST -Body ([System.Text.Encoding]::ASCII.GetBytes($cb)) -ContentType "application/json" -TimeoutSec 25 -ErrorAction Stop
    $inner = $r.result.result
    if ($null -eq $inner) { $inner = $r.result }
    if ($inner.status -ne "Completed") { throw ("content status=" + $inner.status) }
    if (-not $r.excerpt) { throw "content excerpt empty" }
    Stage "GET /api/browser/content" "PASS" ("chars=" + $r.length)
} catch { Stage "GET /api/browser/content" "FAIL" $_.Exception.Message }

# ---- 10. websocket upgrade check ----
try {
    $tcp = New-Object Net.Sockets.TcpClient
    $iar = $tcp.BeginConnect("127.0.0.1", 8765, $null, $null)
    $connected = $iar.AsyncWaitHandle.WaitOne(5000)
    if (-not $connected) { throw "TCP connect timeout" }
    $tcp.EndConnect($iar)
    $stream = $tcp.GetStream()
    $key = "dGhlIHNhbXBsZSBub25jZQ=="
    $req = "GET /ws HTTP/1.1`r`nHost: 127.0.0.1:8765`r`nUpgrade: websocket`r`nConnection: Upgrade`r`nSec-WebSocket-Key: $key`r`nSec-WebSocket-Version: 13`r`n`r`n"
    $bytes = [Text.Encoding]::ASCII.GetBytes($req)
    $stream.Write($bytes, 0, $bytes.Length)
    $buf = New-Object byte[] 1024
    $stream.ReadTimeout = 5000
    $n = $stream.Read($buf, 0, 1024)
    $resp = [Text.Encoding]::ASCII.GetString($buf, 0, $n)
    $tcp.Close()
    if ($resp -like "HTTP/1.1 101*") { Stage "WS /ws upgrade" "PASS" "HTTP 101" }
    else { Stage "WS /ws upgrade" "FAIL" ("first line: " + ($resp.Split("`r`n")[0])) }
} catch { Stage "WS /ws upgrade" "FAIL" $_.Exception.Message }

# ---- summary ----
Log "---- summary ----"
foreach ($s in $stages) { Log ($s.Status + " | " + $s.Stage + " | " + $s.Detail) }
$fails = @($stages | Where-Object { $_.Status -eq "FAIL" })
if ($fails.Count -gt 0) { Log ("RESULT: FAIL (" + $fails.Count + " failed)") } else { Log "RESULT: PASS" }

# ---- cleanup (only processes started by this script) ----
if ((-not $NoCleanup) -and (-not $KeepRunning)) {
    Log "stopping test processes..."
    foreach ($p in $started) {
        if ($p -and -not $p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            Log ("stopped PID " + $p.Id)
        }
    }
    if ($ownChrome -and $chromeProc -and -not $chromeProc.HasExited) {
        Stop-Process -Id $chromeProc.Id -Force -ErrorAction SilentlyContinue
        Log ("stopped own Chromium PID " + $chromeProc.Id)
    }
} else {
    Log "KeepRunning: processes left running - dashboard at http://127.0.0.1:8765/"
}
Log "verify done. log: $LogFile"
if ($fails.Count -gt 0) { exit 1 }
exit 0
