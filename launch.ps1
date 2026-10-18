<#
.SYNOPSIS
    Launch SpaceOS system: Core, Bridge, Browser Provider, Filesystem Provider.

.DESCRIPTION
    Starts all SpaceOS components in dependency order:
    1. Chromium 155 with CDP on 127.0.0.1:9222 (reused if already responding)
    2. space-core.exe (Core IPC server on \\.\pipe\spaceos-core)
    3. space-bridge.exe (HTTP/WebSocket on 127.0.0.1:8765)
    4. browser-provider.exe (registers with Core, serves BROWSER_PROVIDER_PIPE)
    5. filesystem-provider.exe (registers with Core, serves PROVIDER_PIPE)

    Provider readiness is checked via the bridge HTTP API (which is up
    before providers start), plus process-liveness checks.

    MODES:
    - Default (debug): normal Chromium window first (Dashboard opened
      manually at http://127.0.0.1:8765/ for debugging).
    - -Shell: "SpaceOS Shell" — services start first, then a FULL
      Chromium window (native tabs + address bar, never --app/--kiosk)
      opens the Dashboard once the bridge is healthy. A running
      browser is reused by opening the Dashboard in a NEW tab (user
      tabs are never redirected or closed); only same-profile stale
      Chromium processes are stopped, foreign browsers are untouched.
    - -Native: services start first (Chromium still runs as the browser
      provider backend unless -SkipBrowser), then a native WebView2 window
      (`spaceos.exe wry`) opens the Dashboard. No Chromium UI window is
      opened. Requires the wry-enabled runtime build; this mode builds it
      (`cargo build --release -p runtime --features wry-wrapper`) if needed.

.NOTES
    Named pipes used here are local-machine only; no admin rights required.
    Run from C:\spaceos (per-component logs go to C:\spaceos\logs\).
#>

param(
    [string]$ChromePath = "C:\spaceos\chrome\win64-155.0.8059.39\chrome-win64\chrome.exe",
    [string]$WatchPath = "C:\spaceos\data",
    [string]$ProfileDir = "C:\spaceos\chrome-profile",
    [string]$LogDir = "C:\spaceos\logs",
    [string]$WorkDir = "C:\spaceos",
    [int]$CdpPort = 9222,
    [switch]$SkipBrowser,
    [switch]$Shell,
    [switch]$Native
)

$ErrorActionPreference = "Stop"

function Write-Status { param($msg) Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $msg" -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host "[$(Get-Date -Format 'HH:mm:ss')] [PASS] $msg" -ForegroundColor Green }
function Write-Error { param($msg) Write-Host "[$(Get-Date -Format 'HH:mm:ss')] [FAIL] $msg" -ForegroundColor Red }
function Write-Warn { param($msg) Write-Host "[$(Get-Date -Format 'HH:mm:ss')] [WARN] $msg" -ForegroundColor Yellow }

function Wait-Condition {
    param(
        [ScriptBlock]$Condition,
        [string]$Message,
        [int]$TimeoutSeconds = 30,
        [int]$IntervalSeconds = 1
    )
    Write-Status "Waiting for $Message (up to ${TimeoutSeconds}s)..."
    for ($i = 0; $i -lt $TimeoutSeconds; $i++) {
        if (& $Condition) { return $true }
        Start-Sleep -Seconds $IntervalSeconds
    }
    return $false
}

function Stop-SpaceOS {
    param([array]$Procs)
    foreach ($p in $Procs) {
        if ($p -and -not $p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
            Write-Status "Stopped $($p.ProcessName) (PID $($p.Id))"
        }
    }
}

function Start-WithCheck {
    param(
        [string]$Name,
        [string]$ExePath,
        [string[]]$Arguments,
        [hashtable]$Env,
        [ScriptBlock]$ReadyCheck,
        [int]$ReadyTimeout = 30,
        [string]$LogDir = "C:\spaceos\logs",
        [string]$WorkDir = "C:\spaceos"
    )
    if (-not (Test-Path $ExePath)) {
        Write-Error "$Name not found at $ExePath. Build it first."
        exit 1
    }
    $outLog = Join-Path $LogDir "$Name.out.log"
    $errLog = Join-Path $LogDir "$Name.err.log"
    # Apply extra env vars for the child (restored afterwards).
    $savedEnv = @{}
    if ($Env) {
        foreach ($k in $Env.Keys) {
            $savedEnv[$k] = [System.Environment]::GetEnvironmentVariable($k)
            [System.Environment]::SetEnvironmentVariable($k, $Env[$k])
        }
    }
    try {
        Write-Status "Starting $Name..."
        $sp = @{
            FilePath = $ExePath
            PassThru = $true
            WindowStyle = 'Hidden'
            WorkingDirectory = $WorkDir
            RedirectStandardOutput = $outLog
            RedirectStandardError = $errLog
        }
        # NOTE: -ArgumentList with an empty array fails on Windows PowerShell 5.1,
        # so the parameter is only added when there are actual arguments.
        if ($Arguments -and $Arguments.Count -gt 0) { $sp['ArgumentList'] = $Arguments }
        $proc = Start-Process @sp
    } finally {
        foreach ($k in $savedEnv.Keys) {
            [System.Environment]::SetEnvironmentVariable($k, $savedEnv[$k])
        }
    }
    if (-not $proc) {
        Write-Error "Failed to start $Name (Start-Process returned nothing)"
        exit 1
    }
    Start-Sleep -Milliseconds 500
    if ($proc.HasExited) {
        Write-Error "$Name exited immediately with code $($proc.ExitCode). See $errLog"
        exit 1
    }
    Write-Success "$Name started (PID $($proc.Id), logs: $outLog)"
    if ($ReadyCheck) {
        if (-not (Wait-Condition -Condition $ReadyCheck -Message "$Name readiness" -TimeoutSeconds $ReadyTimeout)) {
            $code = if ($proc.HasExited) { "$($proc.ExitCode)" } else { "still running" }
            Write-Error "$Name not ready within ${ReadyTimeout}s (PID $($proc.Id), exit/state: $code). See $outLog $errLog"
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            exit 1
        }
        if ($proc.HasExited) {
            Write-Error "$Name became ready then exited with code $($proc.ExitCode). See $errLog"
            exit 1
        }
    }
    return $proc
}

function Get-CdpVersion {
    param([int]$Port = 9222)
    try {
        return Invoke-RestMethod -Uri "http://127.0.0.1:$Port/json/version" -TimeoutSec 2 -ErrorAction Stop
    } catch { return $null }
}

function Test-Cdp {
    param([int]$Port = 9222)
    return $null -ne (Get-CdpVersion -Port $Port)
}

# True when the CDP browser has a real window (native tabs + address
# bar). A headless instance is NOT a usable Shell: it must be replaced
# (same profile only) or reported, never silently reused as "browser".
function Test-CdpHeaded {
    param([int]$Port = 9222)
    $v = Get-CdpVersion -Port $Port
    if ($null -eq $v) { return $false }
    return ($v.'User-Agent' -notlike '*Headless*')
}

function Test-BridgeHealth {
    try {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/health" -TimeoutSec 2 -ErrorAction Stop
        return $r.status -eq "ok"
    } catch { return $false }
}

function Test-ProviderRegistered {
    param([string]$Id)
    try {
        $r = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/providers" -TimeoutSec 2 -ErrorAction Stop
        foreach ($p in $r.providers) { if ($p.id -eq $Id) { return $true } }
        return $false
    } catch { return $false }
}

# PIDs of running chrome.exe instances launched with OUR profile dir.
# Only these may be stopped/restarted by the launcher; a foreign browser
# (different profile, user session) is never touched.
function Get-OwnChromePids {
    param([string]$ProfileDir)
    $pids = @()
    try {
        $procs = Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction Stop
        foreach ($p in $procs) {
            if ($p.CommandLine -and $p.CommandLine -like ("*" + $ProfileDir + "*")) {
                $pids += $p.ProcessId
            }
        }
    } catch {
        # CIM unavailable: fall back to no matches (never kill blindly).
    }
    return $pids
}

function Open-DashboardUrl {
    param([string]$Url = "http://127.0.0.1:8765/")
    try {
        # No profile flags on purpose: the RUNNING instance opens a new
        # tab. Never redirects or closes existing user tabs.
        Start-Process $Url | Out-Null
        return $true
    } catch { return $false }
}

# Admin is not required for local named pipes; warn only.
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Warn "Not running as Administrator. Local named pipes normally work without it; continuing."
}

foreach ($d in @($ProfileDir, $WatchPath, $LogDir)) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Path $d -Force | Out-Null
        Write-Status "Created directory: $d"
    }
}

# Stop only SpaceOS processes we own (never touch an existing browser).
foreach ($name in @("space-core", "browser-provider", "filesystem-provider", "space-bridge")) {
    $existing = Get-Process -Name $name -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Warn "Stopping stale $name (PID $($existing.Id))"
        $existing | Stop-Process -Force -ErrorAction SilentlyContinue
    }
}
Start-Sleep 1

$processes = @()
$ownBrowser = $false
$shellMode = $Shell -and (-not $SkipBrowser)
if ($Shell -and $SkipBrowser) {
    Write-Warn "Both -Shell and -SkipBrowser given: starting services only, no Shell window."
}

$CdpBase = "http://127.0.0.1:$CdpPort"
$DashboardUrl = "http://127.0.0.1:8765/"
$chromeBaseArgs = @(
    "--remote-debugging-port=$CdpPort",
    "--user-data-dir=$ProfileDir",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding"
)
# Readiness closures honour a custom -CdpPort (bare ${function:} refs
# would pin the default 9222).
$readyCdp = { Test-Cdp -Port $CdpPort }.GetNewClosure()

# Stale same-profile Chromium without CDP would conflict with a fresh
# instance on the same profile: stop ONLY those PIDs (never a browser
# the user runs with another profile).
function Stop-StaleOwnChrome {
    if (Test-Cdp) { return }
    $stale = Get-OwnChromePids -ProfileDir $ProfileDir
    foreach ($pid in $stale) {
        Write-Warn "Stopping stale same-profile Chromium (PID $pid, CDP silent)"
        Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
    }
    if ($stale.Count -gt 0) { Start-Sleep 2 }
}

# ===== 1. Chromium CDP (debug mode: normal window first) =====
# In Shell mode the window opens AFTER services are healthy (step 6).
if (-not $SkipBrowser -and -not $shellMode) {
    if (-not (Test-Path $ChromePath)) {
        Write-Error "Browser not found at: $ChromePath"
        exit 1
    }
    if (Test-Cdp -Port $CdpPort) {
        Write-Success "Chromium CDP already responding on 127.0.0.1:$CdpPort; reusing existing browser"
        Write-Status "Dashboard (debug): http://127.0.0.1:8765/"
    } else {
        Stop-StaleOwnChrome
        $browserProc = Start-WithCheck -Name "chromium" -ExePath $ChromePath -Arguments ($chromeBaseArgs + @($DashboardUrl)) -ReadyTimeout 30 -ReadyCheck $readyCdp -LogDir $LogDir -WorkDir $WorkDir
        $processes += $browserProc
        $ownBrowser = $true
    }
} elseif ($SkipBrowser) {
    Write-Warn "Skipping browser start (--SkipBrowser specified)"
}

# ===== 2. space-core =====
$coreProc = Start-WithCheck -Name "space-core" -ExePath "C:\spaceos\target\release\space-core.exe" -ReadyTimeout 30 -ReadyCheck {
    Test-Path "\\.\pipe\spaceos-core"
} -LogDir $LogDir -WorkDir $WorkDir
$processes += $coreProc

# ===== 3. space-bridge (before providers: they are checked via its API) =====
$bridgeProc = Start-WithCheck -Name "space-bridge" -ExePath "C:\spaceos\target\release\space-bridge.exe" -ReadyTimeout 15 -ReadyCheck ${function:Test-BridgeHealth} -LogDir $LogDir -WorkDir $WorkDir
$processes += $bridgeProc

# ===== 4. browser-provider =====
if (-not $SkipBrowser) {
    $browserProvProc = Start-WithCheck -Name "browser-provider" -ExePath "C:\spaceos\target\release\browser-provider.exe" -Env @{
        SPACEOS_PROVIDER_ID = "browser"
        SPACEOS_BROWSER_CDP_BASE = $CdpBase
    } -ReadyTimeout 30 -ReadyCheck {
        Test-ProviderRegistered -Id "browser"
    } -LogDir $LogDir -WorkDir $WorkDir
    $processes += $browserProvProc
} else {
    Write-Warn "Skipping browser-provider (--SkipBrowser specified)"
}

# ===== 5. filesystem-provider (root via env, not CLI args) =====
# Roots are secure-by-default; allow writes under the served WatchPath.
$fsProvProc = Start-WithCheck -Name "filesystem-provider" -ExePath "C:\spaceos\target\release\filesystem-provider.exe" -Env @{
    SPACEOS_PROVIDER_ID = "filesystem"
    SPACEOS_ROOT = $WatchPath
    SPACEOS_ALLOWED_ROOTS = $WatchPath
} -ReadyTimeout 30 -ReadyCheck {
    Test-ProviderRegistered -Id "filesystem"
} -LogDir $LogDir -WorkDir $WorkDir
$processes += $fsProvProc

# ===== 6. SpaceOS Shell window (Shell mode only, bridge already healthy) =====
# FULL Chromium window: native tabs + address bar. --app/--kiosk are
# banned here by design (they hide the browser UI). User tabs are never
# redirected or closed: reuse opens the Dashboard in a NEW tab.
if ($shellMode -and -not $Native) {
    if (-not (Test-Path $ChromePath)) {
        Write-Error "Browser not found at: $ChromePath"
        Stop-SpaceOS $processes
        exit 1
    }
    if (Test-Cdp -Port $CdpPort) {
        if (Test-CdpHeaded -Port $CdpPort) {
            # Headed browser (maybe a foreign profile): open the
            # Dashboard in a NEW tab via the OS shell association, so
            # the running instance handles it without profile flags
            # (no second instance, no user-tab redirects).
            Write-Success "Headed Chromium on 127.0.0.1:$CdpPort; reusing existing browser"
            if (Open-DashboardUrl -Url $DashboardUrl) {
                Write-Success "Dashboard opened in a new tab: $DashboardUrl"
            } else {
                Write-Warn "Could not open the Dashboard URL; open $DashboardUrl manually."
            }
        } else {
            # Headless CDP is not a usable Shell. Replace it ONLY when
            # it runs our own profile; a foreign headless instance is
            # left alone and reported with an escape hatch (-CdpPort).
            $ownHeadless = Get-OwnChromePids -ProfileDir $ProfileDir
            if ($ownHeadless.Count -eq 0) {
                Write-Error ("CDP on 127.0.0.1:$CdpPort answers headless from an unknown browser. " +
                    "Refusing to kill it. Either close it, or run with -CdpPort <free-port> " +
                    "so SpaceOS starts its own Chromium isolated from yours.")
                Stop-SpaceOS $processes
                exit 1
            }
            Write-Warn "Headless same-profile Chromium holds CDP; replacing with a headed Shell window."
            foreach ($pid in $ownHeadless) {
                Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
            }
            Start-Sleep 3
            if (Test-Cdp -Port $CdpPort) {
                Write-Error "Stale Chromium did not release 127.0.0.1:$CdpPort. Stop it manually and retry."
                Stop-SpaceOS $processes
                exit 1
            }
            Stop-StaleOwnChrome
            $browserProc = Start-WithCheck -Name "chromium-shell" -ExePath $ChromePath -Arguments ($chromeBaseArgs + @($DashboardUrl)) -ReadyTimeout 30 -ReadyCheck $readyCdp -LogDir $LogDir -WorkDir $WorkDir
            $processes += $browserProc
            $ownBrowser = $true
            Write-Success "SpaceOS Shell window opened (full browser, own profile)."
        }
    } else {
        Stop-StaleOwnChrome
        $browserProc = Start-WithCheck -Name "chromium-shell" -ExePath $ChromePath -Arguments ($chromeBaseArgs + @($DashboardUrl)) -ReadyTimeout 30 -ReadyCheck $readyCdp -LogDir $LogDir -WorkDir $WorkDir
        $processes += $browserProc
        $ownBrowser = $true
        Write-Success "SpaceOS Shell window opened (full browser, own profile)."
    }
}

# ===== 6b. Native SpaceOS window (WebView2 via `spaceos wry`) =====
# Opt-in client: no Chromium UI window -- the Dashboard runs in a native
# WebView2 window. Chromium still runs as the browser-provider backend
# (unless -SkipBrowser). The runtime binary must be built with the
# `wry-wrapper` feature; we build it here (cached after the first time) so
# the mode works out of the box.
if ($Native) {
    $nativeExe = "C:\spaceos\target\release\spaceos.exe"
    Write-Status "Native mode: ensuring wry-enabled runtime build..."
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        # Merge stderr into the pipeline on purpose: cargo writes warnings to
        # stderr, and with $ErrorActionPreference=Stop a native-command error
        # record would abort the launcher.
        & cargo build --release -p runtime --features wry-wrapper --manifest-path "C:\spaceos\Cargo.toml" 2>&1 | Out-String | Out-Null
        $buildCode = $LASTEXITCODE
    } catch {
        $buildCode = -1
        Write-Warn "cargo invocation failed: $($_.Exception.Message)"
    } finally {
        $ErrorActionPreference = $prevEap
    }
    if ($buildCode -ne 0) {
        Write-Error "Native runtime build failed (cargo exit $buildCode)."
        Stop-SpaceOS $processes
        exit 1
    }
    if (-not (Test-Path $nativeExe)) {
        Write-Error "Native binary not found at $nativeExe"
        Stop-SpaceOS $processes
        exit 1
    }
    # --url makes the target explicit: load the live Dashboard, never start a
    # second local UI server on the same port (that used to kill the window).
    $nativeProc = Start-WithCheck -Name "spaceos-native" -ExePath $nativeExe -Arguments @("wry", "--url", $DashboardUrl) -LogDir $LogDir -WorkDir $WorkDir
    $processes += $nativeProc
    Write-Success "Native SpaceOS window opened (WebView2): $DashboardUrl"
}

# Final verification
Write-Status "Verifying all components..."
$allReady = $true
if (-not (Test-Path "\\.\pipe\spaceos-core")) {
    Write-Error "Core pipe not available"
    $allReady = $false
}
if (-not (Test-BridgeHealth)) {
    Write-Error "Bridge HTTP server not responding"
    $allReady = $false
} else {
    Write-Success "Bridge HTTP server ready"
}
try {
    $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8765/api/components" -TimeoutSec 5 -ErrorAction Stop
    Write-Success "Components endpoint reachable"
} catch {
    Write-Warn "Could not fetch component statuses: $($_.Exception.Message)"
}

if (-not $allReady) {
    Write-Error "Some components failed to start properly"
    Stop-SpaceOS $processes
    exit 1
}

Write-Success "All components started and verified!"
if ($shellMode) {
    Write-Host ""
    Write-Host "  SpaceOS Shell is open in app mode. Close the Shell window"
    Write-Host "  or press Ctrl+C here to stop all components."
}
Write-Host ""
Write-Host "============================================================"
if ($shellMode) {
Write-Host "                  SpaceOS Shell Ready"
} else {
Write-Host "                  SpaceOS System Ready"
}
Write-Host "============================================================"
Write-Host "  Mode:         $($(if ($Native) { 'Native (WebView2 window)' } elseif ($shellMode) { 'Shell (full Chromium window)' } elseif ($SkipBrowser) { 'Headless (no browser)' } else { 'Debug (normal browser)' }))"
Write-Host "  Dashboard:    http://127.0.0.1:8765/"
Write-Host "  WebSocket:    ws://127.0.0.1:8765/ws"
Write-Host "  API:          http://127.0.0.1:8765/api/"
Write-Host "  Core IPC:     \\.\pipe\spaceos-core"
Write-Host "  Browser CDP:  http://127.0.0.1:9222"
Write-Host "  Logs:         $LogDir"
Write-Host "============================================================"
Write-Host ""
Write-Host "Press Ctrl+C to stop all components..."

try {
    while ($true) {
        Start-Sleep 5
        foreach ($p in $processes) {
            if ($p.HasExited) {
                Write-Error "Process $($p.Id) ($($p.ProcessName)) exited unexpectedly with code $($p.ExitCode)! Logs in $LogDir"
            }
        }
    }
} catch {
    Write-Warn "Shutdown requested..."
} finally {
    Write-Status "Shutting down all components..."
    Stop-SpaceOS $processes
    if ($ownBrowser) {
        Write-Status "Chromium started by this script was stopped with the stack."
    } else {
        Write-Status "A reused (foreign) browser was left running; only SpaceOS services were stopped."
    }
    Write-Success "All components stopped."
}
