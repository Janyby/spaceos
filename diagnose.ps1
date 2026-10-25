<#>
.SYNOPSIS
    SpaceOS Environment Diagnostic - checks system readiness for SpaceOS components.

.DESCRIPTION
    Validates Windows version, Chrome installation, CDP connectivity, and SpaceOS executables.
    Run in Administrator PowerShell.

.NOTES
    Run: powershell -ExecutionPolicy Bypass -File C:\spaceos\diagnose.ps1
#>

param(
    [string]$ChromePath = "C:\spaceos\chrome\win64-155.0.8059.39\chrome-win64\chrome.exe",
    [string]$ProfileDir = "C:\spaceos\chrome-profile"
)

$ErrorActionPreference = "Stop"

function Write-Status { param($msg) { Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $msg" -ForegroundColor Cyan } }
function Write-Success { param($msg) { Write-Host "[$(Get-Date -Format 'HH:mm:ss')] [PASS] $msg" -ForegroundColor Green } }
function Write-Error { param($msg) { Write-Host "[$(Get-Date -Format 'HH:mm:ss')] [FAIL] $msg" -ForegroundColor Red } }
function Write-Warn { param($msg) { Write-Host "[$(Get-Date -Format 'HH:mm:ss')] [WARN] $msg" -ForegroundColor Yellow } }

$results = @()

function Add-Result { param($name, $passed, $details) { $results += [pscustomobject]@{ Name=$name; Passed=$passed; Details=$details } } }

Write-Host "============================================================"
Write-Host "          SpaceOS Environment Diagnostic                     "
Write-Host "============================================================"
Write-Host ""

# 1. Windows Version
Write-Status "Checking Windows version..."
$os = Get-CimInstance Win32_OperatingSystem
$ver = $os.Caption
$build = $os.BuildNumber
$verStr = "Windows " + $ver + " (Build " + $build + ")"
Write-Status $verStr
Add-Result "Windows Version" $true ($ver + " Build " + $build)

# 2. Administrator check
Write-Status "Checking Administrator privileges..."
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
Add-Result "Administrator" $isAdmin (if ($isAdmin) { "Yes" } else { "No - Required for named pipes" })

# 3. .NET / PowerShell
Write-Status "Checking .NET Runtime..."
$netVer = [System.Environment]::Version
Add-Result ".NET Runtime" $true ("v" + $netVer.ToString())

# 4. Cargo / Rust
Write-Status "Checking Rust toolchain..."
$cargoVer = cargo --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Add-Result "Rust/Cargo" $true $cargoVer.Trim()
} else {
    Add-Result "Rust/Cargo" $false "Not found in PATH"
}

# 5. Chrome executable
Write-Status "Checking Chrome executable..."
$ChromePath = "C:\spaceos\chrome\win64-155.0.8059.39\chrome-win64\chrome.exe"
$ProfileDir = "C:\spaceos\chrome-profile"
if (Test-Path $ChromePath) {
    Add-Result "Chrome Executable" $true "Found at " + $ChromePath
    
    # Try version
    try {
        $verOut = & $ChromePath --version 2>&1
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            Add-Result "Chrome Version" $true $verOut.Trim()
        } else {
            Add-Result "Chrome Version" $false ("Exit code: " + $exitCode)
        }
    } catch {
        Add-Result "Chrome Version" $false ("Exception: " + $_.Exception.Message)
    }
} else {
    Add-Result "Chrome Executable" $false ("Not found at " + $ChromePath)
}

# 6. Chrome CDP test
Write-Status "Testing Chrome CDP (launching with separate profile)..."
if (-not (Test-Path $ProfileDir)) {
    New-Item -ItemType Directory -Path $ProfileDir -Force | Out-Null
}

# Kill existing Chrome
Get-Process -Name chrome -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep 1

$chromeArgs = @(
    "--remote-debugging-port=9222",
    "--user-data-dir=$ProfileDir",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding"
)

Write-Status "Starting Chrome with CDP..."
$chromeProc = Start-Process -FilePath $ChromePath -ArgumentList $chromeArgs -PassThru -WindowStyle Hidden
if ($chromeProc) {
    Add-Result "Chrome Process Start" $true ("PID: " + $chromeProc.Id)
    
    # Wait for CDP
    Write-Status "Waiting for CDP endpoint (up to 30s)..."
    $cdpReady = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $resp = Invoke-RestMethod -Uri "http://127.0.0.1:9222/json/version" -ErrorAction Stop -TimeoutSec 2
            if ($resp) {
                Add-Result "CDP Endpoint" $true ("Version: " + $resp.Browser)
                $cdpReady = $true
                break
            }
        } catch { }
        Start-Sleep 1
    }
    if (-not $cdpReady) {
        Add-Result "CDP Endpoint" $false "Timeout after 30s"
    }
    
    # Cleanup
    Stop-Process -Id $chromeProc.Id -Force -ErrorAction SilentlyContinue
} else {
    Add-Result "Chrome Process Start" $false "Failed to start process"
}

# 7. SpaceOS executables
Write-Status "Checking SpaceOS executables..."
$exes = @(
    "space-core.exe",
    "browser-provider.exe", 
    "filesystem-provider.exe",
    "space-bridge.exe"
)
foreach ($exe in $exes) {
    $path = "C:\spaceos\target\release\" + $exe
    if (Test-Path $path) {
        Add-Result "Executable: " + $exe $true ("Found at " + $path)
    } else {
        Add-Result "Executable: " + $exe $false ("Missing at " + $path)
    }
}

# 8. Named pipe support
Write-Status "Checking named pipe capability..."
try {
    $pipe = [System.IO.Pipes.NamedPipeServerStream]::new("test-pipe", [System.IO.Pipes.PipeDirection]::InOut, 1, [System.IO.Pipes.PipeTransmissionMode]::Byte, [System.IO.Pipes.PipeOptions]::None)
    $pipe.Close()
    Add-Result "Named Pipes" $true "Supported"
} catch {
    Add-Result "Named Pipes" $false $_.Exception.Message
}

# Summary
Write-Host ""
Write-Host "============================================================"
Write-Host "                     Diagnostic Summary                       "
Write-Host "============================================================"

$passed = ($results | Where-Object { $_.Passed }).Count
$failed = ($results | Where-Object { -not $_.Passed }).Count

$results | Format-Table -AutoSize @{Expression={$_.Name};Label="Check";Width=25}, @{Expression={if($_.Passed){"PASS"}else{"FAIL"}};Label="Status";Width=10}, @{Expression={$_.Details};Label="Details";Width=60}

Write-Host ""
Write-Host "Passed: " + $passed + "  Failed: " + $failed

if ($failed -gt 0) {
    Write-Host ""
    Write-Warn "Some checks failed. Review details above."
    exit 1
} else {
    Write-Host ""
    Write-Success "All checks passed! Environment ready for SpaceOS."
    exit 0
}