@echo off
cd /d C:\spaceos
echo Checking Windows version...
for /f "tokens=2*" %%a in ('wmic os get Caption /value') do set "ver=%%b"
echo Windows %ver%

echo Checking Administrator privileges...
net session >nul 2>&1
if %errorlevel% == 0 (
    echo [PASS] Administrator: Yes
) else (
    echo [FAIL] Administrator: No - Required for named pipes
)

echo Checking .NET Runtime...
for /f "tokens=2*" %%a in ('reg query "HKLM\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full" /v Release 2^>nul') do set "release=%%b"
if defined release (
    echo [PASS] .NET Runtime: v4.x (Release %release%)
) else (
    echo [WARN] .NET Runtime: Not detected via registry
)

echo Checking Rust toolchain...
cargo --version >nul 2>&1
if %errorlevel% == 0 (
    for /f "tokens=*" %%a in ('cargo --version') do set "cargoVer=%%a"
    echo [PASS] Rust/Cargo: %cargoVer%
) else (
    echo [FAIL] Rust/Cargo: Not found in PATH
)

echo Checking Chrome executable...
set CHROME_PATH=C:\spaceos\chrome\win64-155.0.8059.39\chrome-win64\chrome.exe
if exist "%CHROME_PATH%" (
    echo [PASS] Chrome Executable: Found at %CHROME_PATH%
    "%CHROME_PATH%" --version >nul 2>&1
    if %errorlevel% == 0 (
        for /f "tokens=*" %%a in ('"%CHROME_PATH%" --version 2^>nul') do set "chromeVer=%%a"
        echo [PASS] Chrome Version: %chromeVer%
    ) else (
        echo [FAIL] Chrome Version: Exit code %errorlevel%
    )
) else (
    echo [FAIL] Chrome Executable: Not found at %CHROME_PATH%
)

echo Checking SpaceOS executables...
set "FOUND=0"
set "MISSING=0"
for %%f in (space-core.exe browser-provider.exe filesystem-provider.exe space-bridge.exe) do (
    if exist "C:\spaceos\target\release\%%f" (
        echo [PASS] Executable: %%f - Found
        set /a FOUND+=1
    ) else (
        echo [FAIL] Executable: %%f - Missing
        set /a MISSING+=1
    )
)

echo Checking named pipe capability...
powershell -NoProfile -Command "try { $pipe = [System.IO.Pipes.NamedPipeServerStream]::new('test-pipe', [System.IO.Pipes.PipeDirection]::InOut, 1, [System.IO.Pipes.PipeTransmissionMode]::Byte, [System.IO.Pipes.PipeOptions]::None); $pipe.Close(); exit 0 } catch { exit 1 }"
if %errorlevel% == 0 (
    echo [PASS] Named Pipes: Supported
) else (
    echo [FAIL] Named Pipes: Not supported
)

echo.
echo ==========================================
echo           Diagnostic Summary
echo ==========================================
echo Done.