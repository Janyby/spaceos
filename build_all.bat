@echo off
cd /d C:\spaceos
echo Building space-core...
cargo build --release -p space-core 2>&1
echo space-core exit code: %errorlevel%
echo.
echo Building browser-provider...
cargo build --release -p browser-provider 2>&1
echo browser-provider exit code: %errorlevel%
echo.
echo Building filesystem-provider...
cargo build --release -p filesystem-provider 2>&1
echo filesystem-provider exit code: %errorlevel%
echo.
echo Building space-bridge...
cargo build --release -p space-bridge 2>&1
echo space-bridge exit code: %errorlevel%
echo.
echo Checking executables:
if exist target\release\space-core.exe echo space-core.exe: FOUND
if not exist target\release\space-core.exe echo space-core.exe: MISSING
if exist target\release\browser-provider.exe echo browser-provider.exe: FOUND
if not exist target\release\browser-provider.exe echo browser-provider.exe: MISSING
if exist target\release\filesystem-provider.exe echo filesystem-provider.exe: FOUND
if not exist target\release\filesystem-provider.exe echo filesystem-provider.exe: MISSING
if exist target\release\space-bridge.exe echo space-bridge.exe: FOUND
if not exist target\release\space-bridge.exe echo space-bridge.exe: MISSING