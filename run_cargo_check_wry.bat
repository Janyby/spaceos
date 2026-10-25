@echo off
set OUTPUT=C:\spaceos\target\cargo_check_wry_feature.txt
echo Starting cargo check with --features wry-wrapper >> %OUTPUT% 2>&1
echo %date% %time% >> %OUTPUT% 2>&1
cargo check -p runtime --features wry-wrapper --message-format=short >> %OUTPUT% 2>&1
echo Exit code: %ERRORLEVEL% >> %OUTPUT% 2>&1
