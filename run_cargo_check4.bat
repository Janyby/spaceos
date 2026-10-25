@echo off
set OUTPUT=C:\spaceos\target\cargo_check_output4.txt
echo Starting cargo check >> %OUTPUT%
echo %date% %time% >> %OUTPUT%
cargo check -p runtime --message-format=short >> %OUTPUT% 2>&1
echo Exit code: %ERRORLEVEL% >> %OUTPUT%
