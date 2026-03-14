@echo off
cd /d "%~dp0"
echo Updating Binance Screener...

set BASE=https://raw.githubusercontent.com/xopromo/binance-collector/main

powershell -Command "Invoke-WebRequest -Uri '%BASE%/scripts/screener.py'     -OutFile 'scripts\screener.py'"
if errorlevel 1 goto fail
powershell -Command "Invoke-WebRequest -Uri '%BASE%/scripts/pair_filters.py' -OutFile 'scripts\pair_filters.py'"
if errorlevel 1 goto fail
powershell -Command "Invoke-WebRequest -Uri '%BASE%/scripts/collect_data.py' -OutFile 'scripts\collect_data.py'"
if errorlevel 1 goto fail

echo Done! Restarting screener...
taskkill /f /im streamlit.exe >nul 2>&1
timeout /t 1 >nul
start "" run_screener.bat
exit /b 0

:fail
echo ERROR: no internet or repo unavailable.
pause
exit /b 1
