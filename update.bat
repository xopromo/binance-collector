@echo off
cd /d "%~dp0"
echo Updating Binance Screener...

curl -sf -o scripts\screener.py     https://raw.githubusercontent.com/xopromo/binance-collector/main/scripts/screener.py
curl -sf -o scripts\pair_filters.py https://raw.githubusercontent.com/xopromo/binance-collector/main/scripts/pair_filters.py
curl -sf -o scripts\collect_data.py https://raw.githubusercontent.com/xopromo/binance-collector/main/scripts/collect_data.py

if errorlevel 1 (
    echo ERROR: no internet or repo unavailable.
    pause
    exit /b 1
)

echo Done! Restarting screener...
taskkill /f /im streamlit.exe >nul 2>&1
timeout /t 1 >nul
start "" run_screener.bat
