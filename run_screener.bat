@echo off
cd /d "%~dp0"
echo Starting Binance Screener...
echo Browser will open at: http://localhost:8501
echo Press Ctrl+C to stop.
echo.
streamlit run scripts/screener.py --server.headless false
pause
