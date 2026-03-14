@echo off
cd /d "%~dp0"
set PYTHONUTF8=1
python -m streamlit run scripts\screener.py --server.headless true
