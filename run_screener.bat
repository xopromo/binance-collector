@echo off
cd /d "%~dp0"
python -m streamlit run scripts\screener.py --server.headless true
