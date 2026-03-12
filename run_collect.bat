@echo off
cd /d "%~dp0"
python scripts\collect_data.py >> logs\collect.log 2>&1
