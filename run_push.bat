@echo off
cd /d "%~dp0"
python scripts\push_to_github.py >> logs\push.log 2>&1
