@echo off
cd /d "%~dp0"
setlocal enabledelayedexpansion

echo ==========================================
echo  Binance Data Collector - Setup
echo ==========================================
echo.

net session >/dev/null 2>&1
if errorlevel 1 (
    echo Zapustite ot imeni Administratora!
    echo Pravaya knopka na faile - Zapusk ot imeni administratora
    pause
    exit /b 1
)

echo [1/5] Python...
python --version >/dev/null 2>&1
if errorlevel 1 (
    echo Python ne naiden! Skachivaju ustanovshchik...
    curl -o "%TEMP%\python_installer.exe" https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
    echo VAZNO: pri ustanovke postavte galochku "Add Python to PATH"
    start "" "%TEMP%\python_installer.exe"
    echo Posle ustanovki zapustite etot fail snova.
    pause
    exit /b 1
)
python --version
echo Python OK

echo.
echo [2/5] Biblioteki Python...
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r scripts\requirements.txt
if errorlevel 1 (
    echo OSHIBKA: proverte internet-soedinenie
    pause
    exit /b 1
)
echo Biblioteki OK

echo.
echo [3/5] GitHub token...
if exist "github_token.txt" (
    echo Token uzhe sokhranyon - OK
) else (
    echo Nuzhen GitHub token dlya otpravki dannych v oblako.
    echo.
    set /p GH_TOKEN="Vstavte GitHub token i nazhmite Enter: "
    if "!GH_TOKEN!"=="" (
        echo Token ne vveden.
    ) else (
        echo !GH_TOKEN!> github_token.txt
        echo Token sokhranyon - OK
    )
)

echo.
echo [4/5] Proverka Binance...
python -c "import requests; r=requests.get('https://api.binance.com/api/v3/ping',timeout=5); print('Binance OK') if r.status_code==200 else print('ERR Binance')"
if errorlevel 1 (
    echo Net soedinenija s Binance!
    pause
    exit /b 1
)

if not exist "logs" mkdir logs

echo.
echo [5/5] Planirovshhik zadach...
set "COLLECT_BAT=%~dp0run_collect.bat"
set "PUSH_BAT=%~dp0run_push.bat"

schtasks /delete /tn "BinanceCollector" /f >/dev/null 2>&1
schtasks /create /tn "BinanceCollector" /tr "cmd /c \"%COLLECT_BAT%\"" /sc minute /mo 5 /st 00:00 /ru "%USERNAME%" /f >/dev/null
if errorlevel 1 (
    echo ERR: Zadacha sbora ne sozdana
    pause
    exit /b 1
)
echo Sbor kazhdye 5 minut - OK

schtasks /delete /tn "BinanceGithubPush" /f >/dev/null 2>&1
schtasks /create /tn "BinanceGithubPush" /tr "cmd /c \"%PUSH_BAT%\"" /sc hourly /st 00:30 /ru "%USERNAME%" /f >/dev/null
if errorlevel 1 (
    echo WARN: Push zadacha ne sozdana
) else (
    echo Push GitHub kazhdyi chas - OK
)

echo.
echo ==========================================
echo  Ustanovka zavershena!
echo ==========================================
echo  Sbor: kazhdye 5 minut
echo  Push: kazhdyi chas
echo  Data: %~dp0data\
echo  Logs: %~dp0logs\
echo.
echo Pervyi sbor dannych...
echo.

python scripts\collect_data.py

echo.
echo Gotovo! Nazhmite lyubuyu klavishu.
pause >/dev/null
