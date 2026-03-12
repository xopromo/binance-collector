@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ============================================
echo   Binance Data Collector — Установка
echo ============================================
echo.

:: ─── Проверка прав администратора ───────────────────────────────────────────
net session >nul 2>&1
if errorlevel 1 (
    echo [!] Запустите этот файл от имени Администратора!
    echo     Правая кнопка мыши на файле -- «Запуск от имени администратора»
    pause
    exit /b 1
)

:: ─── Проверка Python ─────────────────────────────────────────────────────────
echo [1/5] Проверяю Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo [!] Python не найден!
    echo     Сейчас скачается установщик Python.
    echo     При установке обязательно поставьте галочку "Add Python to PATH"
    echo     Затем запустите этот файл снова.
    echo.
    pause
    curl -o "%TEMP%\python_installer.exe" https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
    start "" "%TEMP%\python_installer.exe"
    exit /b 1
)

for /f "tokens=*" %%i in ('python --version 2^>^&1') do set PY_VER=%%i
echo     Найден: %PY_VER% — OK

:: ─── Установка библиотек ─────────────────────────────────────────────────────
echo.
echo [2/5] Устанавливаю библиотеки Python...
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r scripts\requirements.txt
if errorlevel 1 (
    echo [ERR] Ошибка установки. Проверьте интернет-соединение.
    pause
    exit /b 1
)
echo     Установлено — OK

:: ─── GitHub токен ────────────────────────────────────────────────────────────
echo.
echo [3/5] Настройка GitHub...

if exist "github_token.txt" (
    echo     Токен уже сохранён — OK
) else (
    echo     Нужен GitHub токен для отправки данных в облако.
    echo     Ваш токен можно найти на: github.com/settings/tokens
    echo.
    set /p GH_TOKEN="     Введите GitHub токен: "
    if "!GH_TOKEN!"=="" (
        echo     [!] Токен не введён. Данные будут сохраняться только локально.
    ) else (
        echo !GH_TOKEN!> github_token.txt
        echo     Токен сохранён — OK
    )
)

:: ─── Проверка Binance ────────────────────────────────────────────────────────
echo.
echo [4/5] Проверяю подключение к Binance...
python -c "import requests; r=requests.get('https://api.binance.com/api/v3/ping',timeout=5); print('    Binance API — OK') if r.status_code==200 else print('    [ERR] Binance недоступен')"
if errorlevel 1 (
    echo     [!] Нет соединения с Binance. Проверьте интернет.
    pause
    exit /b 1
)

:: ─── Создание папки логов ────────────────────────────────────────────────────
if not exist "logs" mkdir logs

:: ─── Планировщик задач ───────────────────────────────────────────────────────
echo.
echo [5/5] Создаю задачи в Планировщике Windows...

set "COLLECT_BAT=%~dp0run_collect.bat"
set "PUSH_BAT=%~dp0run_push.bat"

:: Сбор данных — каждые 5 минут
schtasks /delete /tn "BinanceCollector" /f >nul 2>&1
schtasks /create /tn "BinanceCollector" ^
    /tr "cmd /c \"%COLLECT_BAT%\"" ^
    /sc minute /mo 5 ^
    /st 00:00 ^
    /ru "%USERNAME%" ^
    /f >nul
if errorlevel 1 (
    echo     [ERR] Не удалось создать задачу сбора данных
    pause
    exit /b 1
)
echo     Сбор данных каждые 5 минут — OK

:: Пуш на GitHub — каждый час
schtasks /delete /tn "BinanceGithubPush" /f >nul 2>&1
schtasks /create /tn "BinanceGithubPush" ^
    /tr "cmd /c \"%PUSH_BAT%\"" ^
    /sc hourly ^
    /st 00:30 ^
    /ru "%USERNAME%" ^
    /f >nul
if errorlevel 1 (
    echo     [ERR] Не удалось создать задачу пуша на GitHub
) else (
    echo     Пуш на GitHub каждый час — OK
)

:: ─── Первый запуск ───────────────────────────────────────────────────────────
echo.
echo ============================================
echo   Установка завершена!
echo ============================================
echo.
echo   Сбор данных:   каждые 5 минут (пока ПК включён)
echo   Пуш на GitHub: каждый час
echo   Данные:        %~dp0data\
echo   Логи:          %~dp0logs\
echo.
echo   Запускаю первый сбор данных...
echo.

python scripts\collect_data.py

echo.
echo   Готово! Нажмите любую клавишу для выхода.
pause >nul
