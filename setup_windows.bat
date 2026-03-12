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
    echo     Правая кнопка мыши → «Запуск от имени администратора»
    pause
    exit /b 1
)

:: ─── Проверка Python ─────────────────────────────────────────────────────────
echo [1/4] Проверяю Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo [!] Python не найден!
    echo     Сейчас откроется страница загрузки.
    echo     Установите Python, поставьте галочку "Add Python to PATH",
    echo     затем запустите этот файл снова.
    echo.
    pause
    start https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe
    exit /b 1
)

for /f "tokens=*" %%i in ('python --version 2^>^&1') do set PY_VER=%%i
echo     Найден: %PY_VER% — OK

:: ─── Установка библиотек ─────────────────────────────────────────────────────
echo.
echo [2/4] Устанавливаю библиотеки Python...
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r scripts\requirements.txt
if errorlevel 1 (
    echo [ERR] Ошибка установки библиотек. Проверьте интернет-соединение.
    pause
    exit /b 1
)
echo     Установлено — OK

:: ─── Создание папки логов ────────────────────────────────────────────────────
if not exist "logs" mkdir logs

:: ─── Тестовый запуск ─────────────────────────────────────────────────────────
echo.
echo [3/4] Проверяю подключение к Binance...
python -c "import requests; r=requests.get('https://api.binance.com/api/v3/ping',timeout=5); print('    Binance API — OK') if r.status_code==200 else print('    [ERR] Binance недоступен')"
if errorlevel 1 (
    echo     [!] Нет соединения с Binance. Проверьте интернет.
    pause
    exit /b 1
)

:: ─── Планировщик задач ───────────────────────────────────────────────────────
echo.
echo [4/4] Создаю задачи в Планировщике Windows...

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
    pause
    exit /b 1
)
echo     Пуш на GitHub каждый час — OK

:: ─── Первый запуск ───────────────────────────────────────────────────────────
echo.
echo ============================================
echo   Установка завершена успешно!
echo ============================================
echo.
echo   Сбор данных: каждые 5 минут
echo   Пуш на GitHub: каждый час
echo   Данные: %~dp0data\
echo   Логи:   %~dp0logs\
echo.
echo   Запускаю первый сбор данных...
echo.

python scripts\collect_data.py
if errorlevel 1 (
    echo [ERR] Ошибка при первом запуске. Смотрите вывод выше.
) else (
    echo.
    echo   Готово! Данные собраны и сохранены в data\
)

echo.
echo   Нажмите любую клавишу для выхода.
pause >nul
