@echo off
setlocal
chcp 65001 >nul
set "PYTHONIOENCODING=utf-8"
cd /d "%~dp0"
set "PYTHONPATH=%~dp0..\.local-deps;%PYTHONPATH%"
if exist "%~dp0.venv\Scripts\python.exe" (
    set "COLLECTOR_PYTHON=%~dp0.venv\Scripts\python.exe"
    goto run
)
if exist "%~dp0..\.venv\Scripts\python.exe" (
    set "COLLECTOR_PYTHON=%~dp0..\.venv\Scripts\python.exe"
    goto run
)
if exist "%USERPROFILE%\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" (
    set "COLLECTOR_PYTHON=%USERPROFILE%\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
    goto run
)
where python >nul 2>nul
if errorlevel 1 (
    echo Python was not found. Install Python and requirements-collector.txt.
    pause
    exit /b 1
)
set "COLLECTOR_PYTHON=python"
:run
echo [collector] Starting Python...
"%COLLECTOR_PYTHON%" -u "%~dp0run_collector.py" %*
set "COLLECTOR_EXIT=%ERRORLEVEL%"
if not "%COLLECTOR_EXIT%"=="0" pause
exit /b %COLLECTOR_EXIT%
