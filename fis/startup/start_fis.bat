@echo off
REM FIS Auto-Start — place in shell:startup or run manually
REM Starts FIS in background using pythonw.exe (no console window)

cd /d "%~dp0..\.."
set FIS_ROOT=%CD%

REM Prefer pythonw for windowless execution
where pythonw >nul 2>nul
if %ERRORLEVEL% equ 0 (
    start "" pythonw -m fis _service
) else (
    start /min "" python -m fis _service
)
