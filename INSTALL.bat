@echo off
echo ============================================
echo  FILE INTELLIGENCE SYSTEM - INSTALLER
echo ============================================
echo.

cd /d D:\GitHub\file-intelligence-system

echo [1/4] Installing Python packages...
echo       This will take 5-10 minutes (PySide6 + spaCy are large)
pip install -r requirements.txt
if errorlevel 1 (
    echo [WARNING] Some packages may have failed. Check output above.
) else (
    echo [OK] Packages installed.
)
echo.

echo [2/4] Downloading spaCy English model...
python -m spacy download en_core_web_sm
echo.

echo [3/4] Creating config from template...
if not exist config\settings.ini (
    copy config\settings.example.ini config\settings.ini
    echo [OK] settings.ini created. Edit with your Postgres password.
) else (
    echo [SKIP] settings.ini already exists.
)
echo.

echo [4/4] Initializing database...
echo       Make sure Postgres at 192.168.1.97 is running.
python -m fis.db.init_db
python -m fis.db.seed_codes
echo.

echo ============================================
echo  INSTALL COMPLETE
echo  Next: Edit config\settings.ini with your
echo  Postgres password, then run:
echo    python -m fis.watcher
echo ============================================
pause
