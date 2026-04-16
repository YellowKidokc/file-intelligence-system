"""Register/unregister FIS with Windows Task Scheduler for auto-start on boot.

Uses schtasks.exe — no admin rights needed for logon triggers.
"""

import subprocess
import sys
from pathlib import Path

from fis.log import get_logger

log = get_logger("startup")

TASK_NAME = "FIS_AutoStart"


def install():
    """Register FIS to start on Windows logon via Task Scheduler."""
    python_exe = sys.executable
    pythonw = python_exe.replace("python.exe", "pythonw.exe")
    if not Path(pythonw).exists():
        pythonw = python_exe

    # Command to run: pythonw -m fis _service
    cmd = f'"{pythonw}" -m fis _service'

    # Working directory = project root
    work_dir = str(Path(__file__).parent.parent.parent)

    try:
        # Delete existing task if present (ignore errors)
        subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            capture_output=True,
        )

        # Create new task with logon trigger
        result = subprocess.run(
            [
                "schtasks", "/Create",
                "/TN", TASK_NAME,
                "/TR", cmd,
                "/SC", "ONLOGON",
                "/RL", "LIMITED",
                "/F",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            log.info("FIS auto-start installed (Task Scheduler: %s).", TASK_NAME)
            log.info("Command: %s", cmd)
            log.info("FIS will start automatically on next login.")
        else:
            # Fallback: create a batch file in shell:startup
            _install_startup_folder(pythonw, work_dir)

    except FileNotFoundError:
        # schtasks not available — use startup folder
        _install_startup_folder(pythonw, work_dir)


def _install_startup_folder(pythonw: str, work_dir: str):
    """Fallback: place a batch file in the user's Startup folder."""
    import os

    startup_dir = Path(os.environ.get("APPDATA", "")) / \
        "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"

    if not startup_dir.exists():
        log.error("Could not find Startup folder: %s", startup_dir)
        return

    bat_path = startup_dir / "fis_autostart.bat"
    bat_content = f'@echo off\ncd /d "{work_dir}"\nstart "" "{pythonw}" -m fis _service\n'
    bat_path.write_text(bat_content, encoding="utf-8")
    log.info("FIS auto-start installed via Startup folder: %s", bat_path)


def uninstall():
    """Remove FIS from auto-start (both Task Scheduler and Startup folder)."""
    import os

    # Remove from Task Scheduler
    try:
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            log.info("Removed Task Scheduler entry: %s", TASK_NAME)
        else:
            log.info("No Task Scheduler entry found for %s.", TASK_NAME)
    except FileNotFoundError:
        pass

    # Remove from Startup folder
    startup_dir = Path(os.environ.get("APPDATA", "")) / \
        "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    bat_path = startup_dir / "fis_autostart.bat"
    if bat_path.exists():
        bat_path.unlink()
        log.info("Removed Startup folder entry: %s", bat_path)

    log.info("FIS auto-start uninstalled.")
