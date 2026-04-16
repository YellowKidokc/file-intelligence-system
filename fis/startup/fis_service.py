"""FIS background service — starts watcher + API + clipboard + tray.

Uses pythonw.exe (no console window). Writes PID file for lifecycle control.
Logs to config/service.log.

Start:  python -m fis start
Stop:   python -m fis stop
Status: python -m fis status
"""

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from fis.log import get_logger

log = get_logger("service")

PID_FILE = Path(__file__).parent.parent.parent / "config" / "fis.pid"
LOG_FILE = Path(__file__).parent.parent.parent / "config" / "service.log"
START_TIME_FILE = Path(__file__).parent.parent.parent / "config" / "fis.start"


def _write_pid():
    """Write current PID to file."""
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    START_TIME_FILE.write_text(datetime.now().isoformat(), encoding="utf-8")


def _remove_pid():
    """Remove PID file on shutdown."""
    PID_FILE.unlink(missing_ok=True)
    START_TIME_FILE.unlink(missing_ok=True)


def _read_pid() -> int | None:
    """Read PID from file, return None if not running."""
    if not PID_FILE.exists():
        return None
    try:
        pid = int(PID_FILE.read_text(encoding="utf-8").strip())
        # Verify the process is actually alive
        os.kill(pid, 0)
        return pid
    except (ValueError, ProcessLookupError, PermissionError):
        PID_FILE.unlink(missing_ok=True)
        return None


def run_service():
    """Run FIS as a foreground service (all components in threads)."""
    pid = _read_pid()
    if pid:
        log.info("FIS is already running (PID %d).", pid)
        return

    _write_pid()
    log.info("FIS service starting (PID %d).", os.getpid())

    # Graceful shutdown on signals
    def _shutdown(signum, frame):
        log.info("FIS service shutting down (signal %d).", signum)
        _remove_pid()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Start API server
    def _start_api():
        try:
            from fis.api import start_api
            start_api()
        except Exception as e:
            log.error("API failed: %s", e)

    # Start clipboard monitor
    def _start_clipboard():
        try:
            from fis.clipboard import start_clipboard_monitor
            start_clipboard_monitor()
        except Exception as e:
            log.error("Clipboard monitor failed: %s", e)

    threading.Thread(target=_start_api, daemon=True, name="fis-api").start()
    threading.Thread(target=_start_clipboard, daemon=True, name="fis-clipboard").start()

    log.info("FIS service running: watcher + API + clipboard.")

    # Watcher blocks on main thread
    try:
        from fis.watcher import start_watcher
        start_watcher()
    except KeyboardInterrupt:
        pass
    finally:
        _remove_pid()
        log.info("FIS service stopped.")


def start_background():
    """Start FIS as a detached background process using pythonw.exe."""
    pid = _read_pid()
    if pid:
        log.info("FIS is already running (PID %d).", pid)
        return

    # Find pythonw.exe for windowless execution on Windows
    python_exe = sys.executable
    pythonw = python_exe.replace("python.exe", "pythonw.exe")
    if not Path(pythonw).exists():
        pythonw = python_exe  # Fallback to regular python

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log_handle = open(LOG_FILE, "a", encoding="utf-8")

    proc = subprocess.Popen(
        [pythonw, "-m", "fis", "_service"],
        stdout=log_handle,
        stderr=log_handle,
        creationflags=getattr(subprocess, "DETACHED_PROCESS", 0)
        | getattr(subprocess, "CREATE_NO_WINDOW", 0),
        start_new_session=True,
    )

    # Wait briefly to confirm it started
    time.sleep(1)
    if proc.poll() is None:
        log.info("FIS started in background (PID %d). Log: %s", proc.pid, LOG_FILE)
    else:
        log.error("FIS failed to start. Check %s", LOG_FILE)


def stop_service():
    """Stop FIS via PID file."""
    pid = _read_pid()
    if not pid:
        log.info("FIS is not running.")
        return

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for graceful shutdown
        for _ in range(10):
            try:
                os.kill(pid, 0)
                time.sleep(0.5)
            except ProcessLookupError:
                break
        log.info("FIS stopped (PID %d).", pid)
    except ProcessLookupError:
        log.info("FIS process already gone (PID %d).", pid)
    finally:
        _remove_pid()


def show_status():
    """Show FIS service status."""
    from fis.db.connection import get_config

    pid = _read_pid()

    if pid:
        # Calculate uptime
        uptime_str = "unknown"
        if START_TIME_FILE.exists():
            try:
                start = datetime.fromisoformat(
                    START_TIME_FILE.read_text(encoding="utf-8").strip()
                )
                delta = datetime.now() - start
                hours, remainder = divmod(int(delta.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}h {minutes}m {seconds}s"
            except (ValueError, OSError):
                pass

        print(f"Status:    RUNNING")
        print(f"PID:       {pid}")
        print(f"Uptime:    {uptime_str}")
    else:
        print(f"Status:    STOPPED")
        print(f"PID:       -")
        print(f"Uptime:    -")

    # Watch folders
    try:
        config = get_config()
        folders_raw = config.get("watcher", "watch_folders", fallback="")
        folders = [f.strip() for f in folders_raw.split(",") if f.strip()]
        print(f"Watching:  {len(folders)} folders")
        for folder in folders:
            exists = Path(folder).exists()
            marker = "OK" if exists else "MISSING"
            print(f"           [{marker}] {folder}")
    except Exception:
        print(f"Watching:  (config not loaded)")

    # Files processed today
    if pid:
        try:
            from fis.db.models import _db
            with _db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM files WHERE created_at::date = CURRENT_DATE"
                    )
                    today = cur.fetchone()["cnt"]
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM files WHERE status = 'pending'"
                    )
                    pending = cur.fetchone()["cnt"]
            print(f"Today:     {today} files processed")
            print(f"Pending:   {pending} files in queue")
        except Exception:
            print(f"Today:     (database unavailable)")
            print(f"Pending:   (database unavailable)")
