"""FIS entry point — start the watcher with system tray."""

import sys
import threading

from fis.watcher import start_watcher


def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "watch":
            start_watcher()
        elif cmd == "backfill":
            from fis.backfill import main as backfill_main
            backfill_main()
        elif cmd == "popup":
            from fis.ui.popup import launch_popup
            launch_popup()
        elif cmd == "tray":
            from fis.ui.tray import launch_tray
            launch_tray()
        elif cmd == "export":
            from fis.export_kickouts import export_kickouts
            export_kickouts()
        elif cmd == "import":
            from fis.export_kickouts import import_corrections
            path = sys.argv[2] if len(sys.argv) > 2 else "kickouts.xlsx"
            import_corrections(path)
        elif cmd == "init":
            from fis.db.init_db import init_db
            init_db()
        elif cmd == "seed":
            from fis.db.seed_codes import seed_codes
            seed_codes()
        elif cmd == "bil-export":
            from fis.bil.bil_api import BIL
            bil = BIL()
            bil.export_daily()
        elif cmd == "api":
            from fis.api import start_api
            port = int(sys.argv[2]) if len(sys.argv) > 2 else 8420
            start_api(port)
        elif cmd == "clipboard":
            from fis.clipboard import start_clipboard_monitor
            start_clipboard_monitor()
        elif cmd == "all":
            # Start everything: API + watcher + clipboard (all as daemons)
            from fis.api import start_api
            from fis.clipboard import start_clipboard_monitor
            threading.Thread(target=start_api, daemon=True).start()
            threading.Thread(target=start_clipboard_monitor, daemon=True).start()
            start_watcher()  # Blocks on main thread
        else:
            _print_usage(cmd)
    else:
        # Default: start watcher
        start_watcher()


def _print_usage(bad_cmd=None):
    if bad_cmd:
        print(f"Unknown command: {bad_cmd}\n")
    print("Usage: python -m fis <command>\n")
    print("Commands:")
    print("  watch       Start the file watcher (default)")
    print("  api [port]  Start REST API (default port: 8420)")
    print("  clipboard   Start clipboard monitor")
    print("  all         Start watcher + API + clipboard")
    print("  backfill    Batch process existing folders")
    print("  popup       Open rename queue popup")
    print("  tray        Start system tray icon")
    print("  export      Export kickouts to Excel")
    print("  import      Import corrections from Excel")
    print("  init        Initialize database schema")
    print("  seed        Seed subject codes")
    print("  bil-export  Export BIL daily digest")


if __name__ == "__main__":
    main()
