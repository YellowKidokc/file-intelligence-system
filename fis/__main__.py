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
        else:
            print(f"Unknown command: {cmd}")
            print("Commands: watch, backfill, popup, export, import, init, seed, bil-export")
    else:
        # Default: start watcher
        start_watcher()


if __name__ == "__main__":
    main()
