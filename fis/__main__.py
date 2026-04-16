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

        # --- Upgrade 1: Flexible codes CLI ---
        elif cmd == "codes":
            _handle_codes()

        # --- Upgrade 2: Service lifecycle CLI ---
        elif cmd == "start":
            from fis.startup.fis_service import start_background
            start_background()
        elif cmd == "stop":
            from fis.startup.fis_service import stop_service
            stop_service()
        elif cmd == "status":
            from fis.startup.fis_service import show_status
            show_status()
        elif cmd == "install":
            from fis.startup.install_startup import install
            install()
        elif cmd == "uninstall":
            from fis.startup.install_startup import uninstall
            uninstall()
        elif cmd == "_service":
            # Internal: run as background service (used by start/install)
            from fis.startup.fis_service import run_service
            run_service()

        # --- Upgrade 3: Recon/Cold-start CLI ---
        elif cmd == "cold-start":
            from fis.recon.cold_start import cold_start
            dry_run = "--dry-run" in sys.argv
            cold_start(dry_run=dry_run)

        else:
            _print_usage(cmd)
    else:
        # Default: start watcher
        start_watcher()


def _handle_codes():
    """Handle 'python -m fis codes <subcommand>' commands."""
    if len(sys.argv) < 3:
        _print_codes_usage()
        return

    subcmd = sys.argv[2]

    if subcmd == "list":
        from fis.db.codes import list_domains, list_subjects
        print("=== Domain Codes ===")
        for d in list_domains():
            aliases = ", ".join(d.get("aliases") or [])
            print(f"  {d['code']:4s}  {d['label']:<20s}  aliases: [{aliases}]")
        print()
        print("=== Subject Codes ===")
        for s in list_subjects():
            aliases = ", ".join((s.get("aliases") or [])[:3])
            print(f"  {s['code']:4s}  {s['label']:<25s}  domain: {s['domain']}  aliases: [{aliases}]")

    elif subcmd == "add-domain":
        if len(sys.argv) < 5:
            print("Usage: python -m fis codes add-domain CODE LABEL")
            return
        from fis.db.codes import add_domain
        code, label = sys.argv[3], " ".join(sys.argv[4:])
        add_domain(code, label)

    elif subcmd == "add-subject":
        if len(sys.argv) < 6:
            print("Usage: python -m fis codes add-subject CODE LABEL DOMAIN")
            return
        from fis.db.codes import add_subject
        code, label, domain = sys.argv[3], sys.argv[4], sys.argv[5]
        add_subject(code, label, domain)

    elif subcmd == "rename":
        if len(sys.argv) < 6:
            print("Usage: python -m fis codes rename OLD NEW TYPE")
            print("  TYPE: domain or subject")
            return
        from fis.db.codes import rename_code
        old, new, code_type = sys.argv[3], sys.argv[4], sys.argv[5]
        rename_code(old, new, code_type)

    else:
        _print_codes_usage()


def _print_codes_usage():
    print("Usage: python -m fis codes <subcommand>\n")
    print("Subcommands:")
    print("  list                           List all domain and subject codes")
    print("  add-domain CODE LABEL          Add a new domain code")
    print("  add-subject CODE LABEL DOMAIN  Add a new subject code")
    print("  rename OLD NEW TYPE            Rename a code (type: domain|subject)")


def _print_usage(bad_cmd=None):
    if bad_cmd:
        print(f"Unknown command: {bad_cmd}\n")
    print("Usage: python -m fis <command>\n")
    print("Core:")
    print("  watch           Start the file watcher (default)")
    print("  api [port]      Start REST API (default port: 8420)")
    print("  clipboard       Start clipboard monitor")
    print("  all             Start watcher + API + clipboard")
    print()
    print("Service:")
    print("  start           Start FIS as background service")
    print("  stop            Stop FIS background service")
    print("  status          Show service status, uptime, queue")
    print("  install         Install auto-start on Windows boot")
    print("  uninstall       Remove auto-start")
    print()
    print("Codes:")
    print("  codes list      List all domain and subject codes")
    print("  codes add-domain CODE LABEL")
    print("  codes add-subject CODE LABEL DOMAIN")
    print("  codes rename OLD NEW TYPE")
    print()
    print("Recon:")
    print("  cold-start           Bootstrap classifier from folder structure")
    print("  cold-start --dry-run Preview without training")
    print()
    print("Tools:")
    print("  backfill        Batch process existing folders")
    print("  popup           Open rename queue popup")
    print("  tray            Start system tray icon")
    print("  export          Export kickouts to Excel")
    print("  import [file]   Import corrections from Excel")
    print("  bil-export      Export BIL daily digest")
    print()
    print("Setup:")
    print("  init            Initialize database schema")
    print("  seed            Seed subject and domain codes")


if __name__ == "__main__":
    main()
