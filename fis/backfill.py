"""Batch backfill — process existing folders through the FIS pipeline."""

import argparse
import os
import sys

from fis.db.connection import get_config
from fis.log import get_logger
from fis.pipeline import FISPipeline
from fis.renamer import rename_file

log = get_logger("backfill")


def backfill(target_path: str, dry_run: bool = False, auto_approve: bool = False):
    """Walk a folder recursively and process every file."""
    pipeline = FISPipeline()
    config = get_config()

    ignore_ext = [
        ext.strip()
        for ext in config.get("watcher", "ignore_extensions", fallback="").split(",")
    ]

    results = {"auto": 0, "pending": 0, "kickout": 0, "duplicate": 0, "error": 0}

    for root, dirs, files in os.walk(target_path):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for fname in files:
            if fname.startswith("."):
                continue

            ext = os.path.splitext(fname)[1].lower()
            if ext in ignore_ext:
                continue

            file_path = os.path.join(root, fname)

            try:
                result = pipeline.process(file_path)
                status = result.get("status", "error")
                results[status] = results.get(status, 0) + 1

                if status == "auto" and not dry_run:
                    rename_file(file_path, result["proposed_name"], result["file_id"])
                    log.info("AUTO %s -> %s", fname, result['proposed_name'])
                elif status == "pending":
                    log.info("QUEUE %s -> %s (%.0f%%)",
                             fname, result.get('proposed_name', '?'), result.get('confidence', 0))
                elif status == "kickout":
                    log.info("KICK %s (%.0f%%)", fname, result.get('confidence', 0))
                elif status == "duplicate":
                    log.info("DUP %s", fname)

            except Exception as e:
                results["error"] += 1
                log.error("%s: %s", fname, e)

    log.info("--- Backfill Complete ---")
    log.info("Auto-renamed: %d", results['auto'])
    log.info("Pending review: %d", results['pending'])
    log.info("Kickouts: %d", results['kickout'])
    log.info("Duplicates: %d", results['duplicate'])
    log.info("Errors: %d", results['error'])


def main():
    parser = argparse.ArgumentParser(description="FIS Backfill — batch process existing files")
    parser.add_argument("--path", required=True, help="Folder to process recursively")
    parser.add_argument("--dry-run", action="store_true", help="Classify only, don't rename")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        log.error("%s is not a directory", args.path)
        sys.exit(1)

    log.info("Backfilling: %s", args.path)
    if args.dry_run:
        log.info("(dry run — no files will be renamed)")

    backfill(args.path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
