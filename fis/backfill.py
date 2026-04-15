"""Batch backfill — process existing folders through the FIS pipeline."""

import argparse
import os
import sys

from fis.db.connection import get_config
from fis.pipeline import FISPipeline
from fis.renamer import rename_file


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
                    print(f"  [AUTO] {fname} -> {result['proposed_name']}")
                elif status == "pending":
                    print(f"  [QUEUE] {fname} -> {result.get('proposed_name', '?')} "
                          f"({result.get('confidence', 0):.0f}%)")
                elif status == "kickout":
                    print(f"  [KICK] {fname} ({result.get('confidence', 0):.0f}%)")
                elif status == "duplicate":
                    print(f"  [DUP] {fname}")

            except Exception as e:
                results["error"] += 1
                print(f"  [ERR] {fname}: {e}")

    print(f"\n--- Backfill Complete ---")
    print(f"Auto-renamed: {results['auto']}")
    print(f"Pending review: {results['pending']}")
    print(f"Kickouts: {results['kickout']}")
    print(f"Duplicates: {results['duplicate']}")
    print(f"Errors: {results['error']}")


def main():
    parser = argparse.ArgumentParser(description="FIS Backfill — batch process existing files")
    parser.add_argument("--path", required=True, help="Folder to process recursively")
    parser.add_argument("--dry-run", action="store_true", help="Classify only, don't rename")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        print(f"Error: {args.path} is not a directory")
        sys.exit(1)

    print(f"Backfilling: {args.path}")
    if args.dry_run:
        print("(dry run — no files will be renamed)")
    print()

    backfill(args.path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
