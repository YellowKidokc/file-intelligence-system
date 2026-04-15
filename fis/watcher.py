"""File watcher — monitors folders and triggers the FIS pipeline."""

import sys
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from fis.db.connection import get_config
from fis.pipeline import FISPipeline
from fis.renamer import rename_file


class FISHandler(FileSystemEventHandler):
    """Handles file creation/modification events."""

    def __init__(self, pipeline: FISPipeline, config):
        self.pipeline = pipeline
        self.debounce = int(config.get("watcher", "debounce_seconds", fallback="3"))
        self.ignore_ext = [
            ext.strip()
            for ext in config.get("watcher", "ignore_extensions", fallback="").split(",")
        ]
        self._pending = {}

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle(event.src_path)

    def _handle(self, file_path: str):
        path = Path(file_path)

        # Skip ignored extensions
        if path.suffix.lower() in self.ignore_ext:
            return

        # Skip hidden files and FIS metadata
        if path.name.startswith(".") or path.name == ".fis_meta.json":
            return

        # Skip files being written (debounce)
        import threading

        if file_path in self._pending:
            self._pending[file_path].cancel()

        timer = threading.Timer(self.debounce, self._process, [file_path])
        self._pending[file_path] = timer
        timer.start()

    def _process(self, file_path: str):
        self._pending.pop(file_path, None)
        try:
            result = self.pipeline.process(file_path)
            if result.get("status") == "auto":
                # Auto-rename high confidence files
                rename_file(
                    file_path,
                    result["proposed_name"],
                    result["file_id"],
                )
                print(f"[AUTO] {result['original_name']} -> {result['proposed_name']}")
            elif result.get("status") == "pending":
                print(f"[QUEUE] {result['original_name']} -> {result['proposed_name']} "
                      f"(confidence: {result['confidence']:.0f})")
            elif result.get("status") == "kickout":
                print(f"[KICKOUT] {result['original_name']} "
                      f"(confidence: {result.get('confidence', 0):.0f})")
            elif result.get("status") == "duplicate":
                print(f"[SKIP] {Path(file_path).name} is duplicate of {result['existing_id']}")
        except Exception as e:
            print(f"[ERROR] {file_path}: {e}")


def start_watcher():
    """Start the file watcher service."""
    config = get_config()
    pipeline = FISPipeline()

    folders_raw = config.get("watcher", "watch_folders", fallback="")
    folders = [f.strip() for f in folders_raw.split(",") if f.strip()]

    if not folders:
        print("No watch folders configured in settings.ini")
        sys.exit(1)

    handler = FISHandler(pipeline, config)
    observer = Observer()

    for folder in folders:
        if Path(folder).exists():
            observer.schedule(handler, folder, recursive=True)
            print(f"Watching: {folder}")
        else:
            print(f"Warning: folder not found, skipping: {folder}")

    observer.start()
    print(f"\nFIS Watcher running. Monitoring {len(folders)} folders.")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nFIS Watcher stopped.")

    observer.join()


if __name__ == "__main__":
    start_watcher()
