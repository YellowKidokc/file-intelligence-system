"""File watcher — monitors folders and triggers the FIS pipeline.

Routes .md files with YAML frontmatter to the recon ingest pipeline.
All other files go through the standard NLP pipeline.
"""

import sys
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from fis.db.connection import get_config
from fis.log import get_logger
from fis.pipeline import FISPipeline
from fis.renamer import rename_file

log = get_logger("watcher")


class FISHandler(FileSystemEventHandler):
    """Handles file creation/modification events.

    Routes .md files with YAML frontmatter to recon ingest.
    All other files go through the standard pipeline.
    """

    def __init__(self, pipeline: FISPipeline, config):
        self.pipeline = pipeline
        self.debounce = int(config.get("watcher", "debounce_seconds", fallback="3"))
        self.ignore_ext = [
            ext.strip()
            for ext in config.get("watcher", "ignore_extensions", fallback="").split(",")
        ]
        self.recon_enabled = config.get("recon", "enabled", fallback="true").lower() == "true"
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
            # Route .md files with frontmatter to recon ingest
            path = Path(file_path)
            if (self.recon_enabled
                    and path.suffix.lower() == ".md"
                    and self._has_frontmatter(file_path)):
                from fis.recon.recon_ingest import ingest
                result = ingest(file_path)
            else:
                result = self.pipeline.process(file_path)

            if result.get("status") == "auto":
                # Auto-rename high confidence files
                rename_file(
                    file_path,
                    result["proposed_name"],
                    result["file_id"],
                )
                log.info("AUTO %s -> %s", result['original_name'], result['proposed_name'])
            elif result.get("status") == "pending":
                log.info("QUEUE %s -> %s (confidence: %.0f)",
                         result['original_name'], result['proposed_name'], result['confidence'])
            elif result.get("status") == "kickout":
                log.info("KICKOUT %s (confidence: %.0f)",
                         result['original_name'], result.get('confidence', 0))
            elif result.get("status") == "duplicate":
                log.info("SKIP %s is duplicate of %s",
                         Path(file_path).name, result['existing_id'])
        except Exception as e:
            log.error("%s: %s", file_path, e)

    @staticmethod
    def _has_frontmatter(file_path: str) -> bool:
        """Quick check: does this file start with YAML frontmatter (---)."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                first_line = f.readline().strip()
                return first_line == "---"
        except (OSError, UnicodeDecodeError):
            return False


def start_watcher():
    """Start the file watcher service."""
    config = get_config()
    pipeline = FISPipeline()

    folders_raw = config.get("watcher", "watch_folders", fallback="")
    folders = [f.strip() for f in folders_raw.split(",") if f.strip()]

    if not folders:
        log.error("No watch folders configured in settings.ini")
        sys.exit(1)

    handler = FISHandler(pipeline, config)
    observer = Observer()

    for folder in folders:
        if Path(folder).exists():
            observer.schedule(handler, folder, recursive=True)
            log.info("Watching: %s", folder)
        else:
            log.warning("Folder not found, skipping: %s", folder)

    observer.start()
    log.info("FIS Watcher running. Monitoring %d folders.", len(folders))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log.info("FIS Watcher stopped.")

    observer.join()


if __name__ == "__main__":
    start_watcher()
