"""Cold-start bootstrapper — harvests labels from existing folder structures.

Scans configured folder paths, maps folder names to domain.subject codes,
and feeds them as high-confidence training examples to the classifier.

Config maps folder paths to codes in settings.ini:
    [cold_start]
    O:\\_Theophysics_v3\\Master Equation = TP.MQ
    O:\\_Theophysics_v3\\Logos Papers = TP.LG

CLI:
    python -m fis cold-start
    python -m fis cold-start --dry-run
"""

import os
from pathlib import Path

from fis.db.codes import resolve_domain, resolve_subject
from fis.db.connection import get_config
from fis.log import get_logger
from fis.nlp.engines import YakeEngine
from fis.nlp.extractor import extract_text

log = get_logger("cold_start")


def _load_folder_mappings() -> dict[str, str]:
    """Load folder -> domain.subject mappings from config.

    Returns dict like {"/path/to/folder": "TP.MQ", ...}
    """
    config = get_config()
    mappings = {}

    if not config.has_section("cold_start"):
        return mappings

    for key, value in config.items("cold_start"):
        # Keys are lowercased by ConfigParser, so we store the original path
        # as the key and the domain.subject code as the value
        path = key.strip()
        code = value.strip().upper()
        if "." in code:
            mappings[path] = code

    return mappings


def cold_start(dry_run: bool = False):
    """Scan folder structures and feed labels to the classifier.

    For each mapped folder:
    1. Walk files in the folder
    2. Extract text
    3. Run YAKE for keywords
    4. Feed (text, keywords, domain, subject) to classifier.learn()
    """
    from fis.nlp.classifier import FISClassifier

    config = get_config()
    mappings = _load_folder_mappings()

    if not mappings:
        log.info("No folder mappings configured in [cold_start] section.")
        log.info("Add mappings to settings.ini like:")
        log.info("  [cold_start]")
        log.info("  O:\\_Theophysics_v3\\Master Equation = TP.MQ")
        return

    ignore_ext = [
        ext.strip()
        for ext in config.get("watcher", "ignore_extensions", fallback="").split(",")
    ]

    yake_top_n = int(config.get("pipeline", "yake_top_n", fallback="5"))
    yake = YakeEngine(top_n=yake_top_n)
    classifier = FISClassifier()

    # Collect training batches
    texts = []
    keywords_list = []
    domains = []
    subjects = []

    total_files = 0
    total_folders = 0

    for folder_path, code in mappings.items():
        folder = Path(folder_path)
        if not folder.exists():
            log.warning("Folder not found, skipping: %s", folder_path)
            continue

        parts = code.split(".")
        domain = resolve_domain(parts[0])
        subject = resolve_subject(parts[1]) if len(parts) > 1 else "GN"

        total_folders += 1
        folder_count = 0

        for root, dirs, files in os.walk(folder):
            dirs[:] = [d for d in dirs if not d.startswith(".")]

            for fname in files:
                if fname.startswith("."):
                    continue

                ext = os.path.splitext(fname)[1].lower()
                if ext in ignore_ext:
                    continue

                file_path = os.path.join(root, fname)

                try:
                    text = extract_text(file_path)
                    if not text.strip():
                        continue

                    keywords = yake.extract(text)

                    if dry_run:
                        log.info("[DRY] %s -> %s.%s (%d keywords)",
                                 fname, domain, subject, len(keywords))
                    else:
                        texts.append(text[:2000])
                        keywords_list.append(keywords)
                        domains.append(domain)
                        subjects.append(subject)

                    folder_count += 1
                    total_files += 1

                except Exception as e:
                    log.error("%s: %s", fname, e)

        log.info("Folder %s -> %s.%s: %d files", folder_path, domain, subject, folder_count)

    if dry_run:
        log.info("--- Cold Start Dry Run ---")
        log.info("Would train on %d files from %d folders.", total_files, total_folders)
        return

    # Feed to classifier in batches
    if texts:
        batch_size = 50
        for i in range(0, len(texts), batch_size):
            batch_end = min(i + batch_size, len(texts))
            classifier.learn(
                texts=texts[i:batch_end],
                keywords_list=keywords_list[i:batch_end],
                domains=domains[i:batch_end],
                subjects=subjects[i:batch_end],
            )
            log.info("Trained batch %d-%d of %d.", i + 1, batch_end, len(texts))

        log.info("--- Cold Start Complete ---")
        log.info("Trained on %d files from %d folders.", total_files, total_folders)
        log.info("Classifier saved to %s", classifier.model_dir)
    else:
        log.info("No files found to train on.")
