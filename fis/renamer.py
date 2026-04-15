"""File renaming logic with Obsidian-aware handling."""

import json
import subprocess
import urllib.parse
from pathlib import Path

from fis.db.connection import get_config
from fis.db.models import update_file_status


def rename_file(old_path: str, new_name: str, file_id: int):
    """Rename a file and update Postgres.

    For Obsidian .md files, uses Obsidian URI protocol to preserve graph links.
    For all other files, does a standard filesystem rename.
    Also writes/updates .fis_meta.json in the containing folder.
    """
    old = Path(old_path)
    if not old.exists():
        return

    config = get_config()
    vault_path = config.get("obsidian", "vault_path", fallback="")

    # Check if this file is inside the Obsidian vault
    if vault_path and str(old).startswith(vault_path) and old.suffix == ".md":
        _rename_obsidian(old, new_name, config)
    else:
        _rename_standard(old, new_name)

    # Update database
    new_path = old.parent / new_name
    update_file_status(file_id, "confirmed", new_name)

    # Update folder metadata
    _update_folder_meta(old.parent)


def _rename_standard(old_path: Path, new_name: str):
    """Standard filesystem rename."""
    new_path = old_path.parent / new_name
    if new_path.exists():
        # Avoid overwriting — append counter
        stem = new_path.stem
        ext = new_path.suffix
        counter = 1
        while new_path.exists():
            new_path = old_path.parent / f"{stem}_{counter}{ext}"
            counter += 1
    old_path.rename(new_path)


def _rename_obsidian(old_path: Path, new_name: str, config):
    """Rename via Obsidian URI protocol to preserve graph links."""
    vault_name = config.get("obsidian", "vault_name", fallback="")
    vault_root = Path(config.get("obsidian", "vault_path", fallback=""))

    # Get relative path within vault (without extension)
    rel_path = old_path.relative_to(vault_root)
    old_note = str(rel_path.with_suffix(""))
    new_note = str(rel_path.parent / Path(new_name).stem)

    # Encode for URI
    old_encoded = urllib.parse.quote(old_note)
    new_encoded = urllib.parse.quote(new_note)

    uri = f"obsidian://rename?vault={urllib.parse.quote(vault_name)}&file={old_encoded}&newname={new_encoded}"

    # Open the URI — Obsidian handles the rename internally
    subprocess.Popen(["cmd", "/c", "start", "", uri], shell=True)

    # Also write YAML frontmatter with FIS metadata
    _write_obsidian_frontmatter(old_path, new_name)


def _write_obsidian_frontmatter(file_path: Path, new_name: str):
    """Add/update FIS metadata in Obsidian note frontmatter."""
    content = file_path.read_text(encoding="utf-8")

    # Parse the new name for metadata
    parts = new_name.replace(file_path.suffix, "").split("_")
    fis_meta = f"fis_name: \"{new_name}\""

    if content.startswith("---"):
        # Frontmatter exists — add/update fis fields
        end = content.index("---", 3)
        frontmatter = content[3:end]
        if "fis_name:" in frontmatter:
            # Update existing
            lines = frontmatter.split("\n")
            lines = [l if not l.startswith("fis_name:") else fis_meta for l in lines]
            frontmatter = "\n".join(lines)
        else:
            frontmatter = frontmatter.rstrip() + "\n" + fis_meta + "\n"
        content = "---" + frontmatter + "---" + content[end + 3:]
    else:
        # No frontmatter — add it
        content = f"---\n{fis_meta}\n---\n{content}"

    file_path.write_text(content, encoding="utf-8")


def _update_folder_meta(folder: Path):
    """Write/update .fis_meta.json sidecar in the folder."""
    meta_path = folder / ".fis_meta.json"

    # Count FIS-classified files in this folder
    fis_files = [f for f in folder.iterdir() if f.is_file() and "_" in f.stem and not f.name.startswith(".")]

    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            meta = {}

    meta["file_count"] = len(fis_files)
    meta["last_updated"] = str(Path(folder).stat().st_mtime)

    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
