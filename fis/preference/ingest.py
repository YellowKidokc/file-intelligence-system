"""Bulk URL ingestion — parse URLs from multiple formats and feed to LinkIntelligence.

Supported formats:
    - Plain text (one URL per line)
    - CSV (columns: url, title, description, ...)
    - JSON (array of objects or array of strings)
    - Browser bookmarks HTML export (Netscape Bookmark File Format)
    - Clipboard paste (mixed text with URLs extracted)

Usage:
    from fis.preference.ingest import BulkIngestor

    bi = BulkIngestor()
    result = bi.from_file("bookmarks.html")
    result = bi.from_file("urls.txt")
    result = bi.from_text("https://example.com\\nhttps://foo.com")
    result = bi.from_urls(["https://a.com", "https://b.com"])
"""

import csv
import io
import json
import re
from html.parser import HTMLParser
from pathlib import Path

from fis.log import get_logger
from fis.preference.links import LinkIntelligence

log = get_logger("ingest")

# Regex to extract URLs from free text
URL_PATTERN = re.compile(
    r'https?://[^\s<>"\'}\])\,]+',
    re.IGNORECASE,
)


class BookmarkHTMLParser(HTMLParser):
    """Parse Netscape Bookmark File Format (exported from Chrome/Firefox/Edge)."""

    def __init__(self):
        super().__init__()
        self.bookmarks = []
        self._current_url = None
        self._current_title = None
        self._in_a_tag = False

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            attr_dict = dict(attrs)
            self._current_url = attr_dict.get("href", "")
            self._current_title = None
            self._in_a_tag = True

    def handle_data(self, data):
        if self._in_a_tag:
            self._current_title = data.strip()

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._in_a_tag:
            if self._current_url and self._current_url.startswith("http"):
                self.bookmarks.append({
                    "url": self._current_url,
                    "title": self._current_title or "",
                })
            self._in_a_tag = False
            self._current_url = None
            self._current_title = None


class BulkIngestor:
    """Parse URLs from multiple formats and feed them to LinkIntelligence."""

    def __init__(self):
        self.li = LinkIntelligence()

    def from_file(self, file_path: str) -> dict:
        """Auto-detect file format and ingest all URLs.

        Returns summary dict from LinkIntelligence.ingest_batch().
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        content = path.read_text(encoding="utf-8", errors="replace")
        suffix = path.suffix.lower()

        if suffix == ".json":
            urls = self._parse_json(content)
        elif suffix in (".csv", ".tsv"):
            delimiter = "\t" if suffix == ".tsv" else ","
            urls = self._parse_csv(content, delimiter)
        elif suffix in (".html", ".htm"):
            urls = self._parse_bookmarks_html(content)
        else:
            # Default: plain text, one URL per line
            urls = self._parse_text(content)

        log.info("Parsed %d URLs from %s (%s)", len(urls), path.name, suffix)
        return self.li.ingest_batch(urls, source="bulk")

    def from_text(self, text: str, source: str = "paste") -> dict:
        """Extract and ingest URLs from raw text (paste, clipboard, etc.)."""
        urls = self._parse_text(text)
        log.info("Extracted %d URLs from text paste", len(urls))
        return self.li.ingest_batch(urls, source=source)

    def from_urls(self, urls: list[str], source: str = "api") -> dict:
        """Ingest a pre-parsed list of URL strings."""
        items = [{"url": u.strip()} for u in urls if u.strip()]
        return self.li.ingest_batch(items, source=source)

    def from_urls_with_meta(self, items: list[dict], source: str = "api") -> dict:
        """Ingest pre-parsed URL dicts with optional title/description/content."""
        return self.li.ingest_batch(items, source=source)

    # --- Parsers ---

    def _parse_text(self, text: str) -> list[dict]:
        """Extract URLs from free-form text. One URL per line, or embedded in text."""
        urls = []
        seen = set()

        for match in URL_PATTERN.finditer(text):
            url = match.group(0).rstrip(".,;:!?)")
            if url not in seen:
                seen.add(url)
                urls.append({"url": url})

        return urls

    def _parse_json(self, content: str) -> list[dict]:
        """Parse JSON — array of strings or array of objects with 'url' key."""
        data = json.loads(content)

        if not isinstance(data, list):
            # Try to find an array in the top-level keys
            for key in ("urls", "links", "bookmarks", "items", "data"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                raise ValueError("JSON must be an array or contain a urls/links/bookmarks key")

        urls = []
        for item in data:
            if isinstance(item, str):
                urls.append({"url": item.strip()})
            elif isinstance(item, dict):
                url = item.get("url") or item.get("href") or item.get("link", "")
                if url:
                    urls.append({
                        "url": url.strip(),
                        "title": item.get("title"),
                        "description": item.get("description") or item.get("desc"),
                        "content_text": item.get("content_text") or item.get("content") or item.get("text"),
                    })
        return urls

    def _parse_csv(self, content: str, delimiter: str = ",") -> list[dict]:
        """Parse CSV with header row. Must have a 'url' column."""
        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)

        # Normalize header names
        if not reader.fieldnames:
            return []

        # Map common column names
        col_map = {}
        for col in reader.fieldnames:
            lower = col.strip().lower()
            if lower in ("url", "link", "href", "address"):
                col_map["url"] = col
            elif lower in ("title", "name"):
                col_map["title"] = col
            elif lower in ("description", "desc", "summary"):
                col_map["description"] = col
            elif lower in ("content", "text", "body"):
                col_map["content_text"] = col

        if "url" not in col_map:
            # Fallback: try first column
            col_map["url"] = reader.fieldnames[0]

        urls = []
        for row in reader:
            url = row.get(col_map["url"], "").strip()
            if url and url.startswith("http"):
                urls.append({
                    "url": url,
                    "title": row.get(col_map.get("title", ""), "").strip() or None,
                    "description": row.get(col_map.get("description", ""), "").strip() or None,
                    "content_text": row.get(col_map.get("content_text", ""), "").strip() or None,
                })
        return urls

    def _parse_bookmarks_html(self, content: str) -> list[dict]:
        """Parse Netscape Bookmark File Format (Chrome/Firefox/Edge export)."""
        parser = BookmarkHTMLParser()
        parser.feed(content)

        log.info("Found %d bookmarks in HTML export", len(parser.bookmarks))
        return parser.bookmarks
