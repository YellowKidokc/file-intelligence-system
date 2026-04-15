"""Link Intelligence — URL ingestion, metadata extraction, and classification.

Turns raw URLs into classified, searchable intelligence items that can be
liked/disliked to train BIL.

Usage:
    from fis.preference.links import LinkIntelligence

    li = LinkIntelligence()
    link = li.ingest("https://arxiv.org/abs/2401.12345")
    links = li.search("quantum consciousness")
"""

import hashlib
import re
from datetime import datetime
from urllib.parse import urlparse

from fis.db.models import _db
from fis.log import get_logger

log = get_logger("links")


class LinkIntelligence:
    """Ingest, classify, and manage URLs as first-class intelligence items."""

    def ingest(self, url: str, title: str = None, description: str = None,
               content_text: str = None, source: str = "manual") -> dict:
        """Ingest a single URL into the links table.

        If content_text is provided, classifies it through FIS NLP.
        If only a URL is given, stores it for later enrichment.

        Returns the link record dict.
        """
        url = url.strip()
        if not url:
            raise ValueError("URL cannot be empty")

        # Check for existing
        existing = self.get_by_url(url)
        if existing:
            log.info("Link already exists: %s (link_id=%s)", url, existing["link_id"])
            return existing

        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Strip www. prefix
        if domain.startswith("www."):
            domain = domain[4:]

        # Classify content if available
        fis_domain = None
        subject_codes = None
        slug = None
        keywords = None
        confidence = None
        classified_at = None
        content_hash = None

        if content_text:
            content_text = content_text[:50000]  # Cap at 50k chars
            content_hash = hashlib.sha256(content_text.encode("utf-8")).hexdigest()

            # Check content dedup
            dup = self._content_exists(content_hash)
            if dup:
                log.info("Duplicate content for %s (matches link_id=%s)", url, dup["link_id"])

            classification = self._classify(content_text)
            fis_domain = classification.get("domain")
            subject_codes = classification.get("subjects")
            slug = classification.get("slug")
            keywords = classification.get("keywords")
            confidence = classification.get("confidence")
            classified_at = datetime.now()

        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO links
                        (url, domain, title, description, content_text,
                         fis_domain, subject_codes, slug, keywords, confidence,
                         content_hash, source, classified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (url, domain, title, description, content_text,
                     fis_domain, subject_codes, slug, keywords, confidence,
                     content_hash, source, classified_at),
                )
                conn.commit()
                link = cur.fetchone()

        log.info("Ingested: %s → domain=%s conf=%.1f%%",
                 url, fis_domain or "?", (confidence or 0))
        return dict(link)

    def ingest_batch(self, urls: list[dict], source: str = "bulk") -> dict:
        """Ingest multiple URLs at once.

        Args:
            urls: List of dicts, each with at least 'url' key.
                  Optional keys: title, description, content_text
            source: Source tag for all items in this batch.

        Returns summary dict with counts.
        """
        results = {"ingested": 0, "skipped": 0, "errors": 0, "links": []}

        for item in urls:
            if isinstance(item, str):
                item = {"url": item}

            url = item.get("url", "").strip()
            if not url:
                results["errors"] += 1
                continue

            try:
                link = self.ingest(
                    url=url,
                    title=item.get("title"),
                    description=item.get("description"),
                    content_text=item.get("content_text"),
                    source=source,
                )
                # Check if it was already existing (skipped)
                if link.get("source") != source:
                    results["skipped"] += 1
                else:
                    results["ingested"] += 1
                results["links"].append({"link_id": link["link_id"], "url": url})
            except Exception as e:
                log.error("Failed to ingest %s: %s", url, e)
                results["errors"] += 1

        log.info("Batch complete: %d ingested, %d skipped, %d errors",
                 results["ingested"], results["skipped"], results["errors"])
        return results

    def enrich(self, link_id: int, title: str = None, description: str = None,
               content_text: str = None) -> dict:
        """Enrich an existing link with metadata and/or content.

        Re-classifies if content_text is newly provided.
        """
        link = self.get_by_id(link_id)
        if not link:
            raise ValueError(f"Link {link_id} not found")

        updates = {}
        if title:
            updates["title"] = title
        if description:
            updates["description"] = description

        if content_text and not link.get("content_text"):
            content_text = content_text[:50000]
            updates["content_text"] = content_text
            updates["content_hash"] = hashlib.sha256(content_text.encode("utf-8")).hexdigest()

            classification = self._classify(content_text)
            updates["fis_domain"] = classification.get("domain")
            updates["subject_codes"] = classification.get("subjects")
            updates["slug"] = classification.get("slug")
            updates["keywords"] = classification.get("keywords")
            updates["confidence"] = classification.get("confidence")
            updates["classified_at"] = datetime.now()

        if not updates:
            return dict(link)

        set_clause = ", ".join(f"{k} = %s" for k in updates)
        values = list(updates.values()) + [link_id]

        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE links SET {set_clause} WHERE link_id = %s RETURNING *",
                    values,
                )
                conn.commit()
                return dict(cur.fetchone())

    def get_by_id(self, link_id: int) -> dict | None:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM links WHERE link_id = %s", (link_id,))
                row = cur.fetchone()
                return dict(row) if row else None

    def get_by_url(self, url: str) -> dict | None:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM links WHERE url = %s", (url.strip(),))
                row = cur.fetchone()
                return dict(row) if row else None

    def search(self, query: str, limit: int = 20, fis_domain: str = None) -> list[dict]:
        """Search links by title, URL, keywords, slug, or domain."""
        with _db() as conn:
            with conn.cursor() as cur:
                conditions = [
                    "l.url ILIKE %s",
                    "l.title ILIKE %s",
                    "l.slug ILIKE %s",
                    "l.domain ILIKE %s",
                    "EXISTS (SELECT 1 FROM unnest(l.keywords) kw WHERE kw ILIKE %s)",
                ]
                params = [f"%{query}%"] * 5

                where = "(" + " OR ".join(conditions) + ")"

                if fis_domain:
                    where += " AND l.fis_domain = %s"
                    params.append(fis_domain)

                cur.execute(
                    f"""
                    SELECT l.*,
                           (SELECT p.action FROM preferences p
                            WHERE p.link_id = l.link_id
                            ORDER BY p.created_at DESC LIMIT 1) AS last_pref
                    FROM links l
                    WHERE {where}
                    ORDER BY l.created_at DESC
                    LIMIT %s
                    """,
                    params + [limit],
                )
                return [dict(r) for r in cur.fetchall()]

    def list_links(self, limit: int = 50, offset: int = 0, source: str = None,
                   fis_domain: str = None, unclassified_only: bool = False) -> list[dict]:
        """List links with optional filters."""
        conditions = []
        params = []

        if source:
            conditions.append("l.source = %s")
            params.append(source)
        if fis_domain:
            conditions.append("l.fis_domain = %s")
            params.append(fis_domain)
        if unclassified_only:
            conditions.append("l.classified_at IS NULL")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT l.*,
                           (SELECT p.action FROM preferences p
                            WHERE p.link_id = l.link_id
                            ORDER BY p.created_at DESC LIMIT 1) AS last_pref
                    FROM links l
                    {where}
                    ORDER BY l.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    params + [limit, offset],
                )
                return [dict(r) for r in cur.fetchall()]

    def stats(self) -> dict:
        """Return summary stats about ingested links."""
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE classified_at IS NOT NULL) AS classified,
                        COUNT(*) FILTER (WHERE classified_at IS NULL) AS unclassified,
                        COUNT(DISTINCT domain) AS unique_domains,
                        COUNT(DISTINCT fis_domain) AS fis_domains_used
                    FROM links
                """)
                row = cur.fetchone()

                # Top URL domains
                cur.execute("""
                    SELECT domain, COUNT(*) AS cnt
                    FROM links WHERE domain IS NOT NULL
                    GROUP BY domain ORDER BY cnt DESC LIMIT 10
                """)
                top_domains = [dict(r) for r in cur.fetchall()]

                return {**dict(row), "top_domains": top_domains}

    def _classify(self, text: str) -> dict:
        """Run FIS NLP classification on text content."""
        try:
            from fis.nlp.engines import YakeEngine, text_to_slug
            from fis.nlp.classifier import FISClassifier

            yake = YakeEngine()
            keywords = yake.extract(text)
            classifier = FISClassifier()
            result = classifier.classify(text, keywords, [])
            slug = text_to_slug(keywords, 20)

            return {
                "domain": result.get("domain"),
                "subjects": result.get("subjects"),
                "slug": slug,
                "keywords": [k["keyword"] for k in keywords[:10]],
                "confidence": result.get("confidence", 0),
            }
        except Exception as e:
            log.error("Classification failed: %s", e)
            return {}

    def _content_exists(self, content_hash: str) -> dict | None:
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT link_id, url FROM links WHERE content_hash = %s LIMIT 1",
                    (content_hash,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
