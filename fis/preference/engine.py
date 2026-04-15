"""Preference Engine — explicit feedback on links and files.

This is the "I like this / I don't like this" layer that lets you
explicitly train BIL on your taste, instead of relying solely on
implicit behavioral signals.

Usage:
    from fis.preference.engine import PreferenceEngine

    pe = PreferenceEngine()
    pe.like(link_id=42, tags=["theophysics", "consciousness"])
    pe.dislike(link_id=99, note="off-topic clickbait")
    pe.rate(link_id=55, score=8)

    profile = pe.taste_profile()
    suggestions = pe.get_unrated(limit=10)
"""

from datetime import datetime

from fis.db.models import _db
from fis.log import get_logger

log = get_logger("preference")


class PreferenceEngine:
    """Explicit preference signals for links and files."""

    # --- Core actions ---

    def like(self, link_id: int = None, file_id: int = None,
             tags: list[str] = None, note: str = None) -> dict:
        """Record a 'like' preference."""
        return self._record("like", 1.0, link_id, file_id, tags, note)

    def dislike(self, link_id: int = None, file_id: int = None,
                tags: list[str] = None, note: str = None) -> dict:
        """Record a 'dislike' preference."""
        return self._record("dislike", 0.0, link_id, file_id, tags, note)

    def rate(self, score: int, link_id: int = None, file_id: int = None,
             tags: list[str] = None, note: str = None) -> dict:
        """Record a numeric rating (1-10 scale, stored as 0.1-1.0)."""
        if not (1 <= score <= 10):
            raise ValueError("Score must be 1-10")
        normalized = score / 10.0
        return self._record("rate", normalized, link_id, file_id, tags, note)

    def _record(self, action: str, score: float, link_id: int = None,
                file_id: int = None, tags: list[str] = None,
                note: str = None) -> dict:
        """Insert a preference record and update taste profiles."""
        if not link_id and not file_id:
            raise ValueError("Either link_id or file_id is required")

        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO preferences (link_id, file_id, action, score, tags, note)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (link_id, file_id, action, score, tags, note),
                )
                conn.commit()
                pref = dict(cur.fetchone())

        log.info("Preference: %s (score=%.1f) link=%s file=%s",
                 action, score, link_id, file_id)

        # Update taste profiles in background
        self._update_profiles(pref)

        return pref

    # --- Query preferences ---

    def get_preferences(self, link_id: int = None, file_id: int = None,
                        action: str = None, limit: int = 50) -> list[dict]:
        """Retrieve preference history with optional filters."""
        conditions = []
        params = []

        if link_id:
            conditions.append("p.link_id = %s")
            params.append(link_id)
        if file_id:
            conditions.append("p.file_id = %s")
            params.append(file_id)
        if action:
            conditions.append("p.action = %s")
            params.append(action)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT p.*,
                           l.url, l.title, l.domain AS url_domain,
                           l.fis_domain, l.slug AS link_slug
                    FROM preferences p
                    LEFT JOIN links l ON p.link_id = l.link_id
                    {where}
                    ORDER BY p.created_at DESC
                    LIMIT %s
                    """,
                    params + [limit],
                )
                return [dict(r) for r in cur.fetchall()]

    def get_unrated(self, limit: int = 20) -> list[dict]:
        """Get links that haven't been rated yet — the review queue."""
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT l.*
                    FROM links l
                    WHERE NOT EXISTS (
                        SELECT 1 FROM preferences p WHERE p.link_id = l.link_id
                    )
                    ORDER BY l.created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return [dict(r) for r in cur.fetchall()]

    def get_unfed(self, limit: int = 100) -> list[dict]:
        """Get preferences that haven't been fed to BIL yet."""
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.*,
                           l.url, l.domain AS url_domain, l.fis_domain,
                           l.subject_codes, l.keywords, l.slug AS link_slug,
                           l.confidence AS link_confidence
                    FROM preferences p
                    LEFT JOIN links l ON p.link_id = l.link_id
                    WHERE p.fed_to_bil = FALSE
                    ORDER BY p.created_at ASC
                    LIMIT %s
                    """,
                    (limit,),
                )
                return [dict(r) for r in cur.fetchall()]

    def mark_fed(self, pref_ids: list[int]):
        """Mark preferences as fed to BIL after a training cycle."""
        if not pref_ids:
            return
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE preferences SET fed_to_bil = TRUE WHERE pref_id = ANY(%s)",
                    (pref_ids,),
                )
                conn.commit()
        log.info("Marked %d preferences as fed to BIL", len(pref_ids))

    # --- Taste profile ---

    def taste_profile(self) -> dict:
        """Build the aggregated taste profile from all preferences.

        Returns a structured profile showing what you like/dislike
        across domains, subjects, keywords, and URL domains.
        """
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dimension, dimension_key, like_count, dislike_count,
                           avg_score, total_signals
                    FROM taste_profiles
                    ORDER BY total_signals DESC
                """)
                rows = cur.fetchall()

        profile = {
            "domains": {},
            "subjects": {},
            "keywords": {},
            "url_domains": {},
            "summary": {},
        }

        for row in rows:
            dim = row["dimension"]
            key = row["dimension_key"]
            entry = {
                "likes": row["like_count"],
                "dislikes": row["dislike_count"],
                "avg_score": round(row["avg_score"], 3),
                "total": row["total_signals"],
                "ratio": round(row["like_count"] / max(row["total_signals"], 1), 3),
            }

            if dim == "domain":
                profile["domains"][key] = entry
            elif dim == "subject":
                profile["subjects"][key] = entry
            elif dim == "keyword":
                profile["keywords"][key] = entry
            elif dim == "url_domain":
                profile["url_domains"][key] = entry

        # Summary stats
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total_prefs,
                        COUNT(*) FILTER (WHERE action = 'like') AS likes,
                        COUNT(*) FILTER (WHERE action = 'dislike') AS dislikes,
                        COUNT(*) FILTER (WHERE action = 'rate') AS ratings,
                        AVG(score) AS avg_score,
                        COUNT(*) FILTER (WHERE fed_to_bil = TRUE) AS fed_to_bil
                    FROM preferences
                """)
                profile["summary"] = dict(cur.fetchone())

        return profile

    def taste_vector(self) -> dict:
        """Return a compact taste vector suitable for BIL feature injection.

        This is what gets fed into BIL predictions as context — a snapshot
        of what you explicitly like, distilled into numeric features.
        """
        with _db() as conn:
            with conn.cursor() as cur:
                # Top liked domains
                cur.execute("""
                    SELECT dimension_key, avg_score
                    FROM taste_profiles
                    WHERE dimension = 'domain'
                    ORDER BY avg_score DESC
                    LIMIT 5
                """)
                top_domains = {r["dimension_key"]: r["avg_score"] for r in cur.fetchall()}

                # Top liked subjects
                cur.execute("""
                    SELECT dimension_key, avg_score
                    FROM taste_profiles
                    WHERE dimension = 'subject'
                    ORDER BY avg_score DESC
                    LIMIT 10
                """)
                top_subjects = {r["dimension_key"]: r["avg_score"] for r in cur.fetchall()}

                # Top liked keywords
                cur.execute("""
                    SELECT dimension_key, avg_score
                    FROM taste_profiles
                    WHERE dimension = 'keyword'
                    ORDER BY avg_score DESC
                    LIMIT 15
                """)
                top_keywords = {r["dimension_key"]: r["avg_score"] for r in cur.fetchall()}

        return {
            "top_domains": top_domains,
            "top_subjects": top_subjects,
            "top_keywords": top_keywords,
        }

    # --- Profile update ---

    def _update_profiles(self, pref: dict):
        """Update taste_profiles table based on a new preference."""
        # Resolve the link/file metadata to extract dimensions
        dimensions = self._extract_dimensions(pref)

        with _db() as conn:
            with conn.cursor() as cur:
                for dim, key in dimensions:
                    cur.execute(
                        """
                        INSERT INTO taste_profiles (dimension, dimension_key,
                            like_count, dislike_count, avg_score, total_signals, last_updated)
                        VALUES (%s, %s, %s, %s, %s, 1, NOW())
                        ON CONFLICT (dimension, dimension_key) DO UPDATE SET
                            like_count = taste_profiles.like_count + EXCLUDED.like_count,
                            dislike_count = taste_profiles.dislike_count + EXCLUDED.dislike_count,
                            avg_score = (taste_profiles.avg_score * taste_profiles.total_signals
                                         + EXCLUDED.avg_score)
                                        / (taste_profiles.total_signals + 1),
                            total_signals = taste_profiles.total_signals + 1,
                            last_updated = NOW()
                        """,
                        (
                            dim, key,
                            1 if pref["action"] == "like" else 0,
                            1 if pref["action"] == "dislike" else 0,
                            pref["score"],
                        ),
                    )
                conn.commit()

    def _extract_dimensions(self, pref: dict) -> list[tuple[str, str]]:
        """Extract (dimension, key) pairs from a preference for profile updates."""
        dims = []

        if pref.get("link_id"):
            with _db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT domain, fis_domain, subject_codes, keywords FROM links WHERE link_id = %s",
                        (pref["link_id"],),
                    )
                    link = cur.fetchone()

            if link:
                if link.get("domain"):
                    dims.append(("url_domain", link["domain"]))
                if link.get("fis_domain"):
                    dims.append(("domain", link["fis_domain"]))
                for sc in (link.get("subject_codes") or []):
                    dims.append(("subject", sc))
                for kw in (link.get("keywords") or [])[:5]:
                    dims.append(("keyword", kw.lower()))

        if pref.get("file_id"):
            with _db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT domain, subject_codes, slug FROM files WHERE file_id = %s",
                        (pref["file_id"],),
                    )
                    f = cur.fetchone()

            if f:
                if f.get("domain"):
                    dims.append(("domain", f["domain"]))
                for sc in (f.get("subject_codes") or []):
                    dims.append(("subject", sc))

        # Also index any user-provided tags
        for tag in (pref.get("tags") or []):
            dims.append(("keyword", tag.lower()))

        return dims

    # --- Stats ---

    def stats(self) -> dict:
        """Preference stats summary."""
        with _db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE action = 'like') AS likes,
                        COUNT(*) FILTER (WHERE action = 'dislike') AS dislikes,
                        COUNT(*) FILTER (WHERE action = 'rate') AS ratings,
                        COUNT(*) FILTER (WHERE fed_to_bil) AS fed_to_bil,
                        COUNT(*) FILTER (WHERE NOT fed_to_bil) AS pending_bil,
                        AVG(score) FILTER (WHERE action = 'rate') AS avg_rating
                    FROM preferences
                """)
                return dict(cur.fetchone())
