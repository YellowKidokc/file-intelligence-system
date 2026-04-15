"""Flexible domain/subject code resolver with in-memory cache.

All code lookups go through this layer. Codes can be resolved by
canonical code OR any alias. Renames update the DB and add the old
code as an alias — filenames are never touched.
"""

from fis.db.models import _db
from fis.log import get_logger

log = get_logger("codes")

# In-memory cache — rebuilt on first access or after mutations
_domain_cache = {}   # code|alias -> canonical code
_subject_cache = {}  # code|alias -> canonical code
_cache_loaded = False


def _load_cache():
    """Load all active codes and aliases into memory dicts."""
    global _domain_cache, _subject_cache, _cache_loaded

    _domain_cache.clear()
    _subject_cache.clear()

    with _db() as conn:
        with conn.cursor() as cur:
            # Domain codes
            cur.execute("SELECT code, aliases FROM domain_codes WHERE is_active = TRUE")
            for row in cur.fetchall():
                code = row["code"].upper()
                _domain_cache[code] = code
                if row.get("aliases"):
                    for alias in row["aliases"]:
                        _domain_cache[alias.upper()] = code

            # Subject codes
            cur.execute("SELECT code, aliases FROM subject_codes WHERE is_active = TRUE")
            for row in cur.fetchall():
                code = row["code"].upper()
                _subject_cache[code] = code
                if row.get("aliases"):
                    for alias in row["aliases"]:
                        _subject_cache[alias.upper()] = code

    _cache_loaded = True


def _ensure_cache():
    if not _cache_loaded:
        _load_cache()


def invalidate_cache():
    """Force cache rebuild on next access."""
    global _cache_loaded
    _cache_loaded = False


def resolve_domain(code_or_alias: str) -> str:
    """Resolve a domain code or alias to its canonical code.

    Returns the input unchanged if no match found.
    """
    _ensure_cache()
    return _domain_cache.get(code_or_alias.upper(), code_or_alias.upper())


def resolve_subject(code_or_alias: str) -> str:
    """Resolve a subject code or alias to its canonical code.

    Returns the input unchanged if no match found.
    """
    _ensure_cache()
    return _subject_cache.get(code_or_alias.upper(), code_or_alias.upper())


def add_domain(code: str, label: str, aliases: list[str] = None, description: str = None):
    """Add a new domain code."""
    code = code.upper()
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO domain_codes (code, label, aliases, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE
                SET label = EXCLUDED.label,
                    aliases = EXCLUDED.aliases,
                    description = EXCLUDED.description
                """,
                (code, label, aliases, description),
            )
        conn.commit()
    invalidate_cache()
    log.info("Added domain: %s = %s", code, label)


def add_subject(code: str, label: str, domain: str, aliases: list[str] = None,
                description: str = None, trigger_words: list[str] = None):
    """Add a new subject code."""
    code = code.upper()
    domain = domain.upper()
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO subject_codes (code, label, domain, parent_domain, aliases,
                                           description, trigger_words, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (code) DO UPDATE
                SET label = EXCLUDED.label,
                    domain = EXCLUDED.domain,
                    parent_domain = EXCLUDED.parent_domain,
                    aliases = EXCLUDED.aliases,
                    description = EXCLUDED.description,
                    trigger_words = EXCLUDED.trigger_words
                """,
                (code, label, domain, domain, aliases, description, trigger_words),
            )
        conn.commit()
    invalidate_cache()
    log.info("Added subject: %s = %s (domain: %s)", code, label, domain)


def rename_code(old_code: str, new_code: str, code_type: str):
    """Rename a code in the DB. Adds old code as alias. Never touches filenames.

    Args:
        old_code: Current code to rename.
        new_code: New canonical code.
        code_type: 'domain' or 'subject'.
    """
    old_code = old_code.upper()
    new_code = new_code.upper()

    if code_type not in ("domain", "subject"):
        raise ValueError("code_type must be 'domain' or 'subject'")

    with _db() as conn:
        with conn.cursor() as cur:
            if code_type == "domain":
                # Get existing row
                cur.execute("SELECT * FROM domain_codes WHERE code = %s", (old_code,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Domain code not found: {old_code}")

                # Build new aliases list including the old code
                existing_aliases = list(row.get("aliases") or [])
                if old_code not in existing_aliases:
                    existing_aliases.append(old_code)

                # Insert new code with old data
                cur.execute(
                    """
                    INSERT INTO domain_codes (code, label, aliases, description, is_active)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (code) DO UPDATE
                    SET label = EXCLUDED.label,
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description
                    """,
                    (new_code, row["label"], existing_aliases, row.get("description")),
                )

                # Deactivate old code
                cur.execute(
                    "UPDATE domain_codes SET is_active = FALSE WHERE code = %s",
                    (old_code,),
                )

                # Update subject_codes that reference the old domain
                cur.execute(
                    "UPDATE subject_codes SET domain = %s, parent_domain = %s WHERE domain = %s",
                    (new_code, new_code, old_code),
                )
                migrated = cur.rowcount

            else:  # subject
                cur.execute("SELECT * FROM subject_codes WHERE code = %s", (old_code,))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Subject code not found: {old_code}")

                existing_aliases = list(row.get("aliases") or [])
                if old_code not in existing_aliases:
                    existing_aliases.append(old_code)

                cur.execute(
                    """
                    INSERT INTO subject_codes (code, label, domain, parent_domain, aliases,
                                               description, trigger_words, is_active, sort_order)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    ON CONFLICT (code) DO UPDATE
                    SET label = EXCLUDED.label,
                        domain = EXCLUDED.domain,
                        parent_domain = EXCLUDED.parent_domain,
                        aliases = EXCLUDED.aliases,
                        description = EXCLUDED.description,
                        trigger_words = EXCLUDED.trigger_words
                    """,
                    (new_code, row["label"], row["domain"], row.get("parent_domain"),
                     existing_aliases, row.get("description"), row.get("trigger_words"),
                     row.get("sort_order", 0)),
                )

                cur.execute(
                    "UPDATE subject_codes SET is_active = FALSE WHERE code = %s",
                    (old_code,),
                )

                # Update references in files table
                cur.execute(
                    """
                    UPDATE files
                    SET subject_codes = array_replace(subject_codes, %s, %s)
                    WHERE %s = ANY(subject_codes)
                    """,
                    (old_code, new_code, old_code),
                )
                migrated = cur.rowcount

            # Log the migration
            cur.execute(
                """
                INSERT INTO code_migrations (old_code, new_code, code_type, migrated_count)
                VALUES (%s, %s, %s, %s)
                """,
                (old_code, new_code, code_type, migrated),
            )

        conn.commit()

    invalidate_cache()
    log.info("Renamed %s code: %s -> %s (%d records migrated)", code_type, old_code, new_code, migrated)


def list_domains(include_inactive: bool = False) -> list:
    """List all domain codes."""
    with _db() as conn:
        with conn.cursor() as cur:
            if include_inactive:
                cur.execute("SELECT * FROM domain_codes ORDER BY code")
            else:
                cur.execute("SELECT * FROM domain_codes WHERE is_active = TRUE ORDER BY code")
            return cur.fetchall()


def list_subjects(domain: str = None, include_inactive: bool = False) -> list:
    """List subject codes, optionally filtered by domain."""
    with _db() as conn:
        with conn.cursor() as cur:
            conditions = []
            params = []

            if not include_inactive:
                conditions.append("is_active = TRUE")
            if domain:
                conditions.append("(domain = %s OR domain = 'ALL')")
                params.append(domain.upper())

            where = ""
            if conditions:
                where = "WHERE " + " AND ".join(conditions)

            cur.execute(
                f"SELECT * FROM subject_codes {where} ORDER BY sort_order, code",
                params,
            )
            return cur.fetchall()
