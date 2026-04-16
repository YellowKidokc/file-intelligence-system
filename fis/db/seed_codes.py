"""Seed subject codes and domain codes into the database."""

from pathlib import Path

from fis.db.connection import get_connection
from fis.log import get_logger

log = get_logger("db")

# Canonical domain codes
DOMAIN_CODES = [
    ("TP", "Theophysics", ["theophysics", "theo"], "Theophysics research and formal papers"),
    ("DT", "Day Trading", ["trading", "day trading"], "Day trading setups, journals, and backtests"),
    ("EV", "Ecommerce", ["ecommerce", "evidence", "ecom"], "Ecommerce and evidence documentation"),
    ("AP", "Apps", ["apps", "applications"], "Application projects and development"),
    ("MD", "Media", ["media", "video", "audio"], "Media files — video, audio, images"),
    ("DC", "Documents", ["documents", "docs"], "General documents and paperwork"),
    ("OB", "Obsidian", ["obsidian", "vault"], "Obsidian vault notes"),
    ("CB", "Codebases", ["codebases", "code", "repos"], "Code repositories and projects"),
    ("SY", "Systems", ["systems", "system", "config"], "System configuration and infrastructure"),
    ("RC", "Recon", ["recon", "reconnaissance", "extraction"], "Recon/extractor pipeline content"),
]


def seed_codes():
    """Seed subject codes from SQL and domain codes from Python constants."""
    sql_dir = Path(__file__).parent.parent.parent / "sql"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Run the flexible codes migration first
            flexible_sql = sql_dir / "03_flexible_codes.sql"
            if flexible_sql.exists():
                cur.execute(flexible_sql.read_text(encoding="utf-8"))
                log.info("Flexible codes schema applied.")

            # Seed domain codes
            for code, label, aliases, description in DOMAIN_CODES:
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
            log.info("Domain codes seeded: %d codes.", len(DOMAIN_CODES))

            # Seed subject codes from SQL
            subject_sql = sql_dir / "02_seed_codes.sql"
            cur.execute(subject_sql.read_text(encoding="utf-8"))
            log.info("Subject codes seeded.")

            # Backfill parent_domain on existing subject codes
            cur.execute(
                "UPDATE subject_codes SET parent_domain = domain WHERE parent_domain IS NULL"
            )

        conn.commit()
        log.info("All codes seeded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    seed_codes()
