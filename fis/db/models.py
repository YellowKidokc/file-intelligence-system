"""Database operations for FIS."""

import hashlib
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from fis.db.connection import get_connection


@contextmanager
def _db():
    """Context manager for database connections — guarantees cleanup."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def compute_sha256(file_path: str) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def get_next_sequence_id() -> str:
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT LPAD(NEXTVAL('files_sequence_id_seq')::text, 6, '0') AS next_id")
            row = cur.fetchone()
            return row["next_id"]


def file_exists_by_hash(sha256: str) -> dict | None:
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM files WHERE sha256 = %s LIMIT 1", (sha256,))
            return cur.fetchone()


def file_exists_by_path(file_path: str) -> dict | None:
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM files WHERE file_path = %s LIMIT 1", (file_path,))
            return cur.fetchone()


def insert_file(
    original_name: str,
    file_path: str,
    sha256: str,
    domain: str = None,
    subject_codes: list = None,
    slug: str = None,
    proposed_name: str = None,
    confidence: float = None,
    status: str = "pending",
    sequence_id: str = None,
) -> dict:
    seq_id = sequence_id or get_next_sequence_id()
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files
                    (sequence_id, original_name, proposed_name, file_path,
                     domain, subject_codes, slug, sha256, status, confidence,
                     source_path, classified_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    seq_id, original_name, proposed_name, file_path,
                    domain, subject_codes, slug, sha256, status, confidence,
                    file_path, datetime.now(),
                ),
            )
            conn.commit()
            return cur.fetchone()


def update_file_status(file_id: int, status: str, final_name: str = None):
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE files
                SET status = %s, final_name = %s, updated_at = NOW()
                WHERE file_id = %s
                """,
                (status, final_name, file_id),
            )
            conn.commit()


def insert_tags(file_id: int, tags: list[dict]):
    with _db() as conn:
        with conn.cursor() as cur:
            for tag in tags:
                cur.execute(
                    """
                    INSERT INTO file_tags (file_id, tag, source, confidence)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (file_id, tag["tag"], tag.get("source", "yake"), tag.get("confidence")),
                )
            conn.commit()


def insert_file_with_tags(
    *,
    original_name: str,
    file_path: str,
    sha256: str,
    tags: list[dict],
    domain: str = None,
    subject_codes: list = None,
    slug: str = None,
    proposed_name: str = None,
    confidence: float = None,
    status: str = "pending",
    sequence_id: str = None,
) -> dict:
    seq_id = sequence_id or get_next_sequence_id()
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files
                    (sequence_id, original_name, proposed_name, file_path,
                     domain, subject_codes, slug, sha256, status, confidence,
                     source_path, classified_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    seq_id, original_name, proposed_name, file_path,
                    domain, subject_codes, slug, sha256, status, confidence,
                    file_path, datetime.now(),
                ),
            )
            row = cur.fetchone()

            for tag in tags:
                cur.execute(
                    """
                    INSERT INTO file_tags (file_id, tag, source, confidence)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (row["file_id"], tag["tag"], tag.get("source", "yake"), tag.get("confidence")),
                )

        conn.commit()
        return row


def insert_correction(file_id: int, old: dict, new: dict):
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO corrections
                    (file_id, old_domain, old_subjects, old_slug,
                     new_domain, new_subjects, new_slug)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    file_id,
                    old.get("domain"), old.get("subjects"), old.get("slug"),
                    new.get("domain"), new.get("subjects"), new.get("slug"),
                ),
            )
            conn.commit()


def get_pending_files(limit: int = 50) -> list:
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM files WHERE status = 'pending' ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()


def get_subject_codes(domain: str = None) -> list:
    with _db() as conn:
        with conn.cursor() as cur:
            if domain:
                cur.execute(
                    "SELECT * FROM subject_codes WHERE domain = %s OR domain = 'ALL'",
                    (domain,),
                )
            else:
                cur.execute("SELECT * FROM subject_codes")
            return cur.fetchall()


def search_files(query: str, limit: int = 20) -> list:
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.*, array_agg(t.tag) AS tags
                FROM files f
                LEFT JOIN file_tags t ON f.file_id = t.file_id
                WHERE f.final_name ILIKE %s
                   OR f.slug ILIKE %s
                   OR f.domain ILIKE %s
                   OR t.tag ILIKE %s
                GROUP BY f.file_id
                ORDER BY f.created_at DESC
                LIMIT %s
                """,
                (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit),
            )
            return cur.fetchall()
