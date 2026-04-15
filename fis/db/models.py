"""Database operations for FIS."""

import hashlib
from datetime import datetime
from pathlib import Path

from fis.db.connection import get_connection


def compute_sha256(file_path: str) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def get_next_sequence_id() -> str:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(file_id), 0) + 1 AS next_id FROM files")
            row = cur.fetchone()
            return str(row["next_id"]).zfill(6)
    finally:
        conn.close()


def file_exists_by_hash(sha256: str) -> dict | None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM files WHERE sha256 = %s LIMIT 1", (sha256,))
            return cur.fetchone()
    finally:
        conn.close()


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
) -> dict:
    seq_id = get_next_sequence_id()
    conn = get_connection()
    try:
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
    finally:
        conn.close()


def update_file_status(file_id: int, status: str, final_name: str = None):
    conn = get_connection()
    try:
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
    finally:
        conn.close()


def insert_tags(file_id: int, tags: list[dict]):
    conn = get_connection()
    try:
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
    finally:
        conn.close()


def insert_correction(file_id: int, old: dict, new: dict):
    conn = get_connection()
    try:
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
    finally:
        conn.close()


def get_pending_files(limit: int = 50) -> list:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM files WHERE status = 'pending' ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_subject_codes(domain: str = None) -> list:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if domain:
                cur.execute(
                    "SELECT * FROM subject_codes WHERE domain = %s OR domain = 'ALL'",
                    (domain,),
                )
            else:
                cur.execute("SELECT * FROM subject_codes")
            return cur.fetchall()
    finally:
        conn.close()


def search_files(query: str, limit: int = 20) -> list:
    conn = get_connection()
    try:
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
    finally:
        conn.close()
