"""Seed subject codes into the database."""

from pathlib import Path

from fis.db.connection import get_connection


def seed_codes():
    sql_path = Path(__file__).parent.parent.parent / "sql" / "02_seed_codes.sql"
    sql = sql_path.read_text(encoding="utf-8")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Subject codes seeded.")
    finally:
        conn.close()


if __name__ == "__main__":
    seed_codes()
