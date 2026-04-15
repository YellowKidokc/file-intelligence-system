"""Initialize the FIS database schema."""

from pathlib import Path

from fis.db.connection import get_connection


def init_db():
    sql_path = Path(__file__).parent.parent.parent / "sql" / "01_schema.sql"
    sql = sql_path.read_text(encoding="utf-8")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        from fis.log import get_logger
        get_logger("db").info("FIS database schema initialized.")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
