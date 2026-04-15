"""Database connection manager for FIS."""

import configparser
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

_CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "settings.ini"


def get_config():
    config = configparser.ConfigParser()
    config.read(_CONFIG_PATH)
    return config


def get_connection():
    config = get_config()
    return psycopg2.connect(
        host=config["database"]["host"],
        port=config["database"]["port"],
        dbname=config["database"]["name"],
        user=config["database"]["user"],
        password=config["database"]["password"],
        cursor_factory=RealDictCursor,
    )


def get_cursor():
    conn = get_connection()
    conn.autocommit = True
    return conn, conn.cursor()
