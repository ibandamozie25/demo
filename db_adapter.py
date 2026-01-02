
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass

# Backends
import sqlite3
import mysql.connector
from mysql.connector import pooling


# ---------------------------
# Env / Config
# ---------------------------
DB_BACKEND = (os.getenv("DB_BACKEND") or "sqlite").strip().lower()

# SQLite (path already in your config.py; fallback here just in case)
SQLITE_PATH = os.getenv("SQLITE_PATH", os.path.join(os.getcwd(), "school.db"))

# MySQL: use either split vars or a MYSQL_URL (mysql+mysqlconnector://user:pass@host:3306/db)
MYSQL_URL = os.getenv("MYSQL_URL", "")
MYSQL_HOST = os.getenv("MYSQL_HOST", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306") or 3306)
MYSQL_DB = os.getenv("MYSQL_DB", "")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "")

if MYSQL_URL and not (MYSQL_HOST and MYSQL_DB and MYSQL_USER):
    # crude parser for mysql+...://user:pass@host:port/db
    # allows mysql:// and mysql+mysqlconnector://
    import urllib.parse as _up
    u = MYSQL_URL.replace("mysql+mysqlconnector://", "mysql://")
    p = _up.urlparse(u)
    MYSQL_USER = p.username or MYSQL_USER
    MYSQL_PASS = p.password or MYSQL_PASS
    MYSQL_HOST = p.hostname or MYSQL_HOST
    MYSQL_PORT = p.port or MYSQL_PORT
    MYSQL_DB = (p.path or "/").lstrip("/") or MYSQL_DB


# ---------------------------
# Row / Cursor wrappers
# ---------------------------
class Row(dict):
    """A dict-like row that also supports numeric indexing like sqlite3.Row."""
    __slots__ = ("_ord",)
    def __init__(self, cols, values):
        super().__init__(zip(cols, values))
        self._ord = list(cols)
    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(self._ord[key])
        return super().__getitem__(key)


class CursorWrapper:
    """Wrap a DB-API cursor to return sqlite-like rows for MySQL; pass-through for sqlite."""
    def __init__(self, cur, backend: str):
        self._cur = cur
        self._backend = backend
        self._cols = None

    def execute(self, sql, params=()):
        self._cur.execute(sql, params)
        # Capture columns for MySQL dict building
        try:
            self._cols = getattr(self._cur, "column_names", None)
        except Exception:
            self._cols = None
        return self

    def fetchone(self):
        if self._backend == "mysql":
            row = self._cur.fetchone()
            if row is None:
                return None
            cols = self._cols or [d[0] for d in self._cur.description]
            return Row(cols, row)
        else:
            return self._cur.fetchone()

    def fetchall(self):
        if self._backend == "mysql":
            rows = self._cur.fetchall()
            if not rows:
                return []
            cols = self._cols or [d[0] for d in self._cur.description]
            return [Row(cols, r) for r in rows]
        else:
            return self._cur.fetchall()

    @property
    def lastrowid(self):
        return self._cur.lastrowid

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass


class ConnWrapper:
    """Connection wrapper with a sqlite-like API, plus light SQL rewriting for MySQL."""
    def __init__(self, conn, backend: str):
        self._conn = conn
        self._backend = backend
        # mimic sqlite behavior
        self.row_factory = None

        if backend == "sqlite":
            try:
                self._conn.execute("PRAGMA foreign_keys = ON;")
            except Exception:
                pass

    # --- main API your code uses ---
    def execute(self, sql, params=()):
        sql, params = self._rewrite(sql, params)
        cur = self._conn.cursor()
        if self._backend == "mysql":
            # With MySQL we want positional %s placeholders
            cur.execute(sql, params)
            return CursorWrapper(cur, "mysql")
        else:
            cur.execute(sql, params)
            return cur

    def cursor(self):
        cur = self._conn.cursor()
        if self._backend == "mysql":
            return CursorWrapper(cur, "mysql")
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    # --- internal: rewrite SQLite-ish SQL to MySQL when needed ---
    def _rewrite(self, sql: str, params):
        if self._backend != "mysql":
            return sql, params

        s = sql.strip()
        # 1) PRAGMA table_info(<table>)
        m = re.match(r"PRAGMA\s+table_info\(\s*([`\"\[]?)([\w]+)\1\s*\)\s*;?$", s, re.I)
        if m:
            table = m.group(2)
            q = """
                SELECT
                    ORDINAL_POSITION-1 AS cid,
                    COLUMN_NAME AS name,
                    DATA_TYPE AS type,
                    CASE WHEN IS_NULLABLE='NO' THEN 1 ELSE 0 END AS notnull,
                    COLUMN_DEFAULT AS dflt_value,
                    CASE WHEN COLUMN_KEY='PRI' THEN 1 ELSE 0 END AS pk
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            # we’ll return rows that look a lot like sqlite’s pragma result
            cur = self._conn.cursor()
            cur.execute(q, (table,))
            # Build sqlite-like rows as tuples to keep your existing code happy
            # (many of your uses only grab [1] for column name)
            rows = []
            for cid, name, typ, notnull, dflt, pk in cur.fetchall():
                rows.append((cid, name, typ, notnull, dflt, pk))
            # create a fake cursor-like object
            return _ListCursor(rows), ()

        # 2) SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1
        if "sqlite_master" in s:
            # crude detection; replace with INFORMATION_SCHEMA.TABLES
            # keep LIMIT 1 logic the same
            # Note: original SQL used '?', we already swap placeholders below
            q = """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                LIMIT 1
            """
            # Make sure params is a tuple
            name = params[0] if isinstance(params, (list, tuple)) and params else params
            return q, (name,)

        # 3) Placeholder conversion: '?' -> '%s'
        # Only replace literal bind markers, not '?' in strings.
        # A simple safe approach: if params is non-empty and we see '?', swap all.
        if params and "?" in s:
            # naive but effective: number of ? must match number of params in your code
            s = s.replace("?", "%s")

        return s, params


class _ListCursor:
    """Tiny stand-in so PRAGMA rewrite can return .fetchall()/.fetchone()."""
    def __init__(self, rows):
        self._rows = rows
        self._i = 0

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        if self._i >= len(self._rows):
            return None
        row = self._rows[self._i]
        self._i += 1
        return row

    def close(self): # for API symmetry
        pass


# ---------------------------
# Public API
# ---------------------------
_pool = None

def _mysql_pool():
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="school_mgr_pool",
            pool_size=int(os.getenv("MYSQL_POOL_SIZE", "5") or 5),
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            autocommit=False,
        )
    return _pool


def get_db_connection():
    """
    Return a connection wrapper with a sqlite-like API.
    - For SQLite: sqlite3 connection (foreign_keys ON)
    - For MySQL : pooled mysql-connector connection with SQL rewriting
    """
    if DB_BACKEND == "mysql":
        conn = _mysql_pool().get_connection()
        return ConnWrapper(conn, "mysql")

    # default: sqlite
    p = SQLITE_PATH
    os.makedirs(os.path.dirname(p), exist_ok=True)
    conn = sqlite3.connect(p, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    return ConnWrapper(conn, "sqlite")


# ---------------------------
# Optional MySQL helpers (explicit checks)
# ---------------------------
def mysql_table_exists(conn, table: str) -> bool:
    sql = """
      SELECT 1
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
      LIMIT 1
    """
    return bool(conn.execute(sql, (table,)).fetchone())

def mysql_has_column(conn, table: str, column: str) -> bool:
    sql = """
      SELECT 1
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
      LIMIT 1
    """
    return bool(conn.execute(sql, (table, column)).fetchone())

def mysql_columns(conn, table: str) -> list[str]:
    rows = conn.execute(f"SHOW COLUMNS FROM `{table}`").fetchall()
    return [r[0] if not isinstance(r, dict) else r.get("Field") for r in rows]


# ---------------------------
# Context manager (optional)
# ---------------------------
@contextmanager
def db() -> ConnWrapper:
    """with db() as conn: # do stuff"""
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
