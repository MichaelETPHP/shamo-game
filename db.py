"""
db.py — PostgreSQL connection pool for SHAMO (shamo schema).

Uses DATABASE_URL from .env.
All connections automatically set search_path = shamo so every
unqualified table name resolves to the shamo schema.

Public API
----------
  get_conn()       → psycopg2 connection (caller must close / use as context manager)
  execute(sql, params, fetch)  → run a query, optionally return rows as list[dict]
  pool             → the underlying psycopg2.pool.ThreadedConnectionPool (for advanced use)
"""
import logging
import os
from contextlib import contextmanager
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    import psycopg2
    from psycopg2 import pool as _pg_pool
    from psycopg2.extras import RealDictCursor
    _PSYCOPG2_OK = True
except ImportError:
    psycopg2 = None
    _pg_pool = None
    RealDictCursor = None
    _PSYCOPG2_OK = False
    logger.warning("psycopg2 not installed — DB pool disabled. Run: pip install psycopg2-binary")

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL: str = (os.getenv("DATABASE_URL") or "").strip()
DB_SCHEMA:    str = (os.getenv("DB_SCHEMA") or "shamo").strip()

# ── Connection pool (lazy-initialised) ───────────────────────────────────────
_pool: Optional[Any] = None   # psycopg2.pool.ThreadedConnectionPool


def _init_pool() -> None:
    """Create the connection pool once (thread-safe lazy init)."""
    global _pool
    if _pool is not None:
        return
    if not _PSYCOPG2_OK:
        raise RuntimeError("psycopg2 not installed")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set in .env")

    _pool = _pg_pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=20,
        dsn=DATABASE_URL,
        options=f"-c search_path={DB_SCHEMA},public",   # ← always use shamo schema
    )
    logger.info("DB pool created [schema=%s] → %s", DB_SCHEMA,
                DATABASE_URL.split("@")[-1])   # log host only, no password


def get_pool():
    """Return the connection pool, initialising it if needed."""
    if _pool is None:
        _init_pool()
    return _pool


@contextmanager
def get_conn():
    """
    Context manager: borrow a connection from the pool.

    Usage:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users LIMIT 1")
                rows = cur.fetchall()
    """
    p = get_pool()
    conn = p.getconn()
    # Ensure search_path is set even if pool option wasn't honoured
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}, public")
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def execute(
    sql: str,
    params: tuple = (),
    fetch: str = "none",   # "none" | "one" | "all"
    commit: bool = True,
) -> Any:
    """
    Run a parameterised SQL query.

    Parameters
    ----------
    sql    : SQL string with %s placeholders
    params : tuple of values
    fetch  : "none" → None, "one" → dict | None, "all" → list[dict]
    commit : commit after execution (True for INSERT/UPDATE/DELETE)

    Returns
    -------
    Depends on fetch:  None / dict / list[dict]
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            result = None
            if fetch == "one":
                row = cur.fetchone()
                result = dict(row) if row else None
            elif fetch == "all":
                rows = cur.fetchall()
                result = [dict(r) for r in rows] if rows else []
            if commit:
                conn.commit()
            return result


def health_check() -> dict:
    """Return DB connectivity status — used by /api/health."""
    try:
        row = execute("SELECT current_database() AS db, current_schema() AS schema",
                      fetch="one", commit=False)
        return {"status": "ok", "db": row.get("db"), "schema": row.get("schema")}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── Legacy shim (kept so old imports don't break) ─────────────────────────────
def init_players_table() -> None:
    """Legacy no-op — table exists in shamo schema via migrations."""
    pass


def register_player(tg_user_id: int, phone: str, username: str, full_name: str) -> bool:
    """Legacy shim — upsert a user row in shamo.users."""
    try:
        execute(
            """
            INSERT INTO users (telegram_id, phone_number, telegram_username, first_name, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (telegram_id) DO UPDATE SET
              phone_number      = COALESCE(EXCLUDED.phone_number, users.phone_number),
              telegram_username = COALESCE(EXCLUDED.telegram_username, users.telegram_username),
              first_name        = COALESCE(EXCLUDED.first_name, users.first_name),
              updated_at        = NOW()
            """,
            (tg_user_id, phone or None, username or None, full_name or None),
        )
        return True
    except Exception as e:
        logger.warning("register_player failed: %s", e)
        return False
