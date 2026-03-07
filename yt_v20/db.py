"""
db.py — Centralized database connection pool for YouTube Pipeline Orchestrator v20

Provides a ThreadedConnectionPool shared across all modules.
Each thread checks out its own connection and returns it when done.

Usage:
    from db import get_conn, release_conn, close_pool, init_pool

Replaces the copy-pasted get_connection() in every module.
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool

from config import get_config

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_pool(cfg: dict | None = None) -> None:
    """
    Initialize (or re-initialize) the connection pool from current config.
    Called once at startup and again whenever config is saved.
    """
    global _pool
    c = cfg or get_config()

    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass

    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        host=c["PG_HOST"],
        port=c["PG_PORT"],
        user=c["PG_USER"],
        password=c["PG_PASSWORD"],
        dbname=c["PG_DB"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_conn():
    """Check out a connection from the pool. Caller must call release_conn() when done."""
    global _pool
    if _pool is None:
        init_pool()
    return _pool.getconn()


def release_conn(conn) -> None:
    """Return a connection to the pool."""
    global _pool
    if _pool and conn:
        _pool.putconn(conn)


def close_pool() -> None:
    """Close all connections — call on app shutdown."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


class PooledConn:
    """
    Context manager for safe pool checkout/return.

    Usage:
        with PooledConn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    def __init__(self):
        self._conn = None

    def __enter__(self):
        self._conn = get_conn()
        self._conn.autocommit = True
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        release_conn(self._conn)
        self._conn = None
        return False
