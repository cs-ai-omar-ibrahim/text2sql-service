from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from psycopg import Connection
from psycopg.errors import OperationalError
from psycopg_pool import ConnectionPool

from app.config import get_settings


@dataclass(frozen=True)
class PoolConfig:
    # Conservative defaults to avoid runaway connections.
    min_size: int = 1
    max_size: int = 10

    # If the pool can't give us a connection fast, fail quickly.
    timeout: float = 5.0

    # Keep connections fresh-ish.
    max_idle: float = 300.0

    # Server-side safety defaults (milliseconds).
    statement_timeout_ms: int = 15_000
    idle_in_transaction_session_timeout_ms: int = 15_000


_pool: Optional[ConnectionPool] = None


def init_db_pool(config: PoolConfig | None = None) -> ConnectionPool:
    global _pool
    if _pool is not None:
        return _pool

    cfg = config or PoolConfig()
    settings = get_settings()

    # psycopg3: set per-connection parameters in options.
    options = " ".join(
        [
            f"-c statement_timeout={cfg.statement_timeout_ms}",
            f"-c idle_in_transaction_session_timeout={cfg.idle_in_transaction_session_timeout_ms}",
            "-c application_name=text2sql-service",
        ]
    )

    _pool = ConnectionPool(
        conninfo=settings.database_url,
        min_size=cfg.min_size,
        max_size=cfg.max_size,
        timeout=cfg.timeout,
        max_idle=cfg.max_idle,
        kwargs={
            "autocommit": True,
            "options": options,
        },
        open=False,
    )
    _pool.open()
    return _pool


def get_db_pool() -> ConnectionPool:
    if _pool is None:
        return init_db_pool()
    return _pool


def close_db_pool() -> None:
    global _pool
    if _pool is None:
        return
    _pool.close()
    _pool = None


def check_db() -> dict:
    """
    Minimal connectivity check. Returns a dict safe for JSON response.
    """
    pool = get_db_pool()
    try:
        with pool.connection() as conn:  # type: Connection
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                one = cur.fetchone()
        return {"ok": True, "result": one[0] if one else None}
    except OperationalError as e:
        return {"ok": False, "error": str(e)}


def list_user_tables() -> dict:
    """
    Return all non-system tables (schema + table name).
    """
    pool = get_db_pool()
    try:
        with pool.connection() as conn:  # type: Connection
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tablename, schemaname
                    FROM pg_catalog.pg_tables
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
                    """
                )
                rows = cur.fetchall() or []
        return {
            "ok": True,
            "tables": [{"schemaname": s, "tablename": t} for (t, s) in rows],
        }
    except OperationalError as e:
        return {"ok": False, "error": str(e)}
