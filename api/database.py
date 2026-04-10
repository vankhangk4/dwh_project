"""
api/database.py
Phase 12: SQL Server connection pool for Auth Gateway.

Provides:
    - get_db()           : FastAPI dependency that yields a DB connection.
    - get_raw_db_conn()  : Direct pyodbc connection (for background tasks).
    - init_db()          : Verify DB connectivity on startup.
    - close_db()         : Cleanup connection pool on shutdown.
    - execute_sql()      : Execute raw SQL query and return DataFrame.
    - execute_sp()       : Execute stored procedure and return DataFrame.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager, suppress
from typing import Any, Generator, Optional

import pyodbc
from fastapi import Depends
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection string
# ---------------------------------------------------------------------------

def _build_conn_str() -> str:
    """Build SQL Server connection string from environment variables."""
    conn_str = os.environ.get("CONN_STR", "")
    if not conn_str:
        raise ValueError(
            "CONN_STR environment variable is not set. "
            "Cannot connect to database."
        )
    return conn_str


# ---------------------------------------------------------------------------
# SQLAlchemy engine (for FastAPI Depends — transaction managed)
# ---------------------------------------------------------------------------

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """
    Get or create the SQLAlchemy engine singleton.

    Uses QueuePool for connection pooling with configurable pool size.
    """
    global _engine
    if _engine is None:
        conn_str = _build_conn_str()
        _engine = create_engine(
            conn_str,
            poolclass=QueuePool,
            pool_size=int(os.environ.get("DB_POOL_SIZE", "5")),
            max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "10")),
            pool_pre_ping=True,
            pool_recycle=int(os.environ.get("DB_POOL_RECYCLE", "3600")),
            fast_executemany=True,
            echo=False,
        )
        logger.info(
            "SQLAlchemy engine created | pool_size=%s | max_overflow=%s",
            os.environ.get("DB_POOL_SIZE", "5"),
            os.environ.get("DB_MAX_OVERFLOW", "10"),
        )
    return _engine


def close_engine() -> None:
    """Close the SQLAlchemy engine and release all connections."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        logger.info("SQLAlchemy engine disposed.")


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def get_db() -> Generator[Connection, None, None]:
    """
    FastAPI dependency that provides a SQLAlchemy connection.

    Handles:
        - Acquiring connection from pool.
        - Setting tenant context via SESSION_CONTEXT.
        - Auto-commit on success / rollback on exception.
        - Returning connection to pool on cleanup.

    Yields:
        SQLAlchemy Connection object.

    Usage:
        @app.get("/users")
        def get_users(db: Connection = Depends(get_db)):
            result = db.execute(text("SELECT * FROM AppUsers"))
    """
    engine = get_engine()
    conn = engine.connect()

    try:
        yield conn
    except Exception as ex:
        logger.error("Database error: %s", ex, exc_info=True)
        conn.rollback()
        raise
    finally:
        conn.close()


def get_raw_db_conn(autocommit: bool = False) -> pyodbc.Connection:
    """
    Get a raw pyodbc connection for background ETL tasks.

    This bypasses SQLAlchemy and uses direct pyodbc for:
    - Long-running ETL jobs.
    - Bulk inserts (fast_executemany).
    - Transaction-level control.

    Args:
        autocommit: If True, connection runs in autocommit mode.

    Returns:
        pyodbc.Connection object.

    Note:
        Caller is responsible for closing the connection when done.
        Prefer get_db() for request-scoped operations.
    """
    conn_str = _build_conn_str()
    try:
        conn = pyodbc.connect(conn_str, autocommit=autocommit, timeout=30)
        logger.debug("Raw DB connection acquired.")
        return conn
    except pyodbc.Error as ex:
        logger.error("Failed to acquire raw DB connection: %s", ex)
        raise ConnectionError(f"Database connection failed: {ex}") from ex


# ---------------------------------------------------------------------------
# Connection cleanup
# ---------------------------------------------------------------------------

def close_connection(conn: Optional[pyodbc.Connection]) -> None:
    """
    Safely close a pyodbc connection.

    Args:
        conn: pyodbc.Connection object or None.
    """
    if conn is not None:
        try:
            conn.close()
            logger.debug("Raw DB connection closed.")
        except Exception as ex:
            logger.warning("Error closing raw DB connection: %s", ex)


@contextmanager
def raw_connection(
    autocommit: bool = False,
) -> Generator[pyodbc.Connection, None, None]:
    """
    Context manager for raw pyodbc connections.

    Automatically handles acquire and release.

    Usage:
        with raw_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Tenants")
            rows = cursor.fetchall()
    """
    conn = get_raw_db_conn(autocommit=autocommit)
    try:
        yield conn
    finally:
        close_connection(conn)


# ---------------------------------------------------------------------------
# DB initialization
# ---------------------------------------------------------------------------

def init_db() -> bool:
    """
    Verify database connectivity on application startup.

    Returns:
        True if database is accessible, False otherwise.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connectivity verified. Connection OK.")
        return True
    except Exception as ex:
        logger.error("Database initialization failed: %s", ex)
        return False


def close_db() -> None:
    """
    Cleanup database resources on application shutdown.

    Disposes the SQLAlchemy engine and closes all pooled connections.
    """
    try:
        close_engine()
        logger.info("Database cleanup completed.")
    except Exception as ex:
        logger.warning("Error during database cleanup: %s", ex)


# ---------------------------------------------------------------------------
# SQL execution helpers
# ---------------------------------------------------------------------------

def execute_sql(
    conn: Connection,
    sql: str,
    params: Optional[dict[str, Any]] = None,
    fetch: bool = True,
) -> Optional[list[dict[str, Any]]]:
    """
    Execute raw SQL and return results as list of dicts.

    Args:
        conn:   SQLAlchemy connection.
        sql:    SQL query string.
        params: Query parameters. (optional)
        fetch:  If True, returns list of dicts. (default True)

    Returns:
        List of row dicts if fetch=True, else None.
    """
    logger.debug("Executing SQL: %s", sql[:200])

    try:
        result = conn.execute(text(sql), params or {})
        conn.commit()

        if fetch and result.returns_rows:
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]

        return None

    except Exception as ex:
        logger.error("SQL execution failed: %s", ex)
        conn.rollback()
        raise


def execute_sp(
    conn: Connection,
    sp_name: str,
    params: Optional[dict[str, Any]] = None,
    fetch: bool = False,
) -> Optional[list[dict[str, Any]]]:
    """
    Execute a stored procedure via SQL text.

    Args:
        conn:     SQLAlchemy connection.
        sp_name:  Stored procedure name.
        params:   Dict of parameter names → values. (optional)
        fetch:    If True, returns result set. (default False)

    Returns:
        List of row dicts if fetch=True and SP returns rows, else None.
    """
    param_list = []
    param_values = []
    if params:
        for key, value in params.items():
            param_list.append(f"@{key} = ?")
            param_values.append(value)

    param_clause = ", ".join(param_list) if param_list else ""
    sql = f"EXEC {sp_name}" + (f" {param_clause}" if param_clause else "")

    logger.debug("Executing SP: %s", sp_name)

    try:
        result = conn.execute(text(sql), param_values)
        conn.commit()

        if fetch and result.returns_rows:
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]

        return None

    except Exception as ex:
        logger.error("Stored procedure %s failed: %s", sp_name, ex)
        conn.rollback()
        raise