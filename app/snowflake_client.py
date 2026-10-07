import logging
import threading
from typing import Dict, List

import snowflake.connector
from snowflake.connector import errors as sf_errors

from app.config import settings

logger = logging.getLogger("ecombot.snowflake")

_connection = None
_lock = threading.Lock()
_last_healthy = True  # outcome of the most recent query/ping; reported when the connection is busy
LOCK_WAIT_SECONDS = 30

# Snowflake error codes that mean the session is gone and a reconnect will help.
_SESSION_EXPIRED_CODES = {390112, 390114, 390195}
# Per-statement timeout (seconds) for warehouse queries; applied per execute, never to the session.
QUERY_TIMEOUT_SECONDS = int(settings.SNOWFLAKE_QUERY_TIMEOUT_SECONDS)


def _connect():
    return snowflake.connector.connect(
        account=settings.SNOWFLAKE_ACCOUNT,
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD.get_secret_value(),
        role=settings.SNOWFLAKE_ROLE or None,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
        schema=settings.SNOWFLAKE_SCHEMA,
        login_timeout=10,
        network_timeout=30,
        client_session_keep_alive=True,
    )


def _get_connection():
    """Return the shared connection, (re)opening it if missing or closed. Caller holds _lock."""
    global _connection
    if _connection is None or _connection.is_closed():
        _connection = _connect()
    return _connection


def _reset_connection():
    """Close and drop the shared connection so the next call reconnects. Caller holds _lock."""
    global _connection
    if _connection is not None:
        try:
            _connection.close()
        except Exception:
            pass
    _connection = None


def _is_session_expired(exc: Exception) -> bool:
    code = getattr(exc, "errno", None)
    if code in _SESSION_EXPIRED_CODES:
        return True
    return isinstance(exc, (sf_errors.OperationalError, sf_errors.InterfaceError))


def query(sql: str, params=None) -> List[Dict]:
    """Execute a SQL query and return results as a list of dicts.

    Reconnects once only when the failure looks like an expired/broken session;
    real SQL errors are raised immediately.
    """
    global _last_healthy
    if not _lock.acquire(timeout=LOCK_WAIT_SECONDS):
        raise TimeoutError("warehouse busy: could not acquire the connection in time")
    try:
        for attempt in range(2):
            try:
                conn = _get_connection()  # a connect failure is raised as-is (never retried here)
                cursor = conn.cursor()
            except Exception:
                _last_healthy = False
                raise
            try:
                cursor.execute(sql, params, timeout=QUERY_TIMEOUT_SECONDS)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                _last_healthy = True
                return [dict(zip(columns, row)) for row in rows]
            except Exception as exc:
                if attempt == 0 and _is_session_expired(exc):
                    logger.warning("Snowflake session error (%s); reconnecting once", type(exc).__name__)
                    _reset_connection()
                    continue
                _last_healthy = not _is_session_expired(exc)  # SQL errors do not mean the warehouse is down
                raise
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        raise RuntimeError("unreachable")  # pragma: no cover
    finally:
        _lock.release()


def ping(timeout_seconds: float = 5.0) -> bool:
    """Readiness probe: SELECT 1 with a per-statement timeout (the session is not altered).

    If the shared connection is busy serving a query, it is by definition alive, so the
    probe reports healthy without waiting behind it. A connect failure is reported once,
    not retried; an expired session is reconnected once like query().
    """
    global _last_healthy
    if not _lock.acquire(timeout=1.0):
        return _last_healthy  # busy: report the most recent known outcome, not an assumption
    try:
        for attempt in range(2):
            try:
                conn = _get_connection()
                cursor = conn.cursor()
            except Exception as exc:
                logger.warning("Snowflake readiness probe could not connect: %s", type(exc).__name__)
                _last_healthy = False
                return False
            try:
                cursor.execute("SELECT 1", timeout=max(1, int(timeout_seconds)))
                cursor.fetchone()
                _last_healthy = True
                return True
            except Exception as exc:
                if attempt == 0 and _is_session_expired(exc):
                    _reset_connection()
                    continue
                logger.warning("Snowflake readiness probe failed: %s", type(exc).__name__)
                _last_healthy = False
                return False
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        _last_healthy = False
        return False
    finally:
        _lock.release()


def close() -> None:
    """Close the shared connection (called at application shutdown)."""
    with _lock:
        _reset_connection()
