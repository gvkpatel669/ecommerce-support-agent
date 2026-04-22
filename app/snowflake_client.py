from typing import Dict, List, Optional

import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from snowflake.connector.errors import BadAuthenticationError

from app.config import settings

_connection = None


def _get_connection():
    """Get or create a Snowflake connection.
    
    This function handles connection creation and invalidation.
    Note: Snowflake session token expiration (error 390114) does not
    mark the connection as closed, so we handle that in query().
    """
    global _connection
    if _connection is None or _connection.is_closed():
        _connection = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            role=settings.SNOWFLAKE_ROLE,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            login_timeout=10,
            network_timeout=30,
        )
    return _connection


def _invalidate_connection():
    """Invalidate the current connection, forcing recreation on next use."""
    global _connection
    _connection = None


def query(sql: str, params=None) -> List[Dict]:
    """Execute a SQL query and return results as a list of dicts."""
    max_retries = 2
    last_error = None
    
    for attempt in range(max_retries):
        conn = _get_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except SnowflakeError as e:
            last_error = e
            # Error 390114 = expired token, 390112 = invalid token
            if e.errno in (390114, 390112) and attempt < max_retries - 1:
                _invalidate_connection()
                continue
            raise
        except BadAuthenticationError as e:
            last_error = e
            if attempt < max_retries - 1:
                _invalidate_connection()
                continue
            raise
        finally:
            cursor.close()
    
    raise last_error
