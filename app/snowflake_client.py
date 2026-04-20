from typing import Dict, List, Optional

import snowflake.connector

from app.config import settings

_connection = None


def _get_connection():
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
        )
    return _connection


def query(sql: str, params: Optional[dict] = None) -> List[Dict]:
    """Execute a SQL query and return results as a list of dicts."""
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
    finally:
        cursor.close()
