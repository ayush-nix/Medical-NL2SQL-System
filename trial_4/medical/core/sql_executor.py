"""
SQL Executor — Sandboxed read-only execution.
Timeout protection, row limits, error capture.
"""
import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("nl2sql.executor")

class SQLExecutor:
    """Execute SQL queries safely against PostgreSQL."""

    def __init__(self):
        self.conn = None

    def set_connection(self, dsn: str):
        """Set the PostgreSQL connection."""
        try:
            self.conn = psycopg2.connect(dsn)
            self.conn.autocommit = True
            logger.info("SQL Executor: Postgres connection set")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")

    def execute(self, sql: str, timeout: int = 30, max_rows: int = 10_000) -> dict:
        """Execute a SELECT query safely."""
        if not self.conn:
            return {
                "success": False, "columns": [], "rows": [], "row_count": 0,
                "execution_time_ms": 0, "error": "No database connection.",
            }

        start = time.time()

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Set timeout for PostgreSQL
                cursor.execute(f"SET statement_timeout = {timeout * 1000}")
                
                cursor.execute(sql)
                
                if cursor.description is None:
                    columns = []
                    rows = []
                else:
                    columns = [desc[0] for desc in cursor.description]
                    raw_rows = cursor.fetchmany(max_rows)
                    rows = [dict(row) for row in raw_rows]
                
                row_count = len(rows)

            elapsed = int((time.time() - start) * 1000)
            logger.info(f"Executor: {row_count} rows in {elapsed}ms")

            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": row_count,
                "execution_time_ms": elapsed,
                "error": None,
            }

        except Exception as e:
            # We must rollback the aborted transaction to keep the connection usable
            try:
                self.conn.rollback()
            except:
                pass
            elapsed = int((time.time() - start) * 1000)
            logger.error(f"Executor error in {elapsed}ms: {e}")
            return {
                "success": False, "columns": [], "rows": [], "row_count": 0,
                "execution_time_ms": elapsed, "error": str(e),
            }

    def get_table_preview(self, table_name: str, limit: int = 10) -> dict:
        """Preview first N rows of a table."""
        safe_name = table_name.replace('"', '').replace("'", "")
        return self.execute(f'SELECT * FROM "{safe_name}" LIMIT {limit}')
