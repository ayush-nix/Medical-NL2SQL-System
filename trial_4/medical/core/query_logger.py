"""
Query Logger — Structured JSON logging for audit trail.
Every query gets a full trace: input → agents → SQL → result.
"""
import json
import os
import time
import logging

logger = logging.getLogger("nl2sql.query_logger")

LOG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "query_logs",
)


class QueryLogger:
    """Append-only JSON query log for audit and analysis."""

    def __init__(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        self.log_file = os.path.join(LOG_DIR, "queries.jsonl")

    def log(self, entry: dict):
        """Log a query execution trace."""
        entry["logged_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception as e:
            logger.error(f"Failed to log query: {e}")

    def get_recent(self, n: int = 20) -> list[dict]:
        """Get last N logged queries."""
        try:
            entries = []
            with open(self.log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        entries.append(json.loads(line))
            return entries[-n:]
        except FileNotFoundError:
            return []
        except Exception as e:
            logger.error(f"Failed to read log: {e}")
            return []


query_logger = QueryLogger()
