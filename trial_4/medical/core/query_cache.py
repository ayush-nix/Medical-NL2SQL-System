"""
Query Cache — LRU with TTL.
Stores successful query results for instant repeat-query responses.
"""
import time
import logging
from collections import OrderedDict

logger = logging.getLogger("nl2sql.cache")


class QueryCache:
    """Thread-safe LRU cache with TTL expiration."""

    def __init__(self, max_size: int = 500, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl = ttl_seconds
        self._cache: OrderedDict[str, dict] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def _normalize_key(self, question: str) -> str:
        return question.lower().strip()

    def get(self, question: str) -> dict | None:
        key = self._normalize_key(question)
        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry["_ts"] < self.ttl:
                self._hits += 1
                self._cache.move_to_end(key)
                result = {k: v for k, v in entry.items() if k != "_ts"}
                return result
            else:
                del self._cache[key]
        self._misses += 1
        return None

    def put(self, question: str, result: dict):
        key = self._normalize_key(question)
        entry = dict(result)
        entry["_ts"] = time.time()
        self._cache[key] = entry
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

    def clear(self):
        self._cache.clear()
        logger.info("Cache cleared")

    @property
    def stats(self) -> dict:
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / max(self._hits + self._misses, 1), 3),
        }
