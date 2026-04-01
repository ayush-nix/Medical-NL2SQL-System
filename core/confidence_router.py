"""
Confidence-Based Query Router — Adaptive Pipeline Routing

Instead of running every query through the full 7-layer pipeline,
this module classifies query complexity and routes:

  - SIMPLE queries  → Skip Schema Linking, go straight to generation (faster)
  - MODERATE queries → Full pipeline (standard)
  - COMPLEX queries  → Full pipeline + extra context + larger model context window

Reference: AgenticSQL (2025) — adaptive routing reduces latency by 40%
on simple queries while preserving accuracy on complex ones.

SECURITY: This module only classifies. No destructive operations.
"""
import re
import logging

logger = logging.getLogger("nl2sql.router")


class ConfidenceRouter:
    """
    Routes queries to different pipeline depths based on complexity.

    Complexity Signals:
    - Number of tables likely needed (single vs multi)
    - Presence of aggregation keywords (COUNT, SUM, AVG)
    - Temporal indicators (date, year, month, last, recent)
    - Comparison/ranking keywords (most, top, best, worst, highest)
    - Subquery indicators (for each, compared to, percentage of)
    """

    # Keywords that signal different complexity levels
    SIMPLE_SIGNALS = {
        "list", "show", "display", "what is", "what are",
        "get", "find", "tell me",
    }

    AGGREGATION_SIGNALS = {
        "count", "how many", "total", "sum", "average", "avg",
        "maximum", "minimum", "max", "min", "mean",
    }

    TEMPORAL_SIGNALS = {
        "year", "month", "date", "last", "recent", "before",
        "after", "between", "since", "quarter", "week",
    }

    RANKING_SIGNALS = {
        "most", "least", "top", "bottom", "best", "worst",
        "highest", "lowest", "rank", "order", "sort",
    }

    SUBQUERY_SIGNALS = {
        "for each", "per", "compared to", "versus",
        "percentage", "ratio", "proportion", "relative",
        "more than average", "above average", "below average",
    }

    JOIN_SIGNALS = {
        "posted", "transferred", "assigned", "along with",
        "together with", "and their", "with their",
    }

    def classify(self, question: str, num_tables: int = 1) -> dict:
        """
        Classify query complexity.
        Returns routing decision with confidence score.
        """
        q = question.lower().strip()

        # Count complexity signals
        scores = {
            "aggregation": self._count_signals(q, self.AGGREGATION_SIGNALS),
            "temporal": self._count_signals(q, self.TEMPORAL_SIGNALS),
            "ranking": self._count_signals(q, self.RANKING_SIGNALS),
            "subquery": self._count_signals(q, self.SUBQUERY_SIGNALS),
            "join": self._count_signals(q, self.JOIN_SIGNALS),
        }

        total_complexity = sum(scores.values())

        # Factor in number of database tables
        if num_tables > 5:
            total_complexity += 1  # More tables = more routing caution

        # Route decision
        if total_complexity == 0 and self._is_simple(q):
            level = "SIMPLE"
            skip_schema_linking = False  # Always link — fast via domain dict
            use_extended_context = False
            confidence = 0.9
        elif total_complexity <= 2:
            level = "MODERATE"
            skip_schema_linking = False
            use_extended_context = False
            confidence = 0.7
        else:
            level = "COMPLEX"
            skip_schema_linking = False
            use_extended_context = True
            confidence = 0.5

        result = {
            "complexity_level": level,
            "complexity_score": total_complexity,
            "skip_schema_linking": skip_schema_linking,
            "use_extended_context": use_extended_context,
            "initial_confidence": confidence,
            "signals": {k: v for k, v in scores.items() if v > 0},
        }

        logger.info(
            f"Query routed: {level} (score={total_complexity}, "
            f"signals={result['signals']})"
        )
        return result

    def _count_signals(self, query: str, signals: set) -> int:
        """Count how many signal keywords appear in the query."""
        count = 0
        for signal in signals:
            if signal in query:
                count += 1
        return count

    def _is_simple(self, query: str) -> bool:
        """Check if query matches simple patterns."""
        for signal in self.SIMPLE_SIGNALS:
            if query.startswith(signal):
                return True

        # Short queries (< 8 words) are likely simple
        if len(query.split()) < 8:
            return True

        return False


# Singleton
confidence_router = ConfidenceRouter()
