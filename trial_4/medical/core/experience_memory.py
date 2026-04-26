"""
Agent 6: Experience Memory — Industry-Grade RLAIF Engine.

This is NOT a simple key-value store. It's a pattern-learning system.

Architecture:
  1. MULTI-SIGNAL MATCHING — Rules are matched using 3 separate signals:
     - Query-text BM25 (what did the user ask?)
     - Column-overlap Jaccard (which columns are involved?)
     - Intent-pattern match (RANKING? AGGREGATION? FILTER?)
     Signals are fused via weighted RRF (Reciprocal Rank Fusion).

  2. RULE ABSTRACTION — Rules are stored at 3 levels:
     - L1: Exact pattern (specific query → specific fix)
     - L2: Column pattern (when using column X, do Y)
     - L3: Intent pattern (for RANKING queries, always do Z)
     Higher levels are more generalizable but less precise.

  3. CONFIDENCE SCORING — Each rule has a confidence score:
     - New rule starts at 0.6
     - +0.1 per additional human confirmation
     - +0.15 if rule was auto-applied and user didn't flag the result
     - Max 1.0
     Rules below 0.3 confidence are pruned on save.

  4. COLUMN FINGERPRINTING — Each rule records which columns it
     relates to. When a new query touches those same columns,
     the rule gets priority. This catches "when using temp_2m_celsius
     vs temp_positive" type errors.

Zero embeddings. Zero vector DB. Pure algorithmic pattern matching.
"""
import json
import os
import re
import time
import logging
from collections import Counter
from utils.text_utils import tokenize, bm25_score

logger = logging.getLogger("nl2sql.agent6_memory")

DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "experience_db.json",
)


def _extract_columns_from_sql(sql: str) -> set[str]:
    """Extract column-like identifiers from SQL string."""
    if not sql:
        return set()
    # Remove string literals
    cleaned = re.sub(r"'[^']*'", "", sql)
    # Remove SQL keywords
    keywords = {
        "select", "from", "where", "and", "or", "not", "in", "between",
        "like", "order", "by", "group", "having", "limit", "as", "on",
        "join", "left", "right", "inner", "outer", "case", "when", "then",
        "else", "end", "is", "null", "asc", "desc", "distinct", "count",
        "avg", "sum", "min", "max", "cast", "integer", "real", "text",
        "date", "with", "union", "all", "exists", "table", "create",
        "into", "values", "set", "update", "delete", "insert",
    }
    tokens = re.findall(r'\b([a-z][a-z0-9_]+)\b', cleaned.lower())
    return {t for t in tokens if t not in keywords and len(t) > 2}


def _extract_intent_pattern(query: str) -> str:
    """Classify query into an intent pattern for rule matching."""
    q = query.lower()
    if any(w in q for w in ["top", "highest", "lowest", "best", "worst", "rank"]):
        return "RANKING"
    if any(w in q for w in ["average", "avg", "mean", "count", "total", "sum"]):
        return "AGGREGATION"
    if any(w in q for w in ["each", "per", "group", "by", "distribution", "breakdown"]):
        return "GROUPBY"
    if any(w in q for w in ["higher than", "greater than", "more than", "above", "below"]):
        return "COMPARISON"
    if any(w in q for w in ["trend", "over time", "change", "recent", "latest"]):
        return "TEMPORAL"
    return "FILTER"


def _jaccard_similarity(set_a: set, set_b: set) -> float:
    """Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


class ExperienceMemory:
    """
    Industry-Grade RLAIF Experience Engine.

    Stores abstract rules learned from human corrections.
    Retrieves them via multi-signal fusion when a similar query appears.
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.rules: list[dict] = []
        self._load()

    def _load(self):
        """Load rules from JSON file."""
        if not os.path.exists(self.db_path):
            self.rules = []
            return

        try:
            with open(self.db_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            raw_rules = data if isinstance(data, list) else data.get("rules", [])

            # Migrate old format rules (from trial_1) to new format
            self.rules = []
            for entry in raw_rules:
                self.rules.append(self._ensure_rule_format(entry))

            logger.info(f"Experience Memory: loaded {len(self.rules)} rules")
        except Exception as e:
            logger.warning(f"Could not load experience DB: {e}")
            self.rules = []

    def _ensure_rule_format(self, entry: dict) -> dict:
        """Ensure a rule entry has all required fields (handles migration)."""
        query = entry.get("query", "")
        failed = entry.get("failed_sql", "")
        correct = entry.get("correct_sql", "")
        rule = entry.get("rule", "")

        return {
            "query": query,
            "rule": rule,
            "failed_sql": failed,
            "correct_sql": correct,
            # Pre-computed signals for fast matching
            "query_tokens": entry.get("query_tokens") or tokenize(query),
            "intent_pattern": entry.get("intent_pattern") or _extract_intent_pattern(query),
            "involved_columns": entry.get("involved_columns") or list(
                _extract_columns_from_sql(failed) | _extract_columns_from_sql(correct)
            ),
            # Abstraction level: L1=exact, L2=column, L3=intent
            "level": entry.get("level", "L1"),
            # Confidence: 0.0–1.0
            "confidence": entry.get("confidence", 0.6),
            "apply_count": entry.get("apply_count", 0),
            "success_count": entry.get("success_count", 0),
            "created_at": entry.get("created_at", time.strftime("%Y-%m-%d %H:%M:%S")),
        }

    def _save(self):
        """Persist rules to JSON. Prune low-confidence + enforce capacity cap."""
        # Prune rules with very low confidence
        self.rules = [r for r in self.rules if r.get("confidence", 0) >= 0.25]
        
        # ANTI-BIAS: Cap at 50 rules max. Evict lowest-confidence first.
        # WHY 50: At 3 rules retrieved per query × ~150 tokens each = 450 tokens.
        # With 50 total rules, retrieval remains fast. Context budget stays under
        # 5% of 8K window. Prevents unbounded growth that causes context overflow.
        MAX_RULES = 50
        if len(self.rules) > MAX_RULES:
            self.rules.sort(key=lambda r: r.get("confidence", 0), reverse=True)
            self.rules = self.rules[:MAX_RULES]
            logger.info(f"Experience Memory: capped at {MAX_RULES} rules (evicted lowest confidence)")

        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            with open(self.db_path, "w", encoding="utf-8") as f:
                json.dump(self.rules, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Could not save experience DB: {e}")

    # ── LEARNING ─────────────────────────────────────────────

    def add_rule(self, original_query: str, failed_sql: str,
                 correct_sql: str, rule_text: str):
        """
        Add a new learned rule from human correction.

        Checks for duplicate/overlapping rules first.
        If a similar rule exists, boosts its confidence instead of duplicating.
        """
        # Check for duplicate (>70% query token overlap)
        new_tokens = set(tokenize(original_query))
        for existing in self.rules:
            existing_tokens = set(existing.get("query_tokens", []))
            overlap = _jaccard_similarity(new_tokens, existing_tokens)
            if overlap > 0.7:
                # Boost existing rule instead of adding duplicate
                existing["confidence"] = min(existing.get("confidence", 0.6) + 0.1, 1.0)
                existing["apply_count"] = existing.get("apply_count", 0) + 1
                # Update rule text if new one is longer/better
                if len(rule_text) > len(existing.get("rule", "")):
                    existing["rule"] = rule_text
                logger.info(f"Experience Memory: boosted existing rule (confidence={existing['confidence']:.2f})")
                self._save()
                return

        # Extract involved columns from both the failed and correct SQL
        failed_cols = _extract_columns_from_sql(failed_sql)
        correct_cols = _extract_columns_from_sql(correct_sql)
        involved = failed_cols | correct_cols

        entry = {
            "query": original_query,
            "rule": rule_text,
            "failed_sql": failed_sql,
            "correct_sql": correct_sql,
            "query_tokens": tokenize(original_query),
            "intent_pattern": _extract_intent_pattern(original_query),
            "involved_columns": list(involved),
            "level": self._determine_level(rule_text, involved),
            "confidence": self._starting_confidence(self._determine_level(rule_text, involved)),
            "apply_count": 0,
            "success_count": 0,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        self.rules.append(entry)
        self._save()
        logger.info(
            f"Experience Memory: new rule added "
            f"(level={entry['level']}, cols={len(involved)}, total={len(self.rules)})"
        )

    def _determine_level(self, rule_text: str, involved_columns: set) -> str:
        """Determine abstraction level of a rule."""
        rule_lower = rule_text.lower()
        # L3: Intent-level rules (very general)
        if any(kw in rule_lower for kw in ["always", "never", "for all", "every query"]):
            return "L3"
        # L2: Column-level rules (mentions specific columns)
        if len(involved_columns) <= 3 and any(
            col in rule_lower for col in involved_columns
        ):
            return "L2"
        # L1: Exact pattern (default)
        return "L1"

    def _starting_confidence(self, level: str) -> float:
        """Starting confidence varies by abstraction level.
        
        WHY: L3 (generalized) rules like 'for RANKING, always ORDER BY DESC'
        can cause bias if they start with the same confidence as specific rules.
        By starting L3 at 0.35 (just above prune threshold), a generalized rule
        must receive 3+ human confirmations before it gains meaningful influence.
        This prevents one bad correction from creating a system-wide biased rule.
        """
        if level == "L3":
            return 0.35  # Must prove itself: 0.35 + 3×0.1 = 0.65 after 3 confirmations
        if level == "L2":
            return 0.50  # Column-level rules: moderate starting trust
        return 0.60  # L1 exact patterns: highest starting trust

    def record_success(self, query: str):
        """Record that a rule was applied and the result was NOT flagged as wrong."""
        query_tokens = set(tokenize(query))
        for rule in self.rules:
            rule_tokens = set(rule.get("query_tokens", []))
            if _jaccard_similarity(query_tokens, rule_tokens) > 0.5:
                rule["success_count"] = rule.get("success_count", 0) + 1
                # Boost confidence for successful applications
                rule["confidence"] = min(rule.get("confidence", 0.6) + 0.05, 1.0)

    # ── RETRIEVAL ────────────────────────────────────────────

    def retrieve_rules(self, query: str, query_columns: list[str] = None,
                       top_k: int = 3, min_confidence: float = 0.3) -> list[dict]:
        """
        Retrieve relevant rules using Multi-Signal RRF Fusion.

        3 signals:
          Signal 1: Query-text BM25 (how similar is the question?)
          Signal 2: Column-overlap Jaccard (do the queries touch same columns?)
          Signal 3: Intent-pattern match (same query type?)

        Returns list of {rule, confidence, match_score} dicts.
        """
        if not self.rules:
            return []

        query_tokens = tokenize(query)
        query_col_set = set(c.lower() for c in (query_columns or []))
        query_intent = _extract_intent_pattern(query)

        candidates = []

        for rule in self.rules:
            if rule.get("confidence", 0) < min_confidence:
                continue

            # Signal 1: Query BM25
            stored_tokens = rule.get("query_tokens") or tokenize(rule.get("query", ""))
            bm25 = bm25_score(query_tokens, stored_tokens)

            # Signal 2: Column Jaccard
            rule_cols = set(rule.get("involved_columns", []))
            col_overlap = _jaccard_similarity(query_col_set, rule_cols) if query_col_set else 0.0

            # Signal 3: Intent match (binary boost)
            intent_match = 1.0 if rule.get("intent_pattern") == query_intent else 0.0

            # RRF Fusion: weighted combination
            # Weights: BM25 (0.5) + Column overlap (0.3) + Intent (0.2)
            combined = (bm25 * 0.5) + (col_overlap * 0.3) + (intent_match * 0.2)

            # Confidence multiplier
            combined *= rule.get("confidence", 0.6)

            # Level bonus: L2 gets small boost for generalizability
            # L3 gets PENALTY (0.9x) — generalized rules should NOT overpower specific ones
            # WHY: L3 rules like 'always ORDER BY DESC' can cause system-wide bias.
            # By penalizing them, they only get used when the other signals (BM25,
            # column overlap) are very strong, meaning the query is genuinely similar.
            level = rule.get("level", "L1")
            if level == "L2":
                combined *= 1.1
            elif level == "L3":
                combined *= 0.9  # Penalty, not bonus

            if combined > 0.05:  # Minimum score threshold
                candidates.append({
                    "rule": rule.get("rule", ""),
                    "confidence": rule.get("confidence", 0.6),
                    "match_score": round(combined, 4),
                    "level": level,
                    "signals": {
                        "bm25": round(bm25, 3),
                        "column_overlap": round(col_overlap, 3),
                        "intent_match": intent_match,
                    },
                })

        # Sort by match score descending
        candidates.sort(key=lambda x: x["match_score"], reverse=True)

        results = candidates[:top_k]
        if results:
            logger.info(
                f"RLAIF retrieved {len(results)} rules "
                f"(best_score={results[0]['match_score']}, level={results[0]['level']})"
            )

        return results

    def retrieve_rule(self, query: str, query_columns: list[str] = None) -> str:
        """
        Retrieve rules as a formatted string for injection into SQL gen prompt.

        Returns empty string if no relevant rules found.
        """
        matched = self.retrieve_rules(query, query_columns, top_k=2, min_confidence=0.3)
        if not matched:
            return ""

        lines = []
        for m in matched:
            conf = m["confidence"]
            level = m["level"]
            lines.append(f"- [{level}, confidence={conf:.1f}] {m['rule']}")

        # Track that rules were applied
        for rule_entry in self.rules:
            for m in matched:
                if rule_entry.get("rule") == m["rule"]:
                    rule_entry["apply_count"] = rule_entry.get("apply_count", 0) + 1

        return "\n".join(lines)

    # ── DIAGNOSTICS ──────────────────────────────────────────

    def get_stats(self) -> dict:
        """Get memory statistics."""
        if not self.rules:
            return {"total_rules": 0, "db_path": self.db_path}

        levels = Counter(r.get("level", "L1") for r in self.rules)
        avg_conf = sum(r.get("confidence", 0) for r in self.rules) / len(self.rules)
        total_applied = sum(r.get("apply_count", 0) for r in self.rules)

        return {
            "total_rules": len(self.rules),
            "levels": dict(levels),
            "avg_confidence": round(avg_conf, 2),
            "total_applications": total_applied,
            "db_path": self.db_path,
        }


# Singleton
experience_memory = ExperienceMemory()
