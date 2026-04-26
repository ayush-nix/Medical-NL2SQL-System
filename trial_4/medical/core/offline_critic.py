"""
Offline Critic — Industry-Grade Rule Deduction Engine.

When a user corrects a wrong SQL, this agent:
1. Diffs the failed SQL vs correct SQL at AST level
2. Identifies the EXACT error category (wrong column, wrong operator, wrong agg, etc.)
3. Deduces an abstract, reusable rule
4. Stores it in Experience Memory with proper fingerprinting

Runs ASYNC — does NOT block the query response.
"""
import re
import logging
from config import MedicalConfig as Config
from models.llm_manager import llm_manager
from core.experience_memory import experience_memory

logger = logging.getLogger("nl2sql.offline_critic")


def _diff_sql_columns(failed_sql: str, correct_sql: str) -> dict:
    """Structural diff between two SQL queries to identify what changed."""
    from core.experience_memory import _extract_columns_from_sql

    failed_cols = _extract_columns_from_sql(failed_sql)
    correct_cols = _extract_columns_from_sql(correct_sql)

    added = correct_cols - failed_cols
    removed = failed_cols - correct_cols
    common = failed_cols & correct_cols

    # Detect operator changes
    failed_lower = failed_sql.lower()
    correct_lower = correct_sql.lower()

    changes = {
        "columns_added": list(added),
        "columns_removed": list(removed),
        "columns_common": list(common),
        "has_agg_change": any(
            (agg in failed_lower) != (agg in correct_lower)
            for agg in ["avg(", "sum(", "count(", "min(", "max("]
        ),
        "has_filter_change": (
            ("where" in failed_lower) != ("where" in correct_lower) or
            failed_lower.count("where") != correct_lower.count("where")
        ),
        "has_order_change": (
            ("order by" in failed_lower) != ("order by" in correct_lower)
        ),
        "has_group_change": (
            ("group by" in failed_lower) != ("group by" in correct_lower)
        ),
    }
    return changes


CRITIC_PROMPT = """You are a senior database engineer reviewing a SQL correction.

ORIGINAL USER QUESTION:
"{question}"

FAILED SQL (WRONG — this was generated but gave incorrect results):
{failed_sql}

CORRECT SQL (RIGHT — this is what the user wanted):
{correct_sql}

STRUCTURAL DIFF:
- Columns added in correction: {cols_added}
- Columns removed from failed: {cols_removed}
- Aggregation changed: {agg_change}
- Filter/WHERE changed: {filter_change}
- ORDER BY changed: {order_change}
- GROUP BY changed: {group_change}

TASK: Extract ONE clear, reusable rule that prevents this specific mistake in future queries.

The rule must be:
1. SPECIFIC enough to be actionable (mention exact column names)
2. GENERAL enough to apply beyond this one query
3. Written as a single sentence starting with "When..."

Example good rules:
- "When user asks about steep areas, filter by slope > 0.5236 (radians), NOT degrees."
- "When computing average risk by elevation, use GROUP BY elevation range with AVG(landslide_probability)."
- "When ranking by landslide risk, ORDER BY landslide_probability DESC."

Output ONLY the rule. No explanation. No prefix.

RULE:"""


class OfflineCritic:
    """Async rule deduction engine with structural SQL diffing."""

    async def diagnose_and_learn(self, user_query: str, failed_sql: str,
                                  correct_sql: str) -> str:
        """
        Analyze a correction, diff the SQL, extract a reusable rule.

        Steps:
        1. Structural diff (columns, operators, agg, filters)
        2. LLM produces abstract rule
        3. Store in Experience Memory with fingerprinting
        """
        # Step 1: Structural diff
        diff = _diff_sql_columns(failed_sql, correct_sql)

        # Step 2: LLM rule deduction
        prompt = CRITIC_PROMPT.format(
            question=user_query,
            failed_sql=failed_sql,
            correct_sql=correct_sql,
            cols_added=", ".join(diff["columns_added"]) or "none",
            cols_removed=", ".join(diff["columns_removed"]) or "none",
            agg_change="Yes" if diff["has_agg_change"] else "No",
            filter_change="Yes" if diff["has_filter_change"] else "No",
            order_change="Yes" if diff["has_order_change"] else "No",
            group_change="Yes" if diff["has_group_change"] else "No",
        )

        try:
            raw = await llm_manager.generate(
                prompt=prompt,
                temperature=0.0,
                num_ctx=Config.CRITIC_NUM_CTX,
            )

            rule = raw.strip()
            # Clean up common prefixes
            for prefix in ["RULE:", "Rule:", "rule:", "- ", "• "]:
                if rule.startswith(prefix):
                    rule = rule[len(prefix):].strip()

            # If LLM produced garbage, build a deterministic fallback rule
            if len(rule) < 20 or len(rule) > 500:
                rule = self._build_fallback_rule(user_query, diff)

        except Exception as e:
            logger.error(f"Offline Critic LLM failed: {e}")
            rule = self._build_fallback_rule(user_query, diff)

        # Step 3: Store in Experience Memory
        experience_memory.add_rule(
            original_query=user_query,
            failed_sql=failed_sql,
            correct_sql=correct_sql,
            rule_text=rule,
        )

        logger.info(f"Offline Critic: rule learned — {rule[:100]}")
        return rule

    def _build_fallback_rule(self, query: str, diff: dict) -> str:
        """Build a deterministic rule when LLM is unavailable."""
        parts = []

        if diff["columns_added"]:
            parts.append(f"include columns: {', '.join(diff['columns_added'])}")
        if diff["columns_removed"]:
            parts.append(f"do not use columns: {', '.join(diff['columns_removed'])}")
        if diff["has_agg_change"]:
            parts.append("check aggregation function is correct")
        if diff["has_filter_change"]:
            parts.append("check WHERE conditions match user intent")
        if diff["has_order_change"]:
            parts.append("check ORDER BY column and direction")
        if diff["has_group_change"]:
            parts.append("check GROUP BY clause")

        if parts:
            return f"When handling queries like '{query[:60]}', " + ", ".join(parts) + "."
        return f"When handling queries like '{query[:60]}', verify column selection matches user intent."


offline_critic = OfflineCritic()
