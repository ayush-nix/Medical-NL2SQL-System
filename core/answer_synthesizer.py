"""
Answer Synthesizer — converts SQL results into natural language answers.

HYBRID MODE:
- FAST: Smart template-based synthesis (no LLM, instant)
- SLOW: LLM-based synthesis (if explicitly requested)

Default: FAST mode for low-latency responses.
"""
import logging
from config import Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.synthesizer")


class AnswerSynthesizer:
    """Convert SQL results to clear natural language answers."""

    SYNTHESIS_PROMPT = """You are a data analyst. Given the SQL query results below,
provide a clear, concise answer to the user's question.

Question: {question}
SQL: {sql}
Results ({row_count} rows):
{formatted_results}

Rules:
1. Answer ONLY based on the data shown.
2. Include specific numbers and values.
3. Be concise — 2-3 sentences max.

Answer:"""

    async def synthesize(self, question: str, sql: str,
                         results: dict, use_llm: bool = False) -> str:
        """Generate NL answer from SQL results."""
        if not results.get("success"):
            return (
                f"The query encountered an error: {results.get('error', 'Unknown')}. "
                f"Please try rephrasing your question."
            )

        rows = results.get("rows", [])
        columns = results.get("columns", [])
        row_count = results.get("row_count", 0)

        if row_count == 0:
            return "No records found matching your query."

        # FAST MODE: Smart template (default — no LLM, instant)
        if not use_llm:
            return self._smart_answer(question, columns, rows, row_count)

        # SLOW MODE: LLM synthesis (optional)
        formatted = self._format_results(columns, rows, max_rows=15)
        try:
            answer = await llm_manager.generate(
                prompt=self.SYNTHESIS_PROMPT.format(
                    question=question,
                    sql=sql,
                    row_count=row_count,
                    formatted_results=formatted,
                ),
                model=Config.FAST_MODEL,
                temperature=0.1,
                num_ctx=Config.FAST_NUM_CTX,
            )
            return answer.strip() if answer.strip() else self._smart_answer(question, columns, rows, row_count)
        except Exception as e:
            logger.error(f"Synthesis error: {e}")
            return self._smart_answer(question, columns, rows, row_count)

    def _smart_answer(self, question: str, columns: list, rows: list,
                      row_count: int) -> str:
        """Smart template-based answer — instant, no LLM needed."""
        q_lower = question.lower()

        # Single value result (e.g., COUNT, AVG)
        if row_count == 1 and len(columns) == 1:
            val = rows[0].get(columns[0], "N/A")
            return f"The result is: **{val}**"

        # Single row, multiple columns
        if row_count == 1:
            parts = []
            for col in columns:
                val = rows[0].get(col, "N/A")
                display_col = col.replace("_", " ").title()
                parts.append(f"**{display_col}**: {val}")
            return "Found 1 record — " + ", ".join(parts)

        # Aggregation with groups (e.g., GROUP BY risk_scale)
        if row_count <= 10 and any(kw in q_lower for kw in ["average", "avg", "count", "sum", "group", "each"]):
            lines = []
            for row in rows:
                parts = [f"{col.replace('_', ' ')}: {row.get(col, 'N/A')}" for col in columns]
                lines.append(" | ".join(parts))
            return f"Found **{row_count}** groups:\n" + "\n".join(f"- {l}" for l in lines)

        # Top N / ranking results
        if row_count <= 10 and any(kw in q_lower for kw in ["top", "highest", "lowest", "best", "worst"]):
            lines = []
            for i, row in enumerate(rows[:10], 1):
                key_vals = [f"{col.replace('_', ' ')}: {row.get(col, 'N/A')}" for col in columns[:4]]
                lines.append(f"{i}. " + ", ".join(key_vals))
            return f"Top **{row_count}** results:\n" + "\n".join(lines)

        # Multiple rows — summary
        summary_parts = [f"Found **{row_count}** records"]

        # Show key numeric stats if available
        numeric_cols = []
        for col in columns:
            try:
                vals = [float(row.get(col, 0)) for row in rows if row.get(col) is not None]
                if vals:
                    numeric_cols.append((col, min(vals), max(vals), sum(vals)/len(vals)))
            except (ValueError, TypeError):
                continue

        if numeric_cols:
            stats = []
            for col, mn, mx, avg in numeric_cols[:3]:
                display = col.replace("_", " ")
                stats.append(f"**{display}**: avg={avg:.2f}, min={mn:.2f}, max={mx:.2f}")
            summary_parts.append("Key stats: " + "; ".join(stats))

        summary_parts.append(f"Columns: {', '.join(columns[:6])}")
        return ". ".join(summary_parts) + "."

    def _format_results(self, columns: list, rows: list,
                        max_rows: int = 15) -> str:
        """Format results as a readable table string."""
        if not rows:
            return "(empty)"
        display_rows = rows[:max_rows]
        lines = []
        lines.append(" | ".join(columns))
        lines.append("-" * len(lines[0]))
        for row in display_rows:
            values = [str(row.get(col, ""))[:20] for col in columns]
            lines.append(" | ".join(values))
        if len(rows) > max_rows:
            lines.append(f"... and {len(rows) - max_rows} more rows")
        return "\n".join(lines)

    def _fallback_answer(self, columns: list, rows: list,
                         row_count: int) -> str:
        """Backward-compatible fallback."""
        return self._smart_answer("", columns, rows, row_count)
