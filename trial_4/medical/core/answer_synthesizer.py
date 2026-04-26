"""
Agent 5: Answer Synthesizer — Smart template-based NL answers.
Zero LLM calls by default (instant). Optional LLM fallback.
"""
import logging
from config import MedicalConfig as Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.agent5_synthesizer")


class AnswerSynthesizer:
    """Convert SQL results to clear natural language answers."""

    SYNTHESIS_PROMPT = """You are a data analyst. Given the SQL results below,
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

        # FAST MODE: Smart template (default)
        if not use_llm:
            return self._smart_answer(question, columns, rows, row_count)

        # SLOW MODE: LLM synthesis (optional)
        formatted = self._format_results(columns, rows, max_rows=15)
        try:
            answer = await llm_manager.generate(
                prompt=self.SYNTHESIS_PROMPT.format(
                    question=question, sql=sql,
                    row_count=row_count, formatted_results=formatted,
                ),
                temperature=0.1,
                num_ctx=2048,
            )
            return answer.strip() if answer.strip() else self._smart_answer(question, columns, rows, row_count)
        except Exception as e:
            logger.error(f"Synthesis error: {e}")
            return self._smart_answer(question, columns, rows, row_count)

    def _smart_answer(self, question: str, columns: list, rows: list,
                      row_count: int) -> str:
        """
        Smart template-based answer — instant, no LLM.
        
        DESIGN DECISION: For multi-row results (>2 rows), show ONLY a summary
        line like "Found 10 matching records." The result TABLE UI already
        displays all the data beautifully — repeating it as text is redundant.
        Text answers are reserved for aggregation/single-value results where
        a natural language description adds real value.
        """
        q_lower = question.lower()

        # ── Single value (e.g. COUNT, AVG, SUM) ──────────────
        if row_count == 1 and len(columns) == 1:
            val = rows[0].get(columns[0], "N/A")
            return f"The result is: **{val}**"

        # ── Single row, multiple columns ──────────────────────
        if row_count == 1:
            parts = []
            for col in columns[:6]:
                val = rows[0].get(col, "N/A")
                display_col = col.replace("_", " ").title()
                parts.append(f"**{display_col}**: {val}")
            return "Found 1 record — " + ", ".join(parts)

        # ── Small aggregation groups (≤10 rows, agg query) ────
        if row_count <= 10 and len(columns) <= 3 and any(
            kw in q_lower for kw in ["average", "avg", "count", "sum", "group", "each", "per"]
        ):
            lines = []
            for row in rows:
                parts = [f"{col.replace('_', ' ')}: {row.get(col, 'N/A')}" for col in columns]
                lines.append(" | ".join(parts))
            return f"Found **{row_count}** groups:\n" + "\n".join(f"- {l}" for l in lines)

        # ── Multi-row results → one-line summary only ─────────
        # The result TABLE handles display. No row-by-row text dump.
        return f"Found **{row_count}** matching records."

    def _format_results(self, columns: list, rows: list, max_rows: int = 15) -> str:
        """Format results as readable table."""
        if not rows:
            return "(empty)"
        display = rows[:max_rows]
        lines = [" | ".join(columns)]
        lines.append("-" * len(lines[0]))
        for row in display:
            values = [str(row.get(col, ""))[:20] for col in columns]
            lines.append(" | ".join(values))
        if len(rows) > max_rows:
            lines.append(f"... and {len(rows) - max_rows} more rows")
        return "\n".join(lines)
