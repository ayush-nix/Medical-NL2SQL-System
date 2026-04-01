"""
SQL Generator — NL-to-SQL engine with SQLCoder.

Optimized for:
1. Compact schema prompt (column pruner feeds only relevant columns)
2. Strict column-name enforcement via type annotations
3. Single-table optimization (no JOIN overhead)
4. Self-correction loop with error feedback

SECURITY: Generates ONLY SELECT statements.
"""
import logging
import time
from config import Config
from core.sql_validator import validate_sql, extract_clean_sql, ValidationResult
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.generator")


class SQLGenerator:
    """
    SQL generator with self-correction loop.
    Uses sqlcoder:7b as primary, mistral as fallback.
    """

    MASTER_PROMPT = """### Task
Generate a SQL query to answer the following question:
"{question}"

### Database Schema
{schema}

{sample_values}

{business_hints}

### Rules
1. Generate ONLY a SELECT statement.
2. Use EXACT column and table names from the schema. Do NOT invent names.
3. Use SQLite syntax: strftime(), substr(), ||, LIMIT (not TOP).
4. Return ONLY the SQL query — no explanation, no markdown fences.

### SQL Query:
"""

    CORRECTION_PROMPT = """The SQL query below failed:

{failed_sql}

Error: {error}

Question: "{question}"
Schema: {schema}

Fix the SQL. Use only columns from the schema. Return ONLY the corrected SQL:
"""

    def __init__(self):
        self.few_shot_examples: list[dict] = []

    def add_few_shot(self, question: str, sql: str):
        """Add a Q→SQL example for in-context learning."""
        self.few_shot_examples.append({
            "question": question,
            "sql": sql
        })

    async def generate(self, question: str, schema_text: str,
                       relationships_text: str, sample_values: str,
                       join_hints: str, business_hints: list[str],
                       schema_metadata=None, plan_context: str = "") -> dict:
        """
        Generation pipeline:
        1. Build compact prompt
        2. Generate SQL with SQLCoder
        3. Validate → self-correct up to N retries
        4. Fallback to Mistral if needed
        """
        start_time = time.time()

        # Build business hints (keep short)
        hints_text = ""
        if business_hints:
            hints_text = "### Hints\n" + "\n".join(
                f"- {h}" for h in business_hints[:3]
            )

        # Build prompt
        prompt = self.MASTER_PROMPT.format(
            question=question,
            schema=schema_text,
            sample_values=sample_values if sample_values else "",
            business_hints=hints_text,
        )

        # ── Generate SQL ─────────────────────────────────────
        sql = ""
        attempts = 0
        model_used = Config.SQL_MODEL
        validation: ValidationResult = ValidationResult(passed=False)

        for attempt in range(Config.MAX_RETRIES + 1):
            attempts = attempt + 1

            if attempt == 0:
                raw = await llm_manager.generate(
                    prompt=prompt,
                    model=Config.SQL_MODEL,
                    temperature=0.0,
                    num_ctx=Config.SQL_NUM_CTX,
                )
            elif attempt < Config.MAX_RETRIES:
                correction = self.CORRECTION_PROMPT.format(
                    failed_sql=sql,
                    error=validation.error,
                    question=question,
                    schema=schema_text,
                )
                raw = await llm_manager.generate(
                    prompt=correction,
                    model=Config.SQL_MODEL,
                    temperature=0.1,
                    num_ctx=Config.SQL_NUM_CTX,
                )
                model_used = Config.SQL_MODEL + " (retry)"
            else:
                raw = await llm_manager.generate(
                    prompt=prompt,
                    model=Config.FAST_MODEL,
                    temperature=0.1,
                    num_ctx=Config.FAST_NUM_CTX,
                )
                model_used = Config.FAST_MODEL + " (fallback)"

            sql = extract_clean_sql(raw)
            logger.info(f"Attempt {attempts}: {sql[:120]}...")

            validation = validate_sql(sql, schema_metadata)
            if validation.passed:
                break

            logger.warning(
                f"Attempt {attempts} failed pass {validation.pass_number}: "
                f"{validation.error}"
            )

        elapsed = time.time() - start_time

        return {
            "sql": sql,
            "valid": validation.passed,
            "validation_error": validation.error if not validation.passed else "",
            "attempts": attempts,
            "model_used": model_used,
            "generation_time_ms": int(elapsed * 1000),
            "confidence": validation.confidence if validation.passed else 0.3,
        }

    async def classify_query(self, question: str) -> str:
        """Classify query type using fast model."""
        try:
            prompt = f"""Classify this question into ONE category.
Question: "{question}"
Categories: SINGLE_TABLE, AGGREGATION, SUBQUERY, TEMPORAL
Reply with ONLY the category name:"""
            result = await llm_manager.generate(
                prompt=prompt,
                model=Config.FAST_MODEL,
                temperature=0.0,
                num_ctx=Config.FAST_NUM_CTX,
            )
            category = result.strip().upper().replace(" ", "_")
            valid = {"SINGLE_TABLE", "AGGREGATION", "SUBQUERY", "TEMPORAL"}
            return category if category in valid else "SINGLE_TABLE"
        except Exception:
            return "SINGLE_TABLE"
