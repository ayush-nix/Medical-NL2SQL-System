"""
Agent 4: SQL Refiner — Self-correction loop.

Triggered ONLY when 4-pass validation fails.
Receives the specific error and asks the LLM to fix it.
Max 2 retries. Each retry goes through full validation again.
"""
import time
import logging
from config import MedicalConfig as Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.agent4_refiner")

REPAIR_PROMPT = """The SQL query below failed validation. Fix it.

FAILED SQL:
{failed_sql}

ERROR (Pass {pass_number}):
{error}

ORIGINAL QUESTION: "{question}"

DATABASE SCHEMA (use ONLY these exact column names):
{schema_ddl}

RULES:
1. Return ONLY the corrected SQL query. No explanation.
2. Use ONLY column names from the CREATE TABLE above.
3. Table name: {table_name}
4. SQLite syntax only.

EXACT COLUMN NAMES AVAILABLE:
{column_names}

CORRECTED SQL:
"""


class SQLRefiner:
    """Agent 4: Fix failed SQL queries with focused LLM repair."""

    async def refine(self, failed_sql: str, error: str, pass_number: int,
                     question: str, schema_ddl: str, table_name: str,
                     column_names: list[str],
                     validate_fn=None) -> dict:
        """
        Attempt to fix a failed SQL query.
        
        Args:
            failed_sql: the SQL that failed validation
            error: the specific error message
            pass_number: which validation pass failed
            question: original user question
            schema_ddl: pruned schema DDL
            table_name: table name
            column_names: list of valid column names
            validate_fn: callable to re-validate (returns ValidationResult)
        
        Returns:
            {
                "sql": str,
                "fixed": bool,
                "attempts": int,
                "errors": [str],
            }
        """
        start = time.time()
        max_retries = Config.MAX_REFINER_RETRIES
        current_sql = failed_sql
        current_error = error
        current_pass = pass_number
        errors = [error]

        for attempt in range(1, max_retries + 1):
            logger.info(f"Agent 4: Repair attempt {attempt}/{max_retries} — Pass {current_pass} error: {current_error[:100]}")

            prompt = REPAIR_PROMPT.format(
                failed_sql=current_sql,
                pass_number=current_pass,
                error=current_error,
                question=question,
                schema_ddl=schema_ddl,
                table_name=table_name,
                column_names=", ".join(column_names),
            )

            try:
                # Use SQL model for repair (SQL is a SQL task, not reasoning)
                raw = await llm_manager.generate_sql(
                    prompt=prompt,
                )

                # Extract SQL from response
                import re
                raw = raw.strip()
                if "```" in raw:
                    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", raw, re.DOTALL | re.IGNORECASE)
                    if match:
                        raw = match.group(1).strip()
                select_match = re.search(r'((?:WITH|SELECT)\s+.+)', raw, re.DOTALL | re.IGNORECASE)
                if select_match:
                    raw = select_match.group(1).strip()
                refined_sql = raw.rstrip(";").strip()

                # Re-validate
                if validate_fn:
                    result = validate_fn(refined_sql)
                    if result.passed:
                        elapsed = int((time.time() - start) * 1000)
                        # Use the auto-fixed version if available
                        final_sql = result.fixed_sql if result.fixed_sql else refined_sql
                        logger.info(f"Agent 4: FIXED on attempt {attempt} in {elapsed}ms")
                        return {
                            "sql": final_sql,
                            "fixed": True,
                            "attempts": attempt,
                            "errors": errors,
                            "refine_time_ms": elapsed,
                        }
                    else:
                        current_sql = refined_sql
                        current_error = result.error
                        current_pass = result.pass_number
                        errors.append(result.error)
                else:
                    # No validator provided — return as-is
                    elapsed = int((time.time() - start) * 1000)
                    return {
                        "sql": refined_sql,
                        "fixed": True,
                        "attempts": attempt,
                        "errors": errors,
                        "refine_time_ms": elapsed,
                    }

            except Exception as e:
                logger.error(f"Agent 4: Repair attempt {attempt} failed: {e}")
                errors.append(str(e))

        elapsed = int((time.time() - start) * 1000)
        logger.warning(f"Agent 4: All {max_retries} repair attempts failed in {elapsed}ms")
        return {
            "sql": current_sql,
            "fixed": False,
            "attempts": max_retries,
            "errors": errors,
            "refine_time_ms": elapsed,
        }
