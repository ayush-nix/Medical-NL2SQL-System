"""
Agent 3 (Medical): SQL Generator — Medical-Domain Adaptive Prompting.

PIVOT LOG from landslide system:
  - REMOVED: All landslide/geoscience skeletons (TEMPORAL_COMPARISON on lat/lon,
    CONSECUTIVE_DAYS on prediction_date, slope/radian unit hints)
  - ADDED: Multi-table JOIN hints (admissions + hospital_lookup + icd_lookup),
    dual-diagnosis pattern (diagnosis OR diagnosis_code1d), ICD-10 LIKE patterns,
    mortality rate skeleton, LOS analysis skeleton, year-over-year skeleton,
    hospital comparison skeleton
  - CHANGED: Table name from 'prediction' to 'admissions', removed all
    encrypted_lat/lon references, updated column dictionary format

Reuses: core.column_sanitizer.build_column_dictionary (domain-agnostic)
        models.llm_manager (domain-agnostic)
"""
import re
import time
import logging

from config import MedicalConfig as Config
from models.llm_manager import llm_manager
from core.column_sanitizer import build_column_dictionary

logger = logging.getLogger("medical.agent3_generator")


# ══════════════════════════════════════════════════════════════
# EASY Prompt (~400 tokens) — simple counts, filters, lookups
# ══════════════════════════════════════════════════════════════
EASY_PROMPT = """### Task
Generate a single SQLite SELECT query to answer the question.
Output ONLY the SQL. No explanation. No markdown.

### Database Schema
-- PRIMARY table: admissions (1.65M military hospital records, 2021-2024)
-- Lookup tables: hospital_lookup, icd_lookup
{schema_ddl}

{column_dictionary}

### Question
{resolved_question}

### Rules
- PRIMARY table is: admissions. Use it for all queries unless you need hospital or ICD lookups.
- ONLY use column names listed in the COLUMN DICTIONARY above — never invent columns.
- SQLite syntax only.
- LIMIT to {limit} unless the question asks for all rows or an aggregate.
- For diagnosis queries: use diagnosis_code1d (ICD-10) OR diagnosis LIKE '%term%' OR icd_remarks_d LIKE '%term%'.
- data_year column = source file year (2021-2024). Use for year comparisons.

### REMINDER — EXACT column names: {column_name_list}

### SQL
SELECT"""


# ══════════════════════════════════════════════════════════════
# MEDIUM Prompt (~1200 tokens) — GROUP BY, aggregation, JOIN
# ══════════════════════════════════════════════════════════════
MEDIUM_PROMPT = """### Task
Generate a single SQLite SELECT query to answer the question.
Output ONLY the SQL. No explanation. No markdown.

### Database Schema
-- PRIMARY table: admissions (1.65M military hospital records, 2021-2024)
-- Lookup tables: hospital_lookup (171 hospitals), icd_lookup (ICD codes → diagnoses)
-- Any column NOT listed below does NOT exist. Do NOT invent columns.
{schema_ddl}

{column_dictionary}

### Question
{resolved_question}

### Pre-Analysis
Target columns: {target_columns}
Intent: {intent_types}
Output type: {output_type}
{business_context}

### SQL Pattern to Follow
{sql_skeleton}

### Rules
1. PRIMARY table is: admissions. Use this in FROM.
2. ONLY use column names from the COLUMN DICTIONARY. Copy-paste exactly.
3. SQLite syntax: DATE(), LIKE (not ILIKE), LIMIT N (not TOP N).
4. LIMIT to {limit} unless the question asks for all rows or aggregation.
5. If output type is ROWS, use SELECT * or SELECT column_list. Do NOT return AVG/COUNT alone.
6. For DIAGNOSIS queries, always try BOTH approaches:
   - diagnosis_code1d LIKE 'X%' (ICD-10 code match)
   - OR icd_remarks_d LIKE '%KEYWORD%' (free-text match)
   This catches cases where ICD code is null but remarks have the disease name.
7. data_year = source file year (2021/2022/2023/2024). Use for year-over-year.
8. For mortality: is_death = 1 covers both DEATH and FOUNDDEAD disposals.
9. For rates/percentages: CAST(SUM(CASE WHEN condition THEN 1 ELSE 0 END) AS REAL) * 100.0 / COUNT(*)

### REMINDER — EXACT column names: {column_name_list}

### SQL
SELECT"""


# ══════════════════════════════════════════════════════════════
# HARD Prompt (~2500 tokens) — complex JOINs, subqueries, rates
# ══════════════════════════════════════════════════════════════
HARD_PROMPT = """### Task
Generate a single SQLite SELECT query to answer the question.
Output ONLY the SQL. No explanation. No markdown.

### Database Schema
-- *** PRIMARY table: admissions (1.65M rows, 71 columns) ***
-- *** Lookup tables: hospital_lookup (171 hospitals), icd_lookup (ICD codes) ***
-- *** ONLY columns listed in the COLUMN DICTIONARY below exist ***
{schema_ddl}

{column_dictionary}

### Real Data Sample (study the data types and value ranges)
{sample_rows}

### Question
{resolved_question}
Original query: "{original_query}"
Output type required: {output_type}

### Step-by-Step Reasoning (from pre-analysis)
{reasoning}

### Pre-Identified Structure
Target columns: {target_columns}
Filter conditions: {filter_conditions}
Aggregation: {aggregation}
Group by: {group_by}
Order by: {order_by}
Limit: {limit}

### Business Context
{business_context}

### SQL Pattern to Follow
{sql_skeleton}

### Experience Hints (consider these patterns from past queries — use your judgment)
{rlaif_rules}

### Critical Rules
1. *** TABLE NAME IS: admissions *** — use this EXACT name in FROM and all subqueries.
2. ONLY use column names from the COLUMN DICTIONARY. Copy-paste EXACTLY as spelled.
3. SQLite syntax ONLY: DATE(), LIKE (not ILIKE), LIMIT N (not TOP N).
4. COLUMN CONSISTENCY: If outer WHERE uses column X, subquery AVG/MIN/MAX MUST use SAME column X.
5. NO HALLUCINATED TIME FILTERS: Do NOT add date filters UNLESS the question explicitly mentions dates/years.
6. FOR DIAGNOSIS QUERIES — use DUAL approach:
   WHERE (diagnosis_code1d LIKE 'I10%' OR icd_remarks_d LIKE '%HYPERTENSION%')
   This handles the 16% null rate in diagnosis_code1d.
7. FOR MORTALITY RATES:
   CAST(SUM(CASE WHEN is_death = 1 THEN 1 ELSE 0 END) AS REAL) * 100.0 / COUNT(*)
8. FOR YEAR-OVER-YEAR: GROUP BY data_year ORDER BY data_year
9. FOR HOSPITAL COMPARISON: GROUP BY medical_unit
10. FOR SOLDIER vs DEPENDENT: GROUP BY relation (values: 'SELF', 'DEPENDENTS')
11. FOR RANK ANALYSIS: Use rank_tier ('ENLISTED', 'JCO', 'OFFICER') for broad groups, rank for specific.
12. FOR LOS ANALYSIS: Use los_days for numeric, los_category for categorical grouping.
13. FOR ABOVE/BELOW AVERAGE: WHERE col > (SELECT AVG(col) FROM admissions)
14. FOR PERCENTILE/TOP N%:
    WHERE col > (SELECT col FROM admissions ORDER BY col DESC LIMIT 1 OFFSET (SELECT COUNT(*)*N/100 FROM admissions))

### REMINDER — EXACT column names: {column_name_list}

### SQL
SELECT"""


# ══════════════════════════════════════════════════════════════
# SQL Pattern Skeletons — Medical-domain specific
# ══════════════════════════════════════════════════════════════
SQL_SKELETONS = {
    "RANKING": """-- For "top N" / "highest" / "lowest" questions:
SELECT * FROM admissions
ORDER BY target_col DESC
LIMIT N""",

    "AGGREGATION": """-- For count/average/sum questions:
SELECT AGG_FUNCTION(target_col) FROM admissions
WHERE filter_conditions""",

    "YEAR_OVER_YEAR": """-- For year-on-year comparison / trends:
SELECT data_year, AGG_FUNC(target_col) as result
FROM admissions
WHERE optional_filters
GROUP BY data_year
ORDER BY data_year""",

    "HOSPITAL_COMPARISON": """-- For hospital-level comparison:
SELECT medical_unit, AGG_FUNC(target_col) as result
FROM admissions
WHERE optional_filters
GROUP BY medical_unit
ORDER BY result DESC
LIMIT 20""",

    "MORTALITY_RATE": """-- For mortality / death rate analysis:
SELECT group_col,
       COUNT(*) as total_admissions,
       SUM(CASE WHEN is_death = 1 THEN 1 ELSE 0 END) as deaths,
       CAST(SUM(CASE WHEN is_death = 1 THEN 1 ELSE 0 END) AS REAL) * 100.0 / COUNT(*) as mortality_rate_pct
FROM admissions
WHERE optional_filters
GROUP BY group_col
ORDER BY mortality_rate_pct DESC""",

    "LOS_ANALYSIS": """-- For length of stay analysis:
SELECT group_col,
       COUNT(*) as admissions,
       ROUND(AVG(los_days), 1) as avg_los,
       MIN(los_days) as min_los,
       MAX(los_days) as max_los
FROM admissions
WHERE los_days IS NOT NULL AND optional_filters
GROUP BY group_col
ORDER BY avg_los DESC""",

    "DIAGNOSIS_SEARCH": """-- For disease / diagnosis queries:
-- ALWAYS use dual approach to handle null ICD codes
SELECT * FROM admissions
WHERE (diagnosis_code1d LIKE 'CODE%' OR icd_remarks_d LIKE '%DISEASE_NAME%')
  AND optional_filters
LIMIT 100""",

    "SOLDIER_VS_DEPENDENT": """-- For soldier vs dependent comparison:
SELECT relation,
       COUNT(*) as total,
       AGG_FUNC(target_col) as result
FROM admissions
GROUP BY relation""",

    "SEASONAL_ANALYSIS": """-- For seasonal disease patterns:
SELECT season, COUNT(*) as admissions
FROM admissions
WHERE disease_filter
GROUP BY season
ORDER BY admissions DESC""",

    "RANK_ANALYSIS": """-- For rank-based analysis:
SELECT rank_tier, COUNT(*) as total
FROM admissions
WHERE relation = 'SELF' AND optional_filters
GROUP BY rank_tier
ORDER BY total DESC""",

    "COMPARISON_VS_AVG": """-- For "higher/lower than average" questions:
SELECT * FROM admissions
WHERE target_col > (SELECT AVG(target_col) FROM admissions)
LIMIT 100""",

    "TEMPORAL_MONTHLY": """-- For monthly trends:
SELECT admission_month, COUNT(*) as admissions
FROM admissions
WHERE optional_filters
GROUP BY admission_month
ORDER BY admission_month""",

    "COMMAND_COMPARISON": """-- For command-level analysis:
SELECT command, COUNT(*) as total, AGG_FUNC(target_col) as result
FROM admissions
WHERE optional_filters
GROUP BY command
ORDER BY result DESC""",
}


class MedicalSQLGenerator:
    """Agent 3 (Medical): Generates SQL from structured plan using adaptive prompting."""

    def _classify_output_type(self, question: str, intent_types: list) -> str:
        """Determine if user wants ROWS, AGGREGATE, or COUNT."""
        q = question.lower()
        if any(w in q for w in ["show", "list", "display", "find", "which", "what are the"]):
            return "ROWS"
        if any(w in q for w in ["how many", "count", "total number"]):
            return "COUNT"
        if any(w in q for w in ["average", "avg", "mean", "rate", "percentage", "ratio"]):
            return "AGGREGATE"
        if "AGGREGATION" in intent_types:
            return "AGGREGATE"
        if "RANKING" in intent_types:
            return "ROWS"
        return "ROWS"

    def _select_skeleton(self, intent_types: list, question: str, semantic_hints: list = None) -> str:
        """Select best SQL skeleton based on detected intent and question patterns."""
        q = question.lower()

        # Check for specific medical patterns first
        if any(w in q for w in ["mortality", "death rate", "fatality"]):
            return SQL_SKELETONS["MORTALITY_RATE"]
        if any(w in q for w in ["length of stay", "los", "stay duration", "how long"]):
            return SQL_SKELETONS["LOS_ANALYSIS"]
        if any(w in q for w in ["year over year", "yearly", "year wise", "annual", "each year"]):
            return SQL_SKELETONS["YEAR_OVER_YEAR"]
        if any(w in q for w in ["hospital", "medical unit", "which hospital"]):
            return SQL_SKELETONS["HOSPITAL_COMPARISON"]
        if any(w in q for w in ["soldier", "dependent", "self vs", "jawan vs officer"]):
            return SQL_SKELETONS["SOLDIER_VS_DEPENDENT"]
        if any(w in q for w in ["season", "monsoon", "summer", "winter"]):
            return SQL_SKELETONS["SEASONAL_ANALYSIS"]
        if any(w in q for w in ["rank", "officer", "jco", "enlisted"]):
            return SQL_SKELETONS["RANK_ANALYSIS"]
        if any(w in q for w in ["command", "western command", "eastern"]):
            return SQL_SKELETONS["COMMAND_COMPARISON"]
        if any(w in q for w in ["month", "monthly"]):
            return SQL_SKELETONS["TEMPORAL_MONTHLY"]

        # Disease search
        disease_terms = ["dengue", "malaria", "covid", "tb", "diabetes", "hypertension",
                        "cardiac", "mental", "injury", "fracture", "cancer", "respiratory"]
        if any(t in q for t in disease_terms):
            return SQL_SKELETONS["DIAGNOSIS_SEARCH"]

        # Generic patterns
        if any(w in q for w in ["above average", "below average", "higher than average"]):
            return SQL_SKELETONS["COMPARISON_VS_AVG"]
        if "RANKING" in intent_types:
            return SQL_SKELETONS["RANKING"]
        if "AGGREGATION" in intent_types:
            return SQL_SKELETONS["AGGREGATION"]

        return "-- No specific skeleton matched, generate from query context"

    async def generate(self, original_query: str, resolved_question: str,
                       schema_ddl: str, table_name: str, enum_values: str = "",
                       reasoning: str = "", target_columns=None,
                       filter_conditions=None, aggregation=None,
                       group_by=None, order_by=None, limit=None,
                       business_hints=None, rlaif_rules=None,
                       valid_column_names=None, sample_rows=None,
                       intent_types=None, complexity: str = "EASY",
                       semantic_hints=None, column_metadata=None,
                       column_profiles=None) -> dict:
        """Generate SQL using complexity-adaptive prompting."""
        start = time.time()

        # Defaults
        intent_types = intent_types or []
        target_columns = target_columns or []
        filter_conditions = filter_conditions or []
        business_hints = business_hints or []
        valid_column_names = valid_column_names or []
        column_metadata = column_metadata or []
        column_profiles = column_profiles or {}

        # Build column dictionary
        col_dict = build_column_dictionary(column_metadata, column_profiles)

        # Detect output type
        output_type = self._classify_output_type(original_query, intent_types)
        logger.info(f"Output type: {output_type}, Complexity: {complexity}")

        # Select skeleton
        skeleton = self._select_skeleton(intent_types, original_query, semantic_hints)
        logger.debug(f"Selected skeleton: {skeleton[:80]}...")

        # Build business context
        business_context = ""
        if business_hints:
            business_context = "Business hints:\n" + "\n".join(f"- {h}" for h in business_hints)
        if enum_values:
            business_context += f"\n\nValid column values:\n{enum_values}"

        # Format sample rows
        sample_str = ""
        if sample_rows:
            sample_str = "Sample rows:\n"
            for i, row in enumerate(sample_rows[:3]):
                sample_str += f"Row {i+1}: {row}\n"

        # Column name list
        col_list = ", ".join(valid_column_names[:50]) if valid_column_names else ""

        # Default limit
        effective_limit = limit or 100

        # Select prompt template by complexity
        if complexity == "HARD":
            template = HARD_PROMPT
        elif complexity == "MEDIUM":
            template = MEDIUM_PROMPT
        else:
            template = EASY_PROMPT

        # Format prompt
        try:
            prompt = template.format(
                schema_ddl=schema_ddl,
                column_dictionary=col_dict,
                resolved_question=resolved_question,
                original_query=original_query,
                target_columns=target_columns,
                intent_types=intent_types,
                output_type=output_type,
                business_context=business_context,
                sql_skeleton=skeleton,
                reasoning=reasoning,
                filter_conditions=filter_conditions,
                aggregation=aggregation,
                group_by=group_by,
                order_by=order_by,
                limit=effective_limit,
                rlaif_rules=rlaif_rules or "None",
                sample_rows=sample_str,
                column_name_list=col_list,
                table_name=table_name,
            )
        except KeyError as e:
            logger.warning(f"Template format error: {e}")
            prompt = EASY_PROMPT.format(
                schema_ddl=schema_ddl,
                column_dictionary=col_dict,
                resolved_question=resolved_question,
                limit=effective_limit,
                column_name_list=col_list,
                table_name=table_name,
            )

        # Generate SQL via LLM
        logger.info(f"Calling LLM ({complexity} prompt, {len(prompt)} chars)...")
        raw_output = await llm_manager.generate_sql(
            prompt=prompt,
            temperature=Config.SQL_TEMPERATURE,
            num_ctx=Config.SQL_NUM_CTX,
        )

        # Extract SQL from response
        sql = self._extract_sql(raw_output)
        generation_time = int((time.time() - start) * 1000)

        logger.info(f"Agent 3 complete: {generation_time}ms, SQL length={len(sql)}")
        logger.debug(f"Generated SQL: {sql}")

        return {
            "sql": sql,
            "generation_time_ms": generation_time,
            "model_used": llm_manager.sql_model,
            "raw_output": raw_output,
            "prompt_used": prompt[:3000],
        }

    def _extract_sql(self, raw: str) -> str:
        """Extract clean SQL from LLM output."""
        if not raw:
            return "SELECT 'No SQL generated' AS error"

        # Prepend SELECT if the prompt ended with "SELECT"
        sql = "SELECT " + raw

        # Remove markdown code blocks
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)

        # Take only the first statement
        if ';' in sql:
            sql = sql.split(';')[0] + ';'
        else:
            sql = sql.strip()

        # Remove trailing explanations
        for marker in ['\n\n', '-- Note:', '-- Explanation:', 'Note:',
                       'Explanation:', 'This query', 'The above']:
            if marker in sql:
                sql = sql[:sql.index(marker)].strip()

        # Remove any trailing ```
        sql = sql.replace('```', '').strip()

        # Ensure it starts with SELECT
        if not sql.upper().startswith('SELECT'):
            sql = 'SELECT ' + sql

        return sql
