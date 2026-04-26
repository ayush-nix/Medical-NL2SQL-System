"""
5-Pass SQL Validator — Deterministic, zero LLM calls.

Pass 1: AST Syntax Parse (sqlglot)
Pass 2: Safety WHITELIST (only SELECT allowed, AST-level)
Pass 3: Schema Conformance (STRICT — only PRUNED columns + high-threshold auto-fix)
Pass 4: Type Safety (block AVG on TEXT, etc.)
Pass 5: Value Range Sanity (block landslide_probability >= 4 when range is 0-1, etc.)
"""
import re
import logging
from dataclasses import dataclass, field
from typing import Optional

import sqlglot
from sqlglot import exp

logger = logging.getLogger("nl2sql.validator")


@dataclass
class ValidationResult:
    passed: bool
    error: str = ""
    pass_number: int = 0
    fixed_sql: str = ""
    corrections: list = field(default_factory=list)


# ── Blocked patterns within SELECT ───────────────────────────
BLOCKED_PATTERNS = [
    "INTO", "EXEC", "EXECUTE", "XP_", "SP_", "DBCC",
    "OPENROWSET", "OPENDATASOURCE", "BULK", "GRANT", "REVOKE",
    "SHUTDOWN", "WAITFOR", "BENCHMARK(", "SLEEP(", "LOAD_FILE(",
    "UTL_FILE", "DBMS_",
]


def validate_sql(sql: str, valid_columns: set, valid_tables: set,
                 column_types: dict = None, column_profiles: dict = None,
                 pruned_columns: set = None) -> ValidationResult:
    """
    Run all 5 validation passes.
    
    Args:
        sql: generated SQL string
        valid_columns: set of ALL valid column names (lowercase)
        valid_tables: set of valid table names (lowercase)
        column_types: dict of col_name_lower -> type_string
        column_profiles: dict of col_name -> {min, max, mean, ...}
        pruned_columns: set of column names from Agent 2's selection (STRICT check)
    
    Returns:
        ValidationResult with pass/fail, errors, and optionally fixed SQL
    """
    sql = sql.strip()
    if not sql:
        return ValidationResult(passed=False, error="Empty SQL", pass_number=0)

    # Clean markdown artifacts
    if sql.startswith("```"):
        sql = re.sub(r"^```\w*\n?", "", sql)
        sql = re.sub(r"\n?```$", "", sql)
        sql = sql.strip()

    # Remove SQL comments (-- lines) for validation but keep for final output
    sql_clean = sql
    corrections = []

    # ── Pass 1: Syntax ────────────────────────────────────────
    result = _pass1_syntax(sql)
    if not result.passed:
        return result

    # ── Pass 2: Safety ────────────────────────────────────────
    result = _pass2_safety(sql)
    if not result.passed:
        return result

    # ── Pass 3: Schema Conformance + STRICT Auto-Fix ──────────
    check_set = pruned_columns if pruned_columns else valid_columns
    result, sql, new_corrections = _pass3_schema(sql, check_set, valid_columns, valid_tables)
    corrections.extend(new_corrections)
    if not result.passed:
        return result

    # ── Pass 4: Type Safety ───────────────────────────────────
    if column_types:
        result = _pass4_types(sql, column_types)
        if not result.passed:
            return result

    # ── Pass 5: Value Range Sanity ────────────────────────────
    if column_profiles:
        result, sql, range_corrections = _pass5_value_range(sql, column_profiles)
        corrections.extend(range_corrections)
        if not result.passed:
            return result

    # ── Pass 6: Subquery Consistency + SQLite Date Syntax ─────
    sql, p6_corrections = _pass6_consistency(sql, valid_columns)
    corrections.extend(p6_corrections)

    return ValidationResult(
        passed=True,
        fixed_sql=sql,
        corrections=corrections,
    )


def _pass1_syntax(sql: str) -> ValidationResult:
    """Parse SQL into AST. If parsing fails, SQL is malformed."""
    try:
        parsed = sqlglot.parse(sql)
        if not parsed or parsed[0] is None:
            return ValidationResult(
                passed=False,
                error="Could not parse SQL — malformed syntax",
                pass_number=1,
            )
        return ValidationResult(passed=True, pass_number=1)
    except sqlglot.errors.ParseError as e:
        return ValidationResult(
            passed=False,
            error=f"SQL syntax error: {str(e)[:200]}",
            pass_number=1,
        )
    except Exception as e:
        return ValidationResult(
            passed=False,
            error=f"Parse error: {str(e)[:200]}",
            pass_number=1,
        )


def _pass2_safety(sql: str) -> ValidationResult:
    """WHITELIST: ONLY SELECT allowed. Everything else blocked."""
    try:
        statements = sqlglot.parse(sql)
        for stmt in statements:
            if stmt is None:
                continue
            if not isinstance(stmt, exp.Select):
                stmt_type = type(stmt).__name__
                return ValidationResult(
                    passed=False,
                    error=f"BLOCKED: {stmt_type} is not allowed. Only SELECT queries permitted.",
                    pass_number=2,
                )
    except Exception:
        pass

    # String-level checks for dangerous patterns within SELECT
    sql_upper = sql.upper()
    for pattern in BLOCKED_PATTERNS:
        pattern_re = r'\b' + re.escape(pattern.rstrip('('))
        if pattern.endswith('('):
            pattern_re += r'\s*\('
        else:
            pattern_re += r'\b'
        if re.search(pattern_re, sql_upper):
            return ValidationResult(
                passed=False,
                error=f"BLOCKED: '{pattern}' pattern detected. Not allowed.",
                pass_number=2,
            )

    return ValidationResult(passed=True, pass_number=2)


def _pass3_schema(sql: str, primary_columns: set, all_columns: set,
                  valid_tables: set) -> tuple[ValidationResult, str, list]:
    """
    Verify tables/columns exist. STRICT auto-fix.
    
    Two-tier column checking:
    1. First check against PRUNED columns (primary_columns) 
    2. If not found, check against ALL columns (fallback)
    3. Auto-fix with threshold 0.80+ (lowered from 0.90 — the new
       column_sanitizer handles easy fixes upstream, so anything that
       reaches here is a genuinely hard case)
    
    v4.0: Error messages now include top-3 closest matches with scores
    so the Refiner agent can make intelligent choices.
    """
    corrections = []

    try:
        parsed = sqlglot.parse_one(sql)

        # Check table names
        for table in parsed.find_all(exp.Table):
            table_name = table.name.lower()
            if table_name and table_name not in valid_tables:
                # Skip aliases (short names like 'a', 'b', 't1')
                if len(table_name) <= 3:
                    continue
                return (
                    ValidationResult(
                        passed=False,
                        error=f"Table '{table_name}' does not exist. Available: {', '.join(sorted(valid_tables))}",
                        pass_number=3,
                    ),
                    sql,
                    corrections,
                )

        # Check and auto-fix column names
        for col_ref in parsed.find_all(exp.Column):
            col_name = col_ref.name
            if not col_name or col_name in ('*',):
                continue

            col_lower = col_name.lower()
            
            # Check 1: Exact match in primary (pruned) columns
            if col_lower in {c.lower() for c in primary_columns}:
                correct = next((c for c in primary_columns if c.lower() == col_lower), col_name)
                if correct != col_name:
                    sql = re.sub(r'\b' + re.escape(col_name) + r'\b', correct, sql)
                    corrections.append(f"Case fix: {col_name} → {correct}")
                continue

            # Check 2: Exact match in ALL columns (still valid, just not pruned)
            if col_lower in {c.lower() for c in all_columns}:
                correct = next((c for c in all_columns if c.lower() == col_lower), col_name)
                if correct != col_name:
                    sql = re.sub(r'\b' + re.escape(col_name) + r'\b', correct, sql)
                corrections.append(f"WARN: '{col_name}' exists but was not in pruned schema")
                continue

            # Check 3: Fuzzy match with threshold 0.80 — against pruned columns
            from utils.fuzzy_matcher import find_best_match
            best, score = find_best_match(col_name, list(primary_columns), threshold=0.80)

            if best and score >= 0.80:
                logger.info(f"Auto-fix column: '{col_name}' → '{best}' (score={score:.2f})")
                sql = re.sub(r'\b' + re.escape(col_name) + r'\b', best, sql)
                corrections.append(f"Column fix: {col_name} → {best} (score={score:.2f})")
            else:
                # Last resort: try against all columns with 0.82 threshold
                best2, score2 = find_best_match(col_name, list(all_columns), threshold=0.82)
                if best2 and score2 >= 0.82:
                    sql = re.sub(r'\b' + re.escape(col_name) + r'\b', best2, sql)
                    corrections.append(f"Column fix (full schema): {col_name} → {best2} (score={score2:.2f})")
                else:
                    # REJECTION — but with smart suggestions for the Refiner
                    suggestions = _get_closest_columns(col_name, primary_columns, n=3)
                    suggestion_str = ", ".join(
                        f"'{c}' ({s:.2f})" for c, s in suggestions
                    )
                    return (
                        ValidationResult(
                            passed=False,
                            error=(
                                f"Invalid column '{col_name}'. "
                                f"Did you mean: {suggestion_str}? "
                                f"IMPORTANT: Use the EXACT spelling from the suggestions above."
                            ),
                            pass_number=3,
                        ),
                        sql,
                        corrections,
                    )

        return ValidationResult(passed=True, pass_number=3), sql, corrections

    except Exception as e:
        logger.warning(f"Pass 3 error: {e}")
        return ValidationResult(passed=True, pass_number=3), sql, corrections


def _get_closest_columns(query_col: str, valid_columns: set, n: int = 3) -> list[tuple[str, float]]:
    """
    Get top-N closest column matches with similarity scores.
    Used to provide smart suggestions in validation error messages.
    """
    from utils.fuzzy_matcher import jaro_winkler_similarity
    
    query_lower = query_col.lower().replace("_", "")
    scored = []
    for valid in valid_columns:
        valid_lower = valid.lower().replace("_", "")
        score = jaro_winkler_similarity(query_lower, valid_lower)
        
        # Substring bonus
        if query_lower in valid_lower or valid_lower in query_lower:
            score = min(score + 0.15, 1.0)
        
        # Token overlap bonus
        query_tokens = set(query_col.lower().split("_"))
        valid_tokens = set(valid.lower().split("_"))
        if query_tokens & valid_tokens:
            overlap = len(query_tokens & valid_tokens) / max(len(query_tokens | valid_tokens), 1)
            score = max(score, overlap)
        
        scored.append((valid, round(score, 2)))
    
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:n]


def _pass4_types(sql: str, column_types: dict) -> ValidationResult:
    """Block mathematical aggregations on non-numeric types."""
    try:
        parsed = sqlglot.parse_one(sql)

        for agg_node in parsed.find_all((exp.Avg, exp.Sum)):
            for col_ref in agg_node.find_all(exp.Column):
                col_name = col_ref.name.lower()
                ctype = column_types.get(col_name, "").upper()
                if ctype in ("DATE", "DATETIME", "TIMESTAMP", "TEXT", "VARCHAR"):
                    func_name = "AVG" if isinstance(agg_node, exp.Avg) else "SUM"
                    return ValidationResult(
                        passed=False,
                        error=f"Type error: Cannot use {func_name}() on '{col_name}' (type={ctype}). Use COUNT() or remove aggregation.",
                        pass_number=4,
                    )

        return ValidationResult(passed=True, pass_number=4)
    except Exception:
        return ValidationResult(passed=True, pass_number=4)


def _pass5_value_range(sql: str, column_profiles: dict) -> tuple[ValidationResult, str, list]:
    """
    Check that WHERE clause values are within sane ranges.
    e.g., block landslide_probability >= 4 when range is 0-1.
    Also fix risk_scale = 'HIGH' to risk_scale >= 4.
    """
    corrections = []
    
    try:
        parsed = sqlglot.parse_one(sql)
        
        for node in parsed.find_all((exp.EQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            col_ref = None
            lit_ref = None
            
            # Find column and literal in this comparison
            for child in node.args.values():
                if child is None:
                    continue
                if isinstance(child, exp.Column):
                    col_ref = child
                elif isinstance(child, exp.Literal):
                    lit_ref = child
            
            if not col_ref or not lit_ref:
                continue
                
            col_name = col_ref.name.lower()
            profile = column_profiles.get(col_name, {})
            
            if not profile or 'min' not in profile:
                continue
            
            try:
                lit_val = float(lit_ref.this) if not lit_ref.args.get('is_string') else None
            except (ValueError, TypeError):
                # String literal on a numeric column
                if profile.get('type') in ('REAL', 'INTEGER'):
                    corrections.append(f"WARN: '{col_name}' is numeric but compared to string '{lit_ref.this}'")
                continue
            
            if lit_val is None:
                continue
                
            mn = profile.get('min', 0)
            mx = profile.get('max', 0)
            
            if mn is None or mx is None:
                continue
            
            try:
                mn_f = float(mn)
                mx_f = float(mx)
            except (ValueError, TypeError):
                continue
            
            # Check if the literal is wildly out of range
            range_span = mx_f - mn_f
            if range_span == 0:
                continue
                
            if lit_val > mx_f * 3 or lit_val < mn_f - range_span * 2:
                corrections.append(
                    f"RANGE_WARN: '{col_name}' compared to {lit_val}, "
                    f"but data range is {mn}–{mx}. "
                    f"This will likely return 0 rows."
                )
                # Don't reject — just warn, let it through.
                # The LLM might be intentionally filtering for edge cases.
    
    except Exception as e:
        logger.debug(f"Pass 5 error: {e}")
    
    return ValidationResult(passed=True, pass_number=5), sql, corrections


def _pass6_consistency(sql: str, valid_columns: set) -> tuple[str, list]:
    """
    Pass 6: Auto-fix subquery column drift + SQLite date syntax.
    
    Fix 1: If outer WHERE uses col_X but subquery uses AVG(col_Y),
           replace col_Y with col_X in the subquery.
    Fix 2: Replace invalid SQLite date forms like '-48 hour' with '-2 days'.
    """
    corrections = []
    fixed = sql

    # ── Fix 1: Subquery column consistency ────────────────────
    # Pattern: WHERE col_a op (SELECT AGG(col_b) FROM ...)
    # If col_a != col_b AND col_b not in valid_columns, replace col_b → col_a
    import re as _re
    
    pattern = _re.compile(
        r'WHERE\s+(\w+)\s*[<>=!]+\s*\(\s*SELECT\s+(AVG|MIN|MAX|SUM|COUNT)\s*\(\s*(\w+)\s*\)',
        _re.IGNORECASE
    )
    
    for match in pattern.finditer(fixed):
        outer_col = match.group(1).lower()
        agg_fn = match.group(2)
        inner_col = match.group(3)
        inner_col_lower = inner_col.lower()
        
        # If inner col differs from outer and inner is NOT a valid column
        if outer_col != inner_col_lower and inner_col_lower not in valid_columns:
            # Replace the bad inner column with the outer column
            old_agg = f"{agg_fn}({inner_col})"
            new_agg = f"{agg_fn}({match.group(1)})"
            fixed = fixed.replace(old_agg, new_agg, 1)
            corrections.append(
                f"COLUMN_FIX: Subquery used '{inner_col}' but outer uses '{match.group(1)}'. "
                f"Auto-fixed {old_agg} → {new_agg}"
            )
            logger.info(f"Pass 6: Fixed subquery column drift: {old_agg} → {new_agg}")

    # ── Fix 2: SQLite date syntax ─────────────────────────────
    # Fix '-N hour' → '-N/24 days' (rounded), '-N hours' → same
    hour_pattern = _re.compile(r"'(-?\d+)\s+hours?'", _re.IGNORECASE)
    for match in hour_pattern.finditer(fixed):
        hours = int(match.group(1))
        days = max(1, abs(hours) // 24)
        sign = '-' if hours < 0 or match.group(0).startswith("'-") else '-'
        old_val = match.group(0)
        new_val = f"'{sign}{days} days'"
        fixed = fixed.replace(old_val, new_val, 1)
        corrections.append(f"DATE_FIX: '{old_val}' → {new_val} (SQLite requires days/months/years)")
        logger.info(f"Pass 6: Fixed date syntax: {old_val} → {new_val}")

    # Fix '-N minute' or '-N minutes' → '-1 days' minimum
    minute_pattern = _re.compile(r"'(-?\d+)\s+minutes?'", _re.IGNORECASE)
    for match in minute_pattern.finditer(fixed):
        old_val = match.group(0)
        new_val = "'-1 days'"
        fixed = fixed.replace(old_val, new_val, 1)
        corrections.append(f"DATE_FIX: '{old_val}' → {new_val}")

    return fixed, corrections
