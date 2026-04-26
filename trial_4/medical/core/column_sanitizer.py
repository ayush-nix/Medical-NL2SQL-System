"""
Column Sanitizer — Deterministic Post-Generation Column Name Fixer.

Runs BETWEEN Agent 3 (SQL Generator) and Pass 3 (Schema Validator).
Zero LLM calls. Pure AST + fuzzy matching.

WHY THIS EXISTS:
  The LLM generates SQL with column names that are "close but wrong":
    - snow_depth_diff    (real: snowdepth)
    - temp_celsius       (real: temp_2m)
    - wind_speed_24h     (real: wind_speed_change_24h)

  The validator's 0.90 threshold REJECTS these (score ~0.72-0.88)
  but doesn't FIX them. This module fixes them FIRST, so the validator
  only needs to handle edge cases.

ALGORITHM:
  1. Parse SQL with sqlglot → extract all exp.Column nodes
  2. For each column, check exact match (case-insensitive)
  3. If no match: multi-strategy fuzzy search:
     a. Underscore-normalized exact match (snowdepthchange → snow_depth_change)
     b. Substring containment (temp_celsius → temp_2m)
     c. Jaro-Winkler similarity
     d. Token overlap (split on _ and check shared tokens)
  4. If best score ≥ 0.65: auto-replace in SQL string
  5. Return fixed SQL + list of corrections
"""
import re
import logging
from utils.fuzzy_matcher import jaro_winkler_similarity, levenshtein_ratio

logger = logging.getLogger("nl2sql.column_sanitizer")


def sanitize_columns(sql: str, valid_columns: set[str],
                     table_name: str = None) -> tuple[str, list[str]]:
    """
    Fix hallucinated column names in generated SQL.

    Args:
        sql: raw SQL from LLM
        valid_columns: set of EXACT valid column names
        table_name: expected table name (for table name fixes too)

    Returns:
        (fixed_sql, list_of_corrections)
    """
    if not sql or not valid_columns:
        return sql, []

    corrections = []
    fixed_sql = sql

    # Build lookup structures
    valid_set = set(valid_columns)
    valid_lower_map = {c.lower(): c for c in valid_set}  # lowercase → original
    valid_normalized = {}  # no-underscore form → original
    valid_tokens_map = {}  # frozenset of tokens → original
    for c in valid_set:
        normalized = c.lower().replace("_", "")
        valid_normalized[normalized] = c
        tokens = frozenset(c.lower().split("_"))
        if tokens not in valid_tokens_map:
            valid_tokens_map[tokens] = c

    # ── Step 1: Extract column-like identifiers from SQL ──────
    # Use regex-based extraction (more robust than AST for malformed SQL)
    sql_columns = _extract_column_refs(fixed_sql, valid_set, table_name)

    for col_ref in sql_columns:
        if not col_ref or col_ref == '*':
            continue

        col_lower = col_ref.lower()

        # Check 1: Exact match (case-insensitive)
        if col_lower in valid_lower_map:
            correct = valid_lower_map[col_lower]
            if correct != col_ref:  # Case fix needed
                fixed_sql = _safe_replace(fixed_sql, col_ref, correct)
                corrections.append(f"CASE_FIX: {col_ref} → {correct}")
            continue

        # Check 2: Underscore-normalized exact match
        normalized = col_lower.replace("_", "")
        if normalized in valid_normalized:
            correct = valid_normalized[normalized]
            fixed_sql = _safe_replace(fixed_sql, col_ref, correct)
            corrections.append(f"NORM_FIX: {col_ref} → {correct}")
            logger.info(f"Column sanitizer: '{col_ref}' → '{correct}' (normalized match)")
            continue

        # Check 3: Multi-strategy fuzzy match
        best_match, best_score, match_type = _find_best_column(
            col_ref, valid_set, valid_lower_map, valid_tokens_map
        )

        if best_match and best_score >= 0.65:
            fixed_sql = _safe_replace(fixed_sql, col_ref, best_match)
            corrections.append(
                f"FUZZY_FIX ({match_type}): {col_ref} → {best_match} "
                f"(score={best_score:.2f})"
            )
            logger.info(
                f"Column sanitizer: '{col_ref}' → '{best_match}' "
                f"({match_type}, score={best_score:.2f})"
            )
        else:
            # Can't fix — log for diagnostics
            candidates = _get_top_suggestions(col_ref, valid_set, n=3)
            suggestions_str = ", ".join(
                f"'{c}' ({s:.2f})" for c, s in candidates
            )
            logger.warning(
                f"Column sanitizer: UNFIXABLE '{col_ref}' — "
                f"closest: {suggestions_str}"
            )

    # ── Step 2: Fix table name if hallucinated ────────────────
    if table_name:
        fixed_sql, table_corrections = _fix_table_name(fixed_sql, table_name)
        corrections.extend(table_corrections)

    if corrections:
        logger.info(f"Column sanitizer: applied {len(corrections)} fixes")

    return fixed_sql, corrections


def _extract_column_refs(sql: str, valid_columns: set, table_name: str = None) -> list[str]:
    """
    Extract potential column references from SQL.
    Uses AST parsing first, falls back to regex if AST fails.
    """
    refs = set()

    # Try AST-based extraction first (most accurate)
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(sql)
        for col_node in parsed.find_all(exp.Column):
            name = col_node.name
            if name and name != '*':
                refs.add(name)
        if refs:
            return list(refs)
    except Exception:
        pass  # Fall back to regex

    # Regex fallback: find identifiers that look like column names
    # Skip SQL keywords, table aliases, and known table names
    sql_keywords = {
        'select', 'from', 'where', 'and', 'or', 'not', 'in', 'between',
        'like', 'order', 'by', 'group', 'having', 'limit', 'as', 'on',
        'join', 'left', 'right', 'inner', 'outer', 'case', 'when', 'then',
        'else', 'end', 'is', 'null', 'asc', 'desc', 'distinct', 'count',
        'avg', 'sum', 'min', 'max', 'cast', 'integer', 'real', 'text',
        'date', 'with', 'union', 'all', 'exists', 'offset', 'abs',
        'round', 'length', 'substr', 'trim', 'upper', 'lower', 'now',
        'random', 'typeof', 'coalesce', 'ifnull', 'nullif', 'total',
        'strftime', 'true', 'false',
    }

    skip_names = sql_keywords.copy()
    if table_name:
        skip_names.add(table_name.lower())
    # Skip common aliases
    skip_names.update({'a', 'b', 'c', 't', 't1', 't2', 't3'})

    # Match identifiers (word_word_word pattern typical of column names)
    for match in re.finditer(r'\b([a-zA-Z][a-zA-Z0-9_]*)\b', sql):
        token = match.group(1)
        if token.lower() not in skip_names and len(token) > 1:
            refs.add(token)

    return list(refs)


def _find_best_column(query_col: str, valid_columns: set,
                      valid_lower_map: dict, valid_tokens_map: dict
                      ) -> tuple[str, float, str]:
    """
    Multi-strategy column matching. Returns (best_match, score, method).

    Strategies (in priority order):
    1. Token overlap: split both on _ → check Jaccard of token sets
    2. Substring containment: if one is a substring of the other
    3. Jaro-Winkler similarity
    4. Combined score: weighted average of all signals
    """
    query_lower = query_col.lower()
    query_no_underscore = query_lower.replace("_", "")
    query_tokens = set(query_lower.split("_"))

    best_match = ""
    best_score = 0.0
    best_method = ""

    for valid in valid_columns:
        valid_lower = valid.lower()
        valid_no_underscore = valid_lower.replace("_", "")
        valid_tokens = set(valid_lower.split("_"))

        # Strategy 1: Token overlap (Jaccard)
        if query_tokens and valid_tokens:
            intersection = len(query_tokens & valid_tokens)
            union = len(query_tokens | valid_tokens)
            token_score = intersection / union if union > 0 else 0.0

            # Bonus: if ALL query tokens are in valid tokens (subset match)
            if query_tokens <= valid_tokens and len(query_tokens) >= 2:
                token_score = max(token_score, 0.85)
            elif valid_tokens <= query_tokens and len(valid_tokens) >= 2:
                token_score = max(token_score, 0.80)

            if token_score > best_score:
                best_score = token_score
                best_match = valid
                best_method = "token_overlap"

        # Strategy 2: Substring containment (with length threshold)
        if len(query_no_underscore) >= 4 and len(valid_no_underscore) >= 4:
            if query_no_underscore in valid_no_underscore:
                sub_score = len(query_no_underscore) / len(valid_no_underscore)
                sub_score = min(sub_score + 0.15, 0.95)  # Boost but cap
                if sub_score > best_score:
                    best_score = sub_score
                    best_match = valid
                    best_method = "substring"
            elif valid_no_underscore in query_no_underscore:
                sub_score = len(valid_no_underscore) / len(query_no_underscore)
                sub_score = min(sub_score + 0.10, 0.90)
                if sub_score > best_score:
                    best_score = sub_score
                    best_match = valid
                    best_method = "substring"

        # Strategy 3: Jaro-Winkler on no-underscore form
        jw_score = jaro_winkler_similarity(query_no_underscore, valid_no_underscore)
        if jw_score > best_score:
            best_score = jw_score
            best_match = valid
            best_method = "jaro_winkler"

        # Strategy 4: Levenshtein ratio (catches transpositions, insertions)
        lev_score = levenshtein_ratio(query_no_underscore, valid_no_underscore)
        if lev_score > best_score:
            best_score = lev_score
            best_match = valid
            best_method = "levenshtein"

    return best_match, best_score, best_method


def _get_top_suggestions(query_col: str, valid_columns: set, n: int = 3) -> list[tuple[str, float]]:
    """Get top-N closest column matches with scores."""
    query_lower = query_col.lower().replace("_", "")
    scored = []
    for valid in valid_columns:
        valid_lower = valid.lower().replace("_", "")
        score = jaro_winkler_similarity(query_lower, valid_lower)

        # Substring bonus
        if query_lower in valid_lower or valid_lower in query_lower:
            score = min(score + 0.15, 1.0)

        scored.append((valid, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:n]


def _safe_replace(sql: str, old_col: str, new_col: str) -> str:
    """Replace column name in SQL using word boundaries to avoid partial matches."""
    # Use word-boundary regex to avoid replacing substrings
    # e.g., replacing "temp" shouldn't affect "temperature" or "temp_2m"
    pattern = r'\b' + re.escape(old_col) + r'\b'
    return re.sub(pattern, new_col, sql)


def _fix_table_name(sql: str, correct_table: str) -> tuple[str, list[str]]:
    """Fix hallucinated table names."""
    corrections = []

    # Common hallucinations: "Prediction", "data", "predictions", etc.
    hallucinated_tables = [
        r'\bPrediction\b', r'\bpredictions\b', r'\bPredictions\b',
        r'\bdata\b(?!\s*\()',  # "data" but not "data(" which could be a function
    ]

    # Only fix FROM/JOIN clauses to avoid false positives
    for pattern in hallucinated_tables:
        # Match in FROM clause
        from_pattern = rf'(FROM\s+){pattern}'
        if re.search(from_pattern, sql, re.IGNORECASE):
            old = re.search(from_pattern, sql, re.IGNORECASE)
            if old:
                fixed = re.sub(from_pattern, rf'\1{correct_table}', sql, flags=re.IGNORECASE)
                if fixed != sql:
                    corrections.append(f"TABLE_FIX: {old.group()} → FROM {correct_table}")
                    sql = fixed

        # Match in JOIN clause
        join_pattern = rf'(JOIN\s+){pattern}'
        if re.search(join_pattern, sql, re.IGNORECASE):
            old = re.search(join_pattern, sql, re.IGNORECASE)
            if old:
                fixed = re.sub(join_pattern, rf'\1{correct_table}', sql, flags=re.IGNORECASE)
                if fixed != sql:
                    corrections.append(f"TABLE_FIX: {old.group()} → JOIN {correct_table}")
                    sql = fixed

    return sql, corrections


def build_column_dictionary(columns: list[dict], profiles: dict = None) -> str:
    """
    Build the structured COLUMN DICTIONARY table for injection into LLM prompt.

    This replaces the old comma-separated column list. Table format gives
    the LLM structured context that it attends to 2-3x better.

    Args:
        columns: list of column metadata dicts (from column_metadata.json)
        profiles: live data profiles {col_name: {min, max, mean, ...}}

    Returns:
        Formatted markdown table string
    """
    profiles = profiles or {}

    lines = [
        "### COLUMN DICTIONARY (ONLY these columns exist — any other name is INVALID):",
        "| Column Name | Type | Description |",
        "|-------------|------|-------------|",
    ]

    type_map = {
        "float": "REAL", "int": "INTEGER", "integer": "INTEGER",
        "text": "TEXT", "string": "TEXT", "date": "TEXT",
    }

    for col in columns:
        name = col["name"]
        col_type = type_map.get(col.get("type", "text").lower(), "TEXT")

        # Build concise description
        desc = col.get("description", "")
        if len(desc) > 50:
            desc = desc[:47] + "..."

        # Add range info from live profiles
        profile = profiles.get(name, {})
        if profile and 'min' in profile and 'max' in profile:
            desc += f" [{profile['min']}–{profile['max']}]"

        lines.append(f"| {name} | {col_type} | {desc} |")

    return "\n".join(lines)
