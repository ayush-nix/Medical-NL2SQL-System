"""
Fuzzy String Matcher — Pure-Python Jaro-Winkler + Levenshtein.
Zero external dependencies.

Used for:
1. Matching vague user terms to column names/synonyms
2. Auto-correcting hallucinated column names in generated SQL
"""


def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Compute the Levenshtein (edit) distance between two strings.
    Number of single-character insertions, deletions, or substitutions
    needed to transform s1 into s2.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[-1]


def levenshtein_ratio(s1: str, s2: str) -> float:
    """Normalized Levenshtein similarity (0.0 to 1.0, higher = more similar)."""
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 1.0
    return 1.0 - (levenshtein_distance(s1, s2) / max_len)


def jaro_similarity(s1: str, s2: str) -> float:
    """
    Compute Jaro similarity between two strings.
    Returns value between 0.0 (no similarity) and 1.0 (identical).
    """
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0

    # Maximum distance for matching
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0

    # Find matching characters
    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    # Count transpositions
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3
    return jaro


def jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
    """
    Compute Jaro-Winkler similarity.
    Gives bonus weight to common prefixes (up to 4 chars).
    
    p: scaling factor for prefix bonus (default 0.1, max 0.25)
    
    Better than plain Levenshtein for column names sharing prefixes:
    e.g., snowdepth vs snow_depth → high similarity
    """
    jaro = jaro_similarity(s1, s2)

    # Common prefix (up to 4 characters)
    prefix_len = 0
    for i in range(min(len(s1), len(s2), 4)):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * p * (1 - jaro)


def find_best_match(query: str, candidates: list[str],
                    threshold: float = 0.75) -> tuple[str, float]:
    """
    Find the best matching string from candidates.
    
    Uses Jaro-Winkler for ranking (better for prefix-heavy column names).
    
    Args:
        query: the string to match
        candidates: list of valid strings to match against
        threshold: minimum similarity score to accept
    
    Returns:
        (best_match, score) or ("", 0.0) if no match above threshold
    """
    query_lower = query.lower().replace("_", "").replace("-", "")
    best_match = ""
    best_score = 0.0

    for candidate in candidates:
        cand_lower = candidate.lower().replace("_", "").replace("-", "")

        # Exact match (ignoring underscores/hyphens)
        if query_lower == cand_lower:
            return candidate, 1.0

        score = jaro_winkler_similarity(query_lower, cand_lower)

        # Bonus for substring containment
        if query_lower in cand_lower or cand_lower in query_lower:
            score = min(score + 0.15, 1.0)

        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score >= threshold:
        return best_match, best_score
    return "", 0.0


def find_column_matches(query_terms: list[str], column_metadata: list[dict],
                        threshold: float = 0.78) -> dict[str, dict]:
    """
    Match user query terms against column names, display names, and synonyms.
    
    Returns dict of {matched_term: {column_name, match_type, score, condition}}
    
    This is the core of how vague user language maps to exact column names.
    A user saying "temperature" will match column "temp_2m_celsius" via synonym.
    A user saying "how cold" will match via synonym "how cold" → temp_2m_celsius.
    """
    matches = {}

    for term in query_terms:
        term_lower = term.lower()
        best_col = ""
        best_score = 0.0
        best_type = ""
        best_condition = None

        for col in column_metadata:
            col_name = col.get("name", "")

            # Check against column name
            score = jaro_winkler_similarity(
                term_lower.replace("_", ""),
                col_name.lower().replace("_", "")
            )
            if score > best_score:
                best_score = score
                best_col = col_name
                best_type = "column_name"

            # Check against display name
            display = col.get("display", "")
            if display:
                score = jaro_winkler_similarity(term_lower, display.lower())
                if score > best_score:
                    best_score = score
                    best_col = col_name
                    best_type = "display_name"

            # Check against synonyms (phrase-level matching)
            for syn in col.get("synonyms", []):
                syn_lower = syn.lower()
                # Exact substring match gets highest score
                if term_lower == syn_lower:
                    best_score = 1.0
                    best_col = col_name
                    best_type = "synonym_exact"
                    break
                elif term_lower in syn_lower or syn_lower in term_lower:
                    score = 0.92
                    if score > best_score:
                        best_score = score
                        best_col = col_name
                        best_type = "synonym_partial"
                else:
                    score = jaro_winkler_similarity(term_lower, syn_lower)
                    if score > best_score:
                        best_score = score
                        best_col = col_name
                        best_type = "synonym_fuzzy"

            # Check against enum values
            for enum_val in col.get("enum", []):
                if str(enum_val).lower() == term_lower:
                    best_score = 1.0
                    best_col = col_name
                    best_type = "enum_value"
                    best_condition = f"= '{enum_val}'" if isinstance(enum_val, str) else f"= {enum_val}"
                    break

            if best_score >= 1.0:
                break

        if best_score >= threshold:
            matches[term] = {
                "column_name": best_col,
                "match_type": best_type,
                "score": round(best_score, 3),
                "condition": best_condition,
            }

    return matches
