"""
Agent 1: Query Understanding Engine — THE BRAIN.

Two-phase approach:
  Phase A (Zero LLM): Abbreviation expansion, fuzzy entity matching,
                       intent classification, ambiguity detection
  Phase B (1 LLM Call): Chain-of-Thought decomposition into structured JSON plan

This agent transforms vague user language into a precise execution plan
that Agent 2 (pruner) and Agent 3 (generator) can directly consume.
"""
import json
import re
import time
import logging
from config import MedicalConfig as Config
from models.llm_manager import llm_manager
from utils.domain_dictionary import ABBREVIATIONS, BUSINESS_TERM_HINTS, GROUP_KEYWORDS
from utils.fuzzy_matcher import find_column_matches
from utils.text_utils import tokenize, extract_numbers

logger = logging.getLogger("nl2sql.agent1_understanding")


# ── Intent patterns ──────────────────────────────────────────
INTENT_PATTERNS = {
    "RANKING": {
        "keywords": ["top", "highest", "lowest", "best", "worst", "most", "least", "rank", "maximum", "minimum"],
        "sql_hint": "Use ORDER BY ... DESC/ASC LIMIT N",
    },
    "AGGREGATION": {
        "keywords": ["average", "avg", "mean", "total", "sum", "count", "how many", "minimum", "maximum", "min", "max"],
        "sql_hint": "Use aggregate functions: AVG(), SUM(), COUNT(), MIN(), MAX()",
    },
    "GROUPBY": {
        "keywords": ["each", "per", "by", "group", "for every", "breakdown", "distribution", "category"],
        "sql_hint": "Use GROUP BY clause",
    },
    "COMPARISON": {
        "keywords": ["higher than average", "above average", "below average",
                      "more than", "less than", "compared to", "greater than",
                      "higher than", "lower than"],
        "sql_hint": "Use subquery: WHERE col > (SELECT AVG(col) FROM table)",
    },
    "TEMPORAL": {
        "keywords": ["date", "when", "recent", "latest", "today", "this week",
                      "last month", "trend", "over time", "yesterday", "last"],
        "sql_hint": "Use prediction_date column with date functions",
    },
    "FILTER": {
        "keywords": ["where", "find", "show", "get", "which", "list", "display", "records"],
        "sql_hint": "Use WHERE clause with conditions",
    },
}

# ── Chain-of-Thought Prompt ──────────────────────────────────
COT_PROMPT = """You are a data analyst. Your task is to decompose a natural language query into a precise SQL execution plan.

The database has table(s) with columns organized in these groups:
{group_summary}

USER QUERY: "{query}"

PRE-ANALYSIS (already computed, use these as strong hints):
- Expanded query: "{expanded_query}"
- Fuzzy column matches: {fuzzy_matches}
- Detected intent: {intent_types}
- Business hints: {business_hints}

INSTRUCTIONS:
1. Think step-by-step about what the user actually wants
2. Resolve ANY ambiguous terms to specific column names from the groups above
3. Determine which column groups are needed
4. Identify exact column names, filters, aggregations, ordering

OUTPUT ONLY valid JSON (no explanation, no markdown):
{{
  "reasoning": "step-by-step logic of what the user wants and how to get it",
  "resolved_question": "unambiguous rewrite using exact column names",
  "target_groups": ["group1", "group2"],
  "target_columns": ["exact_column_name1", "exact_column_name2"],
  "filter_conditions": [
    {{"column": "col_name", "operator": ">=", "value": "4"}}
  ],
  "aggregation": null,
  "group_by": null,
  "order_by": null,
  "limit": null
}}"""


class QueryUnderstandingEngine:
    """Agent 1: Transforms vague NL into structured execution plan."""

    def __init__(self, column_metadata: list[dict] = None, column_profiles: dict = None):
        self.column_metadata = column_metadata or []
        self.column_profiles = column_profiles or {}  # Live data profiles for semantic thresholds
        self.abbreviations = ABBREVIATIONS
        self._group_columns = {}  # group_name -> [column_names]
        if column_metadata:
            self._build_group_index()

    def set_column_metadata(self, metadata: list[dict], profiles: dict = None):
        """Set/update column metadata (called after schema load)."""
        self.column_metadata = metadata
        if profiles:
            self.column_profiles = profiles
        self._build_group_index()

    def _build_group_index(self):
        """Index columns by group for fast group-level lookup."""
        self._group_columns = {}
        for col in self.column_metadata:
            group = col.get("group", "Other")
            if group not in self._group_columns:
                self._group_columns[group] = []
            self._group_columns[group].append(col["name"])

    async def understand(self, query: str) -> dict:
        """
        Full understanding pipeline:
        Phase A: Deterministic pre-processing (instant)
        Phase B: LLM Chain-of-Thought (1 call)
        
        Returns structured plan dict.
        """
        start = time.time()

        # ── Phase A: Pre-LLM (zero cost) ─────────────────────
        phase_a = self._phase_a(query)

        # ── Phase B: LLM Chain-of-Thought ────────────────────
        phase_b = await self._phase_b(query, phase_a)

        elapsed_ms = int((time.time() - start) * 1000)

        # ── CRITICAL: Validate target_columns from LLM ───────
        # The LLM often hallucmates column names like "snow_depth_change"
        # or "wind_speed_24h" that don't exist. We scrub them NOW
        # before they propagate to Agent 2 (pruner) and Agent 3 (generator).
        raw_target_cols = phase_b.get("target_columns", [])
        validated_cols = self._validate_target_columns(raw_target_cols)
        if len(validated_cols) != len(raw_target_cols):
            dropped = set(raw_target_cols) - set(validated_cols)
            if dropped:
                logger.warning(
                    f"Agent 1: DROPPED hallucinated columns from LLM: {dropped}"
                )

        # Also validate filter_conditions column names
        validated_filters = self._validate_filter_columns(
            phase_b.get("filter_conditions", [])
        )

        # Merge Phase A + Phase B into final plan
        plan = {
            "original_query": query,
            "expanded_query": phase_a["expanded_query"],
            "intent_types": phase_a["intent_types"],
            "intent_hints": phase_a["intent_hints"],
            "fuzzy_matches": phase_a["fuzzy_matches"],
            "business_hints": phase_a["business_hints"],
            "detected_limit": phase_a["detected_limit"],
            "decomposed_parts": phase_a.get("decomposed_parts", []),
            "semantic_conditions": phase_a.get("semantic_conditions", []),
            # From LLM — VALIDATED against real schema
            "reasoning": phase_b.get("reasoning", ""),
            "resolved_question": phase_b.get("resolved_question", phase_a["expanded_query"]),
            "target_groups": phase_b.get("target_groups", phase_a["detected_groups"]),
            "target_columns": validated_cols,
            "filter_conditions": validated_filters,
            "aggregation": phase_b.get("aggregation"),
            "group_by": phase_b.get("group_by"),
            "order_by": phase_b.get("order_by"),
            "limit": phase_b.get("limit", phase_a["detected_limit"]),
            "understanding_time_ms": elapsed_ms,
        }

        logger.info(
            f"Agent 1 complete: intent={plan['intent_types']}, "
            f"groups={plan['target_groups']}, "
            f"cols={len(plan['target_columns'])}, "
            f"time={elapsed_ms}ms"
        )
        return plan

    def _phase_a(self, query: str) -> dict:
        """
        Phase A — Deterministic pre-processing. Zero LLM calls.
        
        1. Abbreviation expansion
        2. Fuzzy entity matching against all column synonyms
        3. Intent classification
        4. Group detection
        5. Business hint extraction
        6. Query decomposition (split 'X but Y')
        7. Semantic condition injection (map terms -> structured filters)
        """
        # 1. Abbreviation expansion
        expanded = self._expand_abbreviations(query)

        # 2. Fuzzy entity matching
        query_terms = self._extract_query_phrases(expanded)
        fuzzy_matches = {}
        if self.column_metadata:
            fuzzy_matches = find_column_matches(query_terms, self.column_metadata, threshold=0.78)

        # 3. Intent classification
        intent_types, intent_hints = self._classify_intent(expanded)

        # 4. Group detection
        detected_groups = self._detect_groups(expanded, fuzzy_matches)

        # 5. Business hints
        business_hints = self._get_business_hints(expanded)

        # 6. Query decomposition — split compound queries
        decomposed = self._decompose_compound_query(expanded)

        # 7. Semantic condition injection — convert vague terms to structured conditions
        semantic_conditions = self._inject_semantic_conditions(expanded, fuzzy_matches)

        # 8. Number extraction (for LIMIT)
        numbers = extract_numbers(query)
        detected_limit = numbers[0] if numbers else None

        return {
            "expanded_query": expanded,
            "fuzzy_matches": fuzzy_matches,
            "intent_types": intent_types,
            "intent_hints": intent_hints,
            "detected_groups": detected_groups,
            "business_hints": business_hints,
            "detected_limit": detected_limit,
            "decomposed_parts": decomposed,
            "semantic_conditions": semantic_conditions,
        }

    async def _phase_b(self, original_query: str, phase_a: dict) -> dict:
        """Phase B — LLM Chain-of-Thought decomposition. 1 LLM call."""
        # Build group summary for prompt
        group_summary_lines = []
        for group, cols in self._group_columns.items():
            col_list = ", ".join(cols[:10])
            suffix = f" ... (+{len(cols)-10} more)" if len(cols) > 10 else ""
            group_summary_lines.append(f"  {group} ({len(cols)} cols): {col_list}{suffix}")
        group_summary = "\n".join(group_summary_lines)

        # Format fuzzy matches for prompt
        fuzzy_str = json.dumps(phase_a["fuzzy_matches"], indent=2) if phase_a["fuzzy_matches"] else "None found"

        # Format business hints
        hints_str = "; ".join(phase_a["business_hints"][:5]) if phase_a["business_hints"] else "None"

        prompt = COT_PROMPT.format(
            group_summary=group_summary,
            query=original_query,
            expanded_query=phase_a["expanded_query"],
            fuzzy_matches=fuzzy_str,
            intent_types=", ".join(phase_a["intent_types"]),
            business_hints=hints_str,
        )

        try:
            raw = await llm_manager.reason(
                prompt=prompt,
            )
            result = self._parse_cot_response(raw)
            result["_cot_prompt"] = prompt
            result["_llm_raw"] = raw
            logger.info(f"Agent 1 Phase B: LLM reasoning complete")
            return result
        except Exception as e:
            logger.error(f"Agent 1 Phase B failed: {e}")
            return {
                "reasoning": "LLM unavailable — using deterministic analysis only",
                "resolved_question": phase_a["expanded_query"],
                "target_groups": phase_a["detected_groups"],
                "target_columns": [m["column_name"] for m in phase_a["fuzzy_matches"].values()],
                "filter_conditions": [],
                "aggregation": None,
                "group_by": None,
                "order_by": None,
                "limit": phase_a["detected_limit"],
                "_cot_prompt": prompt,
                "_llm_raw": str(e),
            }

    def _expand_abbreviations(self, query: str) -> str:
        """Expand military and scientific abbreviations."""
        words = query.split()
        expanded = []
        for word in words:
            prefix, suffix = "", ""
            clean = word
            while clean and not clean[0].isalnum():
                prefix += clean[0]
                clean = clean[1:]
            while clean and not clean[-1].isalnum():
                suffix = clean[-1] + suffix
                clean = clean[:-1]

            lower = clean.lower()
            if lower in self.abbreviations:
                expanded.append(prefix + self.abbreviations[lower] + suffix)
            else:
                expanded.append(word)
        return " ".join(expanded)

    def _extract_query_phrases(self, query: str) -> list[str]:
        """Extract meaningful phrases from query for fuzzy matching."""
        # Clean query
        q = re.sub(r'[^\w\s]', ' ', query.lower())
        words = q.split()

        phrases = []
        # Add individual words (filtered)
        for w in words:
            if len(w) > 2 and w not in {"the", "and", "for", "are", "was", "show", "find", "get", "list", "what", "how", "all", "with"}:
                phrases.append(w)

        # Add bigrams (two-word phrases)
        for i in range(len(words) - 1):
            bigram = f"{words[i]} {words[i+1]}"
            phrases.append(bigram)

        # Add trigrams
        for i in range(len(words) - 2):
            trigram = f"{words[i]} {words[i+1]} {words[i+2]}"
            phrases.append(trigram)

        return phrases

    def _classify_intent(self, query: str) -> tuple[list[str], list[str]]:
        """Classify query intent using keyword patterns."""
        q_lower = query.lower()
        types = []
        hints = []

        for itype, info in INTENT_PATTERNS.items():
            for kw in info["keywords"]:
                if kw in q_lower:
                    if itype not in types:
                        types.append(itype)
                        hints.append(info["sql_hint"])
                    break

        if not types:
            types = ["FILTER"]
            hints = ["Use WHERE clause"]

        return types, hints

    def _detect_groups(self, query: str, fuzzy_matches: dict) -> list[str]:
        """Detect which column groups are relevant based on query + fuzzy matches."""
        q_lower = query.lower()
        detected = set()

        # From keyword matching
        for group, keywords in GROUP_KEYWORDS.items():
            for kw in keywords:
                if kw in q_lower:
                    detected.add(group)
                    break

        # From fuzzy matches — find which groups the matched columns belong to
        matched_cols = {m["column_name"] for m in fuzzy_matches.values()}
        for col in self.column_metadata:
            if col["name"] in matched_cols:
                detected.add(col.get("group", "Other"))

        # If nothing specific detected, include ALL groups (schema-agnostic)
        if not detected:
            detected = set(self._group_columns.keys())

        return list(detected)

    def _get_business_hints(self, query: str) -> list[str]:
        """Extract business term hints relevant to the query."""
        hints = []
        q_lower = query.lower()
        # Sort by length descending so longer phrases match first
        sorted_terms = sorted(BUSINESS_TERM_HINTS.keys(), key=len, reverse=True)
        for term in sorted_terms:
            if term in q_lower:
                hints.append(f"'{term}': {BUSINESS_TERM_HINTS[term]}")
        return hints

    def _parse_cot_response(self, raw: str) -> dict:
        """Parse the LLM's JSON response robustly."""
        raw = raw.strip()

        # Remove markdown code fences
        if "```" in raw:
            match = re.search(r"```(?:json)?\s*\n?(.*?)```", raw, re.DOTALL)
            if match:
                raw = match.group(1).strip()

        # Find JSON object
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            raw = json_match.group(0)

        try:
            result = json.loads(raw)
            return {
                "reasoning": result.get("reasoning", ""),
                "resolved_question": result.get("resolved_question", ""),
                "target_groups": result.get("target_groups", []),
                "target_columns": result.get("target_columns", []),
                "filter_conditions": result.get("filter_conditions", []),
                "aggregation": result.get("aggregation"),
                "group_by": result.get("group_by"),
                "order_by": result.get("order_by"),
                "limit": result.get("limit"),
            }
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse CoT JSON: {raw[:200]}")
            return {"reasoning": raw[:500]}

    # ── NEW: Query Decomposition Engine ──────────────────────
    def _decompose_compound_query(self, query: str) -> list[str]:
        """
        Split compound queries at conjunctions BEFORE LLM.
        'X but Y' → ['X', 'Y']
        'A and B are high' → left as-is (both modify same subject)
        """
        q = query.lower().strip()
        
        # Patterns that indicate logical splits
        split_patterns = [
            r'(.+?)\s+(?:but|however|yet|though|even though|while|although)\s+(.+)',
        ]
        
        for pat in split_patterns:
            m = re.match(pat, q, re.IGNORECASE)
            if m:
                parts = [m.group(1).strip(), m.group(2).strip()]
                logger.info(f"Decomposed query into {len(parts)} parts: {parts}")
                return parts
        
        return [q]

    # ── Column Name Validation Gate ───────────────────────────
    def _validate_target_columns(self, columns: list) -> list[str]:
        """
        Scrub LLM-hallucinated column names from Agent 1 Phase B output.
        
        WHY: The LLM sees a group summary with 10 columns per group and
        guesses names like "snow_depth_change" or "wind_speed_24h" that
        don't exist. These flow downstream and cause SQL failures.
        
        ALGORITHM:
        1. Exact match (case-insensitive) → keep
        2. Fuzzy match (≥ 0.70) → replace with closest real column
        3. No match → DROP from list (don't let hallucinations propagate)
        """
        if not columns or not self.column_metadata:
            return columns if isinstance(columns, list) else []
        
        all_names = [c["name"] for c in self.column_metadata]
        all_lower = {c.lower(): c for c in all_names}
        
        validated = []
        for col in columns:
            if not isinstance(col, str):
                continue
            col_lower = col.lower()
            
            # Check 1: Exact match (case-insensitive)
            if col_lower in all_lower:
                validated.append(all_lower[col_lower])
                continue
            
            # Check 2: Underscore-normalized match
            col_normalized = col_lower.replace("_", "")
            for real_name in all_names:
                if real_name.lower().replace("_", "") == col_normalized:
                    validated.append(real_name)
                    logger.info(f"Agent 1 column fix (normalized): '{col}' → '{real_name}'")
                    break
            else:
                # Check 3: Fuzzy match
                from utils.fuzzy_matcher import find_best_match
                best, score = find_best_match(col, all_names, threshold=0.70)
                if best:
                    validated.append(best)
                    logger.info(f"Agent 1 column fix (fuzzy): '{col}' → '{best}' (score={score:.2f})")
                else:
                    # DROP — this column is a hallucination
                    logger.warning(f"Agent 1: DROPPED hallucinated column '{col}' — no match found")
        
        # Remove duplicates while preserving order
        seen = set()
        deduped = []
        for c in validated:
            if c.lower() not in seen:
                seen.add(c.lower())
                deduped.append(c)
        
        return deduped

    def _validate_filter_columns(self, filters: list) -> list:
        """
        Validate column names inside filter_conditions from Phase B.
        Fix or drop filters with hallucinated column names.
        """
        if not filters or not self.column_metadata:
            return filters if isinstance(filters, list) else []
        
        all_names = [c["name"] for c in self.column_metadata]
        all_lower = {c.lower(): c for c in all_names}
        
        validated = []
        for f in filters:
            if not isinstance(f, dict):
                validated.append(f)
                continue
            
            col = f.get("column", "")
            if not col:
                validated.append(f)
                continue
            
            col_lower = col.lower()
            if col_lower in all_lower:
                f["column"] = all_lower[col_lower]
                validated.append(f)
            else:
                # Try fuzzy match
                from utils.fuzzy_matcher import find_best_match
                best, score = find_best_match(col, all_names, threshold=0.70)
                if best:
                    logger.info(f"Agent 1 filter fix: '{col}' → '{best}' (score={score:.2f})")
                    f["column"] = best
                    validated.append(f)
                else:
                    logger.warning(f"Agent 1: DROPPED filter with hallucinated column '{col}'")
        
        return validated

    # ── Semantic Soft Hints (NOT hard filters) ─────────────────
    def _inject_semantic_conditions(self, query: str, fuzzy_matches: dict) -> list[dict]:
        """
        Convert vague domain terms into SOFT HINT strings.
        
        CRITICAL DESIGN DECISION: These are returned as hint text for the LLM
        to consider, NOT as pre-baked WHERE conditions. Soft hints let the LLM
        reason about which column is most appropriate for the user's intent.
        """
        q_lower = query.lower()
        hints = []
        
        SEMANTIC_HINTS = {
            # Risk / Danger — map to landslide columns
            "high risk": "'high risk' means: landslide_probability > 0.7 OR prediction = 1.",
            "low risk": "'low risk' means: landslide_probability < 0.3 AND prediction = 0.",
            "moderate risk": "'moderate risk' means: landslide_probability BETWEEN 0.4 AND 0.7.",
            "extreme": "'extreme' means: landslide_probability > 0.9 AND prediction = 1.",
            "dangerous": "'dangerous' means landslide_probability > 0.7 OR prediction = 1.",
            # Safety
            "safe": "'safe' means prediction = 0 OR landslide_probability < 0.3.",
            "stable": "'stable' means minimal change — look for low variation in values.",
            # Precipitation
            "heavy rain": "'heavy rain' means Daily_Precipitation > 50 mm.",
            "light rain": "'light rain' means Daily_Precipitation BETWEEN 1 AND 10.",
            "no rain": "'no rain' means Daily_Precipitation = 0.",
            # Temperature
            "cold": "'cold' means Surface_Temperature_T2M < 0°C.",
            "warm": "'warm' means Surface_Temperature_T2M > 20°C.",
            # Terrain
            "steep": "'steep' means slope > 0.5236 (30 degrees in radians).",
            "gentle": "'gentle' means slope < 0.3491 (20 degrees in radians).",
            # Moisture
            "wet": "'wet' means Surface_Soil_Moisture > 30 or Relative_Humidity > 80.",
            "dry": "'dry' means Daily_Precipitation = 0 AND Relative_Humidity < 40.",
            "saturated": "'saturated' soil means Surface_Soil_Moisture > 40 OR Root_Zone_Soil_Moisture > 80.",
            # Wind
            "windy": "'windy' means Daily_Wind_Speed > 10 m/s OR Max_Gust > 15.",
            # Average comparisons
            "above average": "Use subquery: WHERE col > (SELECT AVG(col) FROM prediction). Use the SAME column in both.",
            "below average": "Use subquery: WHERE col < (SELECT AVG(col) FROM prediction). Use the SAME column in both.",
            "higher than average": "Use subquery: WHERE col > (SELECT AVG(col) FROM prediction).",
            "above normal": "'above normal' means > average. Use AVG() subquery.",
            # Contradictions
            "hidden": "Model as contradiction: WHERE one_indicator < avg AND another_indicator > avg.",
            "anomaly": "Look for contradictory signals: e.g., WHERE col_a is low AND col_b is high.",
            "contradictory": "Use WHERE condition_a AND opposite_condition_b pattern.",
        }
        
        matched_hints = []
        # Sort by length descending so longer phrases match first
        for phrase in sorted(SEMANTIC_HINTS.keys(), key=len, reverse=True):
            if phrase in q_lower:
                matched_hints.append(SEMANTIC_HINTS[phrase])
                logger.info(f"Semantic soft hint: '{phrase}'")
        
        return matched_hints

    # ── Query Complexity Classification ───────────────────────
    def classify_complexity(self, query: str, intent_types: list) -> str:
        """
        Classify query complexity for adaptive prompting.
        
        EASY: Simple select, basic filter, show rows. → Ultra-minimal prompt.
        MEDIUM: Aggregation, grouping, ranking with filter. → Standard prompt.
        HARD: Subqueries, comparisons vs averages, temporal+filter, contradictions. → Full decomposition.
        
        WHY: DIN-SQL research proves simple queries perform WORSE with complex
        prompts because the model gets confused by irrelevant rules/skeletons.
        "show top 10 rows" should NOT trigger a 2500-token prompt with 13 rules.
        """
        q = query.lower()
        
        # HARD signals: subqueries, comparisons against averages, contradictions
        hard_signals = [
            "than average", "above average", "below average",
            "higher than usual", "lower than usual", "compared to",
            "worse than", "better than",
        ]
        # Compound conditions (X but Y, high X low Y)
        if re.search(r'\b(but|however|yet|while|although)\b', q):
            return "HARD"
        for sig in hard_signals:
            if sig in q:
                return "HARD"
        # Temporal + filter combo
        has_temporal = any(w in q for w in ["last", "recent", "days", "week", "month", "trend"])
        has_filter = any(w in q for w in ["where", "greater", "less", "more", "above", "below"])
        if has_temporal and has_filter:
            return "HARD"
        
        # MEDIUM signals: aggregation, grouping, ranking with specifics
        medium_signals = [
            "average", "avg", "mean", "count", "how many",
            "total", "sum", "minimum", "maximum",
            "group by", "each", "per", "breakdown",
            "highest", "lowest", "best", "worst",
        ]
        for sig in medium_signals:
            if sig in q:
                return "MEDIUM"
        if "AGGREGATION" in intent_types or "GROUPBY" in intent_types or "RANKING" in intent_types:
            return "MEDIUM"
        
        # EASY: everything else (simple selects, show rows, basic filters)
        return "EASY"
