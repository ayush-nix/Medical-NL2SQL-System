"""
Schema Linker — Entity-to-Column Mapper (Layer 2)

OPTIMIZED VERSION:
1. Uses domain dictionary for instant term→column mapping (no LLM needed)
2. Falls back to a compact LLM prompt only for unmapped terms
3. Rewrites vague user queries with exact column names

This is the CRITICAL layer for accuracy — it prevents the SQL model
from hallucinating column names by giving it exact references.
"""
import json
import logging
import re
from config import Config
from models.llm_manager import llm_manager

logger = logging.getLogger("nl2sql.linker")

# ── Domain term → column mapping (instant, no LLM) ──
# These handle the most common vague user terms
TERM_TO_COLUMN = {
    # Risk / prediction
    "high risk": ("risk_scale", ">= 4"),
    "low risk": ("risk_scale", "<= 2"),
    "medium risk": ("risk_scale", "= 3"),
    "risk": ("risk_scale", None),
    "danger": ("risk_scale", None),
    "dangerous": ("risk_scale", ">= 4"),
    "safe": ("prediction", "= 0"),
    "unsafe": ("prediction", "= 1"),
    "avalanche": ("prediction", "= 1"),
    "no avalanche": ("prediction", "= 0"),
    "probability": ("avalanche_probability", None),
    "chance": ("avalanche_probability", None),
    "likelihood": ("avalanche_probability", None),
    "prediction": ("prediction", None),

    # Temperature
    "temperature": ("temp_2m_celsius", None),
    "temp": ("temp_2m_celsius", None),
    "hot": ("temp_2m_celsius", "> 0"),
    "cold": ("temp_2m_celsius", "< -10"),
    "freezing": ("temp_2m_celsius", "< 0"),
    "warming": ("temp_trend_7d", "> 0"),
    "cooling": ("temp_trend_7d", "< 0"),
    "temperature trend": ("temp_trend_7d", None),
    "temp trend": ("temp_trend_7d", None),
    "temperature rising": ("temp_trend_7d", "> 0"),
    "temperature falling": ("temp_trend_7d", "< 0"),
    "temp change": ("temp_change_24h", None),

    # Snow
    "snow depth": ("snow_depth", None),
    "snow": ("snow_depth", None),
    "snowfall": ("snowfall", None),
    "snowpack": ("snow_depth", None),
    "snow cover": ("snow_cover", None),
    "snow depth change": ("snow_depth_change_48h", None),
    "snow increase": ("snow_depth_change_48h", "> 0"),
    "snow decrease": ("snow_depth_change_48h", "< 0"),
    "fresh snow": ("days_since_significant_snow", "< 3"),
    "recent snow": ("days_since_significant_snow", "< 3"),
    "new snow": ("snowfall", "> 0"),
    "snow density": ("snow_density", None),

    # Wind
    "wind": ("wind_speed", None),
    "wind speed": ("wind_speed", None),
    "windy": ("wind_speed", "> 10"),
    "calm": ("wind_speed", "< 3"),
    "wind direction": ("wind_direction", None),
    "wind change": ("wind_speed_change_24h", None),
    "gust": ("wind_speed_max_6h", None),

    # Location
    "location": ("encrypted_lat", None),
    "latitude": ("encrypted_lat", None),
    "longitude": ("encrypted_lon", None),
    "elevation": ("elevation_zone", None),
    "altitude": ("elevation_zone", None),
    "alpine": ("elevation_zone", "= 'Alpine'"),
    "sub-alpine": ("elevation_zone", "= 'Sub-Alpine'"),
    "nival": ("elevation_zone", "= 'Nival'"),

    # Slope
    "slope": ("slope_angle", None),
    "steep": ("slope_angle", "> 35"),
    "gentle": ("slope_angle", "< 25"),
    "aspect": ("aspect", None),
    "south facing": ("south_facing", "= 1"),
    "north facing": ("aspect", "BETWEEN 315 AND 45"),

    # Date
    "date": ("prediction_date", None),
    "when": ("prediction_date", None),
    "today": ("prediction_date", None),
    "recent": ("prediction_date", None),

    # Compound metrics
    "compound risk": ("compound_risk_score", None),
    "risk score": ("compound_risk_score", None),
    "overall risk": ("compound_risk_score", None),
    "top": (None, "ORDER BY ... DESC LIMIT"),
    "highest": (None, "ORDER BY ... DESC"),
    "lowest": (None, "ORDER BY ... ASC"),
    "average": (None, "AVG(...)"),
    "total": (None, "SUM(...)"),
    "count": (None, "COUNT(*)"),
}


class SchemaLinker:
    """
    Layer 2: Maps vague user terms to exact column names.
    
    Two-phase approach:
    1. FAST: Domain dictionary lookup (instant, deterministic)
    2. SLOW: LLM-based linking (only for unmapped terms)
    """

    LINKING_PROMPT = """Map the user's question to exact column names from this schema.

TABLE: {table_name}
COLUMNS: {column_list}

Question: "{question}"

Already mapped terms: {already_mapped}

For any remaining unmapped terms in the question, map them to exact column names.
Rewrite the question replacing vague terms with exact column references.

Output ONLY valid JSON:
{{
  "target_tables": ["{table_name}"],
  "column_mappings": [
    {{"user_term": "term", "maps_to": "column_name"}}
  ],
  "resolved_question": "rewritten question with exact column names"
}}"""

    async def link(self, question: str, schema_metadata) -> dict:
        """Link user entities to schema elements using domain dict + LLM."""
        # Phase 1: Domain dictionary lookup (instant)
        mapped, unmapped_question = self._dict_lookup(question)
        
        if mapped:
            logger.info(f"Domain dict mapped {len(mapped)} terms instantly")

        # Build resolved question from dict mappings
        resolved = question
        for user_term, (col, condition) in mapped.items():
            if col:
                resolved = resolved.replace(user_term, col)

        # Phase 2: LLM for remaining ambiguous terms (if needed)
        tables = list(schema_metadata.tables)
        if len(tables) == 1:
            # Single table — build compact column list for LLM
            table_name = tables[0]
            cols = schema_metadata.columns.get(table_name, [])
            col_list = ", ".join(c.name for c in cols)
            
            already = ", ".join(f"{t}→{c}" for t, (c, _) in mapped.items() if c)

            prompt = self.LINKING_PROMPT.format(
                table_name=table_name,
                column_list=col_list,
                question=question,
                already_mapped=already or "none",
            )

            try:
                raw = await llm_manager.generate(
                    prompt=prompt,
                    model=Config.FAST_MODEL,
                    temperature=0.0,
                    num_ctx=Config.FAST_NUM_CTX,
                )
                result = self._parse_response(raw, schema_metadata)
                # Merge dict mappings with LLM mappings
                for user_term, (col, condition) in mapped.items():
                    if col:
                        result["column_mappings"].append({
                            "user_term": user_term,
                            "maps_to": col,
                            "filter_value": condition,
                        })
                if result.get("resolved_question"):
                    resolved = result["resolved_question"]
                result["resolved_question"] = resolved
                return result
            except Exception as e:
                logger.warning(f"LLM linking failed: {e}")
        
        # Fallback with dict mappings only
        return {
            "target_tables": tables,
            "column_mappings": [
                {"user_term": t, "maps_to": c, "filter_value": cond}
                for t, (c, cond) in mapped.items() if c
            ],
            "resolved_question": resolved,
            "join_needed": False,
        }

    def _dict_lookup(self, question: str) -> tuple:
        """Phase 1: Instant domain dictionary lookup. Returns (mapped_terms, remaining_question)."""
        q_lower = question.lower()
        mapped = {}
        
        # Sort by length descending so longer phrases match first
        sorted_terms = sorted(TERM_TO_COLUMN.keys(), key=len, reverse=True)
        
        for term in sorted_terms:
            if term in q_lower:
                mapped[term] = TERM_TO_COLUMN[term]
        
        return mapped, q_lower

    def _parse_response(self, raw: str, schema_metadata) -> dict:
        """Parse LLM JSON response."""
        raw = raw.strip()
        if "```" in raw:
            match = re.search(r"```(?:json)?\s*\n?(.*?)```", raw, re.DOTALL)
            if match:
                raw = match.group(1).strip()

        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            raw = json_match.group(0)

        try:
            result = json.loads(raw)
        except json.JSONDecodeError:
            return {
                "target_tables": list(schema_metadata.tables),
                "column_mappings": [],
                "resolved_question": "",
                "join_needed": False,
            }

        valid_tables = set(t.lower() for t in schema_metadata.tables)
        target_tables = [
            t for t in result.get("target_tables", [])
            if t.lower() in valid_tables
        ] or list(schema_metadata.tables)

        return {
            "target_tables": target_tables,
            "column_mappings": result.get("column_mappings", []),
            "resolved_question": result.get("resolved_question", ""),
            "join_needed": result.get("join_needed", False),
        }
