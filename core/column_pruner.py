"""
Vectorless Column Pruner — BM25-Style Keyword Matching (CHESS Schema Selector Pattern)

Instead of sending all 152 columns to the LLM, this module selects only the
top-K most relevant columns for each query using keyword matching against
the rich semantic metadata (synonyms, descriptions, group names).

WHY VECTORLESS (not embedding-based):
- Deterministic: same query always returns same columns (critical for SQL)
- No embedding model needed: works fully offline, no extra dependencies
- Faster: BM25 keyword matching is O(n) vs vector similarity O(n·d)
- More precise: exact term matching prevents "semantic drift" hallucinations

Reference: CHESS (2024) — Schema Selector agent reduced tokens by 5x
Reference: PageIndex — vectorless RAG outperforms vector RAG for structured data
"""
import json
import re
import logging
import os
from collections import defaultdict

logger = logging.getLogger("nl2sql.column_pruner")

# Always include these columns regardless of query
ALWAYS_INCLUDE = {"id", "prediction_date", "prediction", "avalanche_probability", "risk_scale", "compound_risk_score"}

# Default top-K columns to return
DEFAULT_TOP_K = 25


class ColumnPruner:
    """
    Selects relevant columns from metadata using BM25-style keyword scoring.
    
    Scoring algorithm:
    1. Exact column name match → 10 points
    2. Synonym match → 8 points  
    3. Display name match → 6 points
    4. Group name match → 4 points
    5. Description match → 2 points per word hit
    """

    def __init__(self, metadata_path: str = None):
        if metadata_path is None:
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            metadata_path = os.path.join(base, "data", "column_metadata.json")

        with open(metadata_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.table_name = data["table_name"]
        self.table_description = data["table_description"]
        self.columns = data["columns"]

        # Build inverted index for fast lookup
        self._build_index()
        logger.info(f"ColumnPruner loaded: {len(self.columns)} columns indexed")

    def _build_index(self):
        """Build an inverted index mapping keywords → column names."""
        self.index = defaultdict(list)

        for col in self.columns:
            name = col["name"]

            # Index column name tokens
            for token in self._tokenize(name):
                self.index[token].append((name, 10))

            # Index display name
            for token in self._tokenize(col.get("display", "")):
                self.index[token].append((name, 6))

            # Index synonyms (highest priority after exact name)
            for syn in col.get("synonyms", []):
                for token in self._tokenize(syn):
                    self.index[token].append((name, 8))

            # Index group
            for token in self._tokenize(col.get("group", "")):
                self.index[token].append((name, 4))

            # Index description words
            for token in self._tokenize(col.get("description", "")):
                self.index[token].append((name, 2))

    def _tokenize(self, text: str) -> list:
        """Simple tokenizer: lowercase, split on non-alphanumeric, remove stopwords."""
        stopwords = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "in", "of", "to", "for", "and", "or", "at", "by", "from",
            "on", "with", "as", "it", "its", "this", "that", "which",
        }
        tokens = re.findall(r"[a-z0-9]+", text.lower())
        return [t for t in tokens if t not in stopwords and len(t) > 1]

    def prune(self, query: str, top_k: int = DEFAULT_TOP_K) -> dict:
        """
        Select the most relevant columns for a natural language query.
        
        Returns:
            dict with 'selected_columns' (list of column metadata),
            'schema_text' (formatted for LLM prompt),
            'scores' (column→score mapping)
        """
        tokens = self._tokenize(query)
        scores = defaultdict(float)

        # Score each column based on keyword hits
        for token in tokens:
            if token in self.index:
                for col_name, weight in self.index[token]:
                    scores[col_name] += weight

        # Always include mandatory columns
        for col_name in ALWAYS_INCLUDE:
            scores[col_name] = max(scores.get(col_name, 0), 1)

        # Sort by score descending, take top-K
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        selected_names = set(name for name, _ in ranked[:top_k])

        # Get full metadata for selected columns
        selected = [c for c in self.columns if c["name"] in selected_names]

        # Build schema text for LLM prompt
        schema_text = self._build_schema_text(selected)

        logger.info(
            f"Column pruning: {len(tokens)} query tokens → "
            f"{len(selected)}/{len(self.columns)} columns selected"
        )

        return {
            "selected_columns": selected,
            "selected_names": [c["name"] for c in selected],
            "schema_text": schema_text,
            "scores": dict(ranked[:top_k]),
            "total_columns": len(self.columns),
            "pruned_count": len(selected),
        }

    def _build_schema_text(self, columns: list) -> str:
        """Build a compact schema with inline sample values (SDE-SQL data probing)."""
        lines = [f"TABLE: {self.table_name}"]
        lines.append(f"-- {self.table_description}")
        lines.append("-- COLUMNS:")

        for col in columns:
            type_str = col.get("type", "text").upper()
            parts = [f"  {col['name']} {type_str}"]

            # Add sample/enum values inline — critical for accuracy
            if col.get("enum"):
                parts.append(f"  values: {col['enum']}")
            elif col.get("range"):
                r = col["range"]
                parts.append(f"  range: [{r[0]}, {r[1]}]")

            # Short description
            desc = col.get("description", "")
            if desc:
                parts.append(f"  -- {desc[:60]}")

            lines.append(" ".join(parts))

        return "\n".join(lines)

    def get_column_groups(self) -> dict:
        """Get columns organized by semantic group."""
        groups = defaultdict(list)
        for col in self.columns:
            groups[col.get("group", "Other")].append(col["name"])
        return dict(groups)


# Singleton
column_pruner = ColumnPruner()
