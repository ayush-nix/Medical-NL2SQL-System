"""
Agent 2: Schema Pruner — Group-Aware Hybrid BM25 RRF.
Zero LLM calls, zero embeddings. Pure Python, ~1ms.

3-Layer Safety Net for never pruning a needed column:
  Layer 1: Group Expansion — include ALL columns from relevant groups
  Layer 2: Agent 1 Injection — force any fuzzy-matched columns
  Layer 3: Adaptive Budget — 15-30 columns based on query complexity

The approach: We already have high-quality synonym lists for every column.
BM25 against these lists matches or exceeds embedding-based retrieval
for our specific domain. Embeddings shine when you DON'T have synonym lists.
"""
import json
import os
import time
import logging
from config import MedicalConfig as Config
from utils.text_utils import tokenize, multi_signal_score

logger = logging.getLogger("nl2sql.agent2_pruner")


class SchemaPruner:
    """
    Group-Aware Hybrid BM25 Schema Pruner.
    
    Algorithm:
    1. Select relevant groups (from Agent 1 plan)
    2. Expand to ALL columns within those groups
    3. BM25-score each column against the query
    4. Inject mandatory + Agent 1 forced columns
    5. Adaptive top-K cutoff
    6. Build pruned DDL
    """

    def __init__(self, metadata_path: str = None, column_profiles: dict = None):
        if metadata_path is None:
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            metadata_path = os.path.join(base, "data", "column_metadata.json")

        with open(metadata_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.table_name = data.get("primary_table", data.get("table_name", "admissions"))
        self.table_description = data.get("table_description", data.get("description", ""))
        self.columns = data["columns"]
        self.column_profiles = column_profiles or {}  # Live data profiles

        # Build group index
        self._group_index = {}  # group -> [col_dict]
        self._col_by_name = {}  # col_name -> col_dict
        for col in self.columns:
            group = col.get("group", "Other")
            if group not in self._group_index:
                self._group_index[group] = []
            self._group_index[group].append(col)
            self._col_by_name[col["name"]] = col

        self.all_column_names = set(self._col_by_name.keys())

        logger.info(
            f"SchemaPruner ready: {len(self.columns)} columns, "
            f"{len(self._group_index)} groups, "
            f"{len(self.column_profiles)} profiles"
        )

    def prune(self, query: str, agent1_plan: dict = None) -> dict:
        """
        Execute the 3-layer pruning pipeline.
        
        Args:
            query: original user query
            agent1_plan: structured plan from Agent 1
        
        Returns:
            {
                "selected_columns": [col_dict, ...],
                "selected_names": [str, ...],
                "schema_ddl": str,  # CREATE TABLE DDL
                "total_columns": int,
                "pruned_count": int,
                "groups_used": [str, ...],
            }
        """
        start = time.time()
        agent1_plan = agent1_plan or {}

        # ── Layer 1: Group Expansion ─────────────────────────
        target_groups = agent1_plan.get("target_groups", list(self._group_index.keys()))
        candidate_columns = []
        groups_used = set()

        for group in target_groups:
            if group in self._group_index:
                candidate_columns.extend(self._group_index[group])
                groups_used.add(group)

        # If no groups matched, fall back to all groups
        if not candidate_columns:
            candidate_columns = self.columns[:]
            groups_used = set(self._group_index.keys())

        logger.info(f"Layer 1 (Group Expansion): {len(candidate_columns)} candidates from {len(groups_used)} groups")

        # ── Layer 2: BM25 Scoring ────────────────────────────
        query_tokens = tokenize(query)
        scored = []

        for col in candidate_columns:
            score = multi_signal_score(query_tokens, col)
            scored.append((col, score))

        # Sort by score descending
        scored.sort(key=lambda x: x[1], reverse=True)

        # ── Layer 3: Adaptive Budget + Forced Injection ──────
        # Determine budget based on complexity
        budget = self._compute_budget(agent1_plan, groups_used)

        # Start with mandatory columns
        selected_names = set()
        selected_cols = []

        # 3a. Mandatory columns (always included)
        for mandatory_name in Config.PRUNE_MANDATORY_COLS:
            if mandatory_name in self._col_by_name:
                selected_names.add(mandatory_name)
                selected_cols.append(self._col_by_name[mandatory_name])

        # 3b. Agent 1 forced columns (fuzzy-matched)
        forced_from_agent1 = set()
        for match_info in agent1_plan.get("fuzzy_matches", {}).values():
            col_name = match_info.get("column_name", "")
            if col_name in self._col_by_name and col_name not in selected_names:
                selected_names.add(col_name)
                selected_cols.append(self._col_by_name[col_name])
                forced_from_agent1.add(col_name)

        # Agent 1 explicit target columns
        for col_name in agent1_plan.get("target_columns", []):
            if col_name in self._col_by_name and col_name not in selected_names:
                selected_names.add(col_name)
                selected_cols.append(self._col_by_name[col_name])
                forced_from_agent1.add(col_name)

        # 3c. Fill remaining budget from BM25-ranked columns
        remaining_budget = budget - len(selected_names)
        for col, score in scored:
            if remaining_budget <= 0:
                break
            if col["name"] not in selected_names:
                selected_names.add(col["name"])
                selected_cols.append(col)
                remaining_budget -= 1

        # ── Build DDL ────────────────────────────────────────
        schema_ddl = self._build_ddl(selected_cols)

        elapsed_ms = round((time.time() - start) * 1000, 1)

        logger.info(
            f"Agent 2 complete: {len(selected_cols)}/{len(self.columns)} cols, "
            f"budget={budget}, forced={len(forced_from_agent1)}, "
            f"groups={list(groups_used)}, time={elapsed_ms}ms"
        )

        return {
            "selected_columns": selected_cols,
            "selected_names": list(selected_names),
            "schema_ddl": schema_ddl,
            "total_columns": len(self.columns),
            "pruned_count": len(selected_cols),
            "groups_used": list(groups_used),
            "prune_time_ms": elapsed_ms,
        }

    def _compute_budget(self, agent1_plan: dict, groups_used: set) -> int:
        """
        Adaptive column budget. Not fixed — scales with query complexity.
        
        - Simple single-group query: 15 columns
        - Multi-group query: 20 columns
        - Complex (aggregation + groupby + filter): 25 columns
        - Very complex (comparison + temporal): 30 columns
        """
        base = Config.PRUNE_BASE_BUDGET  # 15
        max_budget = Config.PRUNE_MAX_BUDGET  # 30

        budget = base

        # More groups = more columns needed
        if len(groups_used) > 2:
            budget += 5
        if len(groups_used) > 4:
            budget += 5

        # Complex intent = more columns
        intents = agent1_plan.get("intent_types", [])
        if "AGGREGATION" in intents and "GROUPBY" in intents:
            budget += 3
        if "COMPARISON" in intents:
            budget += 3
        if "TEMPORAL" in intents:
            budget += 2

        # Multiple filter conditions
        filters = agent1_plan.get("filter_conditions", [])
        if len(filters) > 2:
            budget += 3

        return min(budget, max_budget)

    def _build_ddl(self, columns: list[dict]) -> str:
        """Build CREATE TABLE DDL with type info, descriptions, AND live data profiles."""
        type_map = {
            "float": "REAL", "int": "INTEGER", "integer": "INTEGER",
            "text": "TEXT", "string": "TEXT", "date": "TEXT",
        }

        col_defs = []
        for col in columns:
            col_type = type_map.get(col.get("type", "text").lower(), "TEXT")
            name = col['name']

            # Build comment with description + live profile
            comment_parts = []
            desc = col.get("description", "")
            if desc:
                comment_parts.append(desc[:60])

            # Inject live data profile (THE KEY FIX for value range blindness)
            profile = self.column_profiles.get(name, {})
            if profile:
                if 'min' in profile and 'max' in profile:
                    mn, mx, av = profile.get('min','?'), profile.get('max','?'), profile.get('mean','?')
                    comment_parts.append(f"range: {mn}–{mx}, mean: {av}")
                    if profile.get('samples'):
                        comment_parts.append(f"examples: {', '.join(profile['samples'][:3])}")
                elif profile.get('top_values'):
                    comment_parts.append(f"values: {', '.join(profile['top_values'][:5])}")
            elif col.get("enum"):
                enum_str = ", ".join(str(v) for v in col["enum"])
                comment_parts.append(f"values: [{enum_str}]")

            if col.get("unit"):
                comment_parts.append(f"unit: {col['unit']}")

            comment = f"  -- {'; '.join(comment_parts)}" if comment_parts else ""
            col_defs.append(f"  {name} {col_type}{comment}")

        ddl = f"CREATE TABLE {self.table_name} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        return ddl

    def get_enum_values(self, column_names: list[str]) -> str:
        """Get enum/sample values for specified columns (for SQL gen prompt)."""
        lines = []
        for name in column_names:
            col = self._col_by_name.get(name)
            if not col:
                continue
            if col.get("enum"):
                enum_str = ", ".join(str(v) for v in col["enum"])
                lines.append(f"  {name}: [{enum_str}]")
        return "\n".join(lines) if lines else ""

    def validate_column_name(self, col_name: str) -> tuple[bool, str]:
        """
        Check if a column name exists. If not, find closest match.
        Returns (is_valid, corrected_name_or_original).
        """
        if col_name in self.all_column_names:
            return True, col_name

        # Case-insensitive check
        for valid in self.all_column_names:
            if valid.lower() == col_name.lower():
                return False, valid

        # Fuzzy match (Levenshtein-based)
        from utils.fuzzy_matcher import find_best_match
        best, score = find_best_match(col_name, list(self.all_column_names), threshold=0.70)
        if best:
            return False, best
        return False, ""
