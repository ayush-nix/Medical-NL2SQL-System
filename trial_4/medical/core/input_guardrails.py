"""
Agent 0: Input Guardrails — Security layer.
Pure Python, zero LLM calls.

Prevents:
- SQL injection via NL input
- Prompt injection attacks
- Excessively long queries
- CRUD attempt masking
"""
import re
import logging
from config import MedicalConfig as Config

logger = logging.getLogger("nl2sql.guardrails")


# ── SQL injection patterns (regex) ───────────────────────────
SQL_INJECTION_PATTERNS = [
    r";\s*(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE)\b",
    r"\bDROP\s+TABLE\b",
    r"\bDELETE\s+FROM\b",
    r"\bUPDATE\s+\w+\s+SET\b",
    r"\bINSERT\s+INTO\b",
    r"\bALTER\s+TABLE\b",
    r"\bCREATE\s+(TABLE|DATABASE|INDEX)\b",
    r"\bTRUNCATE\s+TABLE\b",
    r"\bEXEC(UTE)?\s*\(",
    r"--\s*$",  # SQL comment at end
    r"\bUNION\s+(ALL\s+)?SELECT\b",
    r"\bWAITFOR\s+DELAY\b",
    r"\bSLEEP\s*\(",
    r"\bBENCHMARK\s*\(",
    r"\bLOAD_FILE\s*\(",
    r"\bINTO\s+(OUT|DUMP)FILE\b",
    r"'\s*(OR|AND)\s+'?\d*'?\s*=\s*'?\d*",  # ' OR '1'='1
    r"\bSHUTDOWN\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
]

# ── Prompt injection patterns ────────────────────────────────
PROMPT_INJECTION_PATTERNS = [
    r"ignore\s+(all\s+)?previous\s+(instructions?|context)",
    r"forget\s+(all\s+)?(your\s+)?instructions?",
    r"you\s+are\s+now\s+a",
    r"new\s+instructions?\s*:",
    r"system\s*prompt\s*:",
    r"override\s+(all\s+)?rules?",
    r"disregard\s+(all\s+)?",
    r"pretend\s+you\s+are",
    r"act\s+as\s+(if|a)",
    r"reveal\s+(your\s+)?(system|prompt|instructions?)",
    r"what\s+(is|are)\s+your\s+(system\s+)?prompt",
    r"show\s+(me\s+)?(your\s+)?(system|prompt|instructions?)",
]

# ── Compiled patterns for performance ────────────────────────
_SQL_PATTERNS = [re.compile(p, re.IGNORECASE) for p in SQL_INJECTION_PATTERNS]
_PROMPT_PATTERNS = [re.compile(p, re.IGNORECASE) for p in PROMPT_INJECTION_PATTERNS]


class InputGuardrails:
    """
    Security gate — runs BEFORE any processing.
    All checks are deterministic regex/string. Zero LLM cost.
    """

    def validate(self, query: str) -> dict:
        """
        Validate user input. Returns:
        {
            "safe": bool,
            "sanitized_query": str,
            "blocked_reason": str or None,
        }
        """
        if not query or not query.strip():
            return {
                "safe": False,
                "sanitized_query": "",
                "blocked_reason": "Empty query",
            }

        query = query.strip()

        # ── Length check ──────────────────────────────────────
        if len(query) > Config.MAX_QUERY_LENGTH:
            logger.warning(f"Query too long: {len(query)} chars (max {Config.MAX_QUERY_LENGTH})")
            return {
                "safe": False,
                "sanitized_query": query[:Config.MAX_QUERY_LENGTH],
                "blocked_reason": f"Query exceeds {Config.MAX_QUERY_LENGTH} character limit. Please shorten your question.",
            }

        # ── SQL injection check ───────────────────────────────
        for pattern in _SQL_PATTERNS:
            if pattern.search(query):
                logger.warning(f"SQL injection blocked: pattern={pattern.pattern}, query={query[:100]}")
                return {
                    "safe": False,
                    "sanitized_query": query,
                    "blocked_reason": "Query contains potentially harmful SQL patterns. Please rephrase as a natural language question.",
                }

        # ── Prompt injection check ────────────────────────────
        for pattern in _PROMPT_PATTERNS:
            if pattern.search(query):
                logger.warning(f"Prompt injection blocked: pattern={pattern.pattern}")
                return {
                    "safe": False,
                    "sanitized_query": query,
                    "blocked_reason": "Query contains instruction-override patterns. Please ask a data question.",
                }

        # ── Sanitize: remove any stray semicolons ─────────────
        sanitized = query.replace(";", "").strip()

        logger.info(f"Input guardrails: PASSED ({len(sanitized)} chars)")
        return {
            "safe": True,
            "sanitized_query": sanitized,
            "blocked_reason": None,
        }


# Singleton
input_guardrails = InputGuardrails()
