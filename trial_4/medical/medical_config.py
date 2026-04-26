"""
Medical NL2SQL — Configuration
Overrides base config for the military hospital dataset.
"""
import os


class MedicalConfig:
    # ── Ollama LLM Settings ──────────────────────────────────────
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

    REASONING_MODEL = os.getenv("REASONING_MODEL", "gpt_oss_120b:latest")
    SQL_MODEL = os.getenv("SQL_MODEL", "llama3.1:8b")
    FALLBACK_REASONING_MODEL = os.getenv("FALLBACK_REASONING_MODEL", "llama3.1:8b")
    FALLBACK_SQL_MODEL = os.getenv("FALLBACK_SQL_MODEL", "llama3.1:8b")
    PRIMARY_MODEL = os.getenv("PRIMARY_MODEL", "llama3.1:8b")

    # ── Generation Parameters ────────────────────────────────────
    COT_TEMPERATURE = 0.1
    SQL_TEMPERATURE = 0.0
    REFINER_TEMPERATURE = 0.0

    COT_NUM_CTX = 8192
    SQL_NUM_CTX = 4096
    REFINER_NUM_CTX = 4096
    CRITIC_NUM_CTX = 2048

    # ── Schema Pruning ───────────────────────────────────────────
    # 71 columns in unified table — need generous budget
    PRUNE_BASE_BUDGET = 35
    PRUNE_MAX_BUDGET = 55
    PRUNE_MANDATORY_COLS = [
        "data_year", "medical_unit", "diagnosis_code1d", "diagnosis",
        "admsn_date", "dschrg_date", "los_days", "category", "disposal",
    ]

    # ── SQL Execution Safety ─────────────────────────────────────
    QUERY_TIMEOUT_SECONDS = 60  # Larger dataset needs more time
    MAX_RESULT_ROWS = 10_000
    MAX_REFINER_RETRIES = 2

    # ── Input Guardrails ─────────────────────────────────────────
    MAX_QUERY_LENGTH = 500

    # ── Cache Settings ───────────────────────────────────────────
    CACHE_MAX_SIZE = 500
    CACHE_TTL_SECONDS = 3600

    # ── Server Settings ──────────────────────────────────────────
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8001"))
