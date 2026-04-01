"""
NL2SQL Configuration — All settings centralized here.
Air-gapped, read-only, no internet dependencies.
"""
import os


class Config:
    # ── Ollama LLM Settings ──────────────────────────────────────
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

    # ┌─────────────────────────────────────────────────────────┐
    # │  MODEL RECOMMENDATIONS (swap when available)            │
    # │                                                         │
    # │  SQL Generation:                                        │
    # │    1st: arctic-text2sql (Snowflake, ExCoT, best SQL)    │
    # │    2nd: sqlcoder:7b-2    (Defog, fine-tuned on Spider)  │
    # │    3rd: qwen2.5-coder:7b (Alibaba, strong code+SQL)    │
    # │                                                         │
    # │  Base/Brain (Planner, Linker, Judge, Synthesizer):      │
    # │    1st: qwen3:8b         (best reasoning + multilingual)│
    # │    2nd: llama3.1:8b      (strong all-rounder)           │
    # │    3rd: mistral-small:latest (fast, good planning)      │
    # │                                                         │
    # │  To swap: ollama pull <model> then update env vars      │
    # └─────────────────────────────────────────────────────────┘

    # Primary SQL generation model (Coder Model — SQLCoder, fine-tuned for text2sql)
    SQL_MODEL = os.getenv("SQL_MODEL", "sqlcoder:7b")
    # Fallback for complex queries
    FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "mistral:latest")
    # Fast model for planner, linker, critic, synthesis (Brain Model)
    FAST_MODEL = os.getenv("FAST_MODEL", "mistral:latest")
    # Embedding model for RAG
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text-v2-moe:latest")

    # ── Generation Parameters ────────────────────────────────────
    SQL_TEMPERATURE = 0.0       # Deterministic SQL generation
    SQL_NUM_CTX = 4096          # Context window for SQL model (focused single-table)
    FAST_NUM_CTX = 2048         # Context for planner/linker/critic (speed)
    FALLBACK_NUM_CTX = 8192     # Larger context for fallback model

    # ── SQL Execution Safety ─────────────────────────────────────
    QUERY_TIMEOUT_SECONDS = 30
    MAX_RESULT_ROWS = 10_000
    MAX_RETRIES = 1  # One self-correction attempt (column validator suggests fix)

    # ── Database Connection ──────────────────────────────────────
    # Will be configured at startup via API or connection string
    DB_API_ENDPOINT = os.getenv("DB_API_ENDPOINT", "")
    DB_CONNECTION_STRING = os.getenv(
        "DB_CONNECTION_STRING",
        "postgresql+asyncpg://readonly_user:password@localhost:5432/armydb"
    )
    DB_TYPE = os.getenv("DB_TYPE", "postgresql")  # postgresql, mysql, mssql, oracle

    # ── RAG Settings ─────────────────────────────────────────────
    SCHEMA_TOP_K = 5          # Top-K tables from RAG retrieval
    FEW_SHOT_TOP_K = 3        # Top-K few-shot examples per query
    FK_AUGMENTATION_HOPS = 1  # BFS hops for FK augmentation

    # ── Cache Settings ───────────────────────────────────────────
    CACHE_MAX_SIZE = 1000
    CACHE_TTL_SECONDS = 3600  # 1 hour

    # ── Server Settings ──────────────────────────────────────────
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))
