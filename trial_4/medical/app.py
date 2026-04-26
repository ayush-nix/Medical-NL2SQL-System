"""
Medical NL2SQL — Military Hospital Orchestrator (Port 8001)
Self-contained 6-agent pipeline for 1.65M hospital admission records.

Pipeline:
  Agent 0: Input Guardrails (instant)
  Agent 1: Query Understanding (1 LLM call + medical domain dictionary)
  Agent 2: Schema Pruner (instant)
  Agent 3: SQL Generator (1 LLM call, medical-specific prompts)
  4-Pass Validator + Column Sanitizer (instant)
  Agent 4: Self-Correction Refiner (conditional)
  SQL Executor + Answer Synthesizer (instant)
  Agent 6: Experience Memory (instant)

ENDPOINTS:
  GET  /                        — Frontend UI
  POST /api/load-unified-data   — Load cleaned SQLite into NL2SQL
  GET  /api/download-csv        — Download unified_admissions.csv
  POST /api/query               — Submit NL query
  GET  /api/schema              — View database schema
  GET  /api/health              — Health check

Run:
    cd medical/
    python app.py
"""
import json
import time
import sqlite3
import logging
import sys
import os
from pathlib import Path

# ── Ensure medical/ is on sys.path for local imports ─────────
MEDICAL_DIR = Path(__file__).parent.resolve()
if str(MEDICAL_DIR) not in sys.path:
    sys.path.insert(0, str(MEDICAL_DIR))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import MedicalConfig as Config
from core.schema_introspector import SchemaIntrospector, SchemaMetadata, ColumnInfo
from core.input_guardrails import input_guardrails
from core.query_understanding import QueryUnderstandingEngine
from core.schema_pruner import SchemaPruner
from core.sql_generator import MedicalSQLGenerator
from core.sql_validator import validate_sql
from core.sql_refiner import SQLRefiner
from core.sql_executor import SQLExecutor
from core.answer_synthesizer import AnswerSynthesizer
from core.experience_memory import experience_memory
from core.query_cache import QueryCache
from core.query_logger import query_logger
from core.column_sanitizer import sanitize_columns

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("medical")

# ── FastAPI App ──────────────────────────────────────────────
app = FastAPI(
    title="Medical NL2SQL — Military Hospital",
    description="6-agent pipeline for 1.65M military hospital records (2021-2024)",
    version="1.0.0",
)

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled error: {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "question": "", "sql": "", "valid": False,
            "results": {"success": False, "columns": [], "rows": [],
                        "row_count": 0, "error": str(exc)},
            "answer": f"Error: {type(exc).__name__}: {str(exc)}",
            "confidence": 0, "cached": False,
            "generation_time_ms": 0, "total_time_ms": 0,
        },
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Core Components ──────────────────────────────────────────
introspector = SchemaIntrospector()
understanding_engine = QueryUnderstandingEngine()
schema_pruner = None
generator = MedicalSQLGenerator()
refiner = SQLRefiner()
executor = SQLExecutor()
synthesizer = AnswerSynthesizer()
cache = QueryCache(max_size=Config.CACHE_MAX_SIZE, ttl_seconds=Config.CACHE_TTL_SECONDS)

# ── Pre-warm model ───────────────────────────────────────────
from models.llm_manager import llm_manager
try:
    logger.info("Pre-warming LLM model...")
    llm_manager.warmup()
    logger.info("Model ready")
except Exception as e:
    logger.warning(f"Model warmup failed (non-critical): {e}")

# ── State ────────────────────────────────────────────────────
app_state = {
    "schema_loaded": False, "tables": [], "total_rows": 0,
    "query_count": 0, "load_time": None,
}

class QueryRequest(BaseModel):
    question: str
    use_cache: bool = True

class QueryResponse(BaseModel):
    question: str
    resolved_question: str = ""
    sql: str
    valid: bool
    results: dict
    answer: str
    confidence: float
    cached: bool
    generation_time_ms: int
    total_time_ms: int
    attempts: int = 0
    model_used: str = ""
    pipeline_trace: dict = {}

# ── Static files ─────────────────────────────────────────────
static_dir = MEDICAL_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# ── Paths ────────────────────────────────────────────────────
CLEANED_DATA_DIR = MEDICAL_DIR / "cleaned_data"
DB_PATH = CLEANED_DATA_DIR / "unified_admissions.db"
CSV_PATH = CLEANED_DATA_DIR / "unified_admissions.csv"
METADATA_PATH = MEDICAL_DIR / "data" / "column_metadata.json"


def _init_agents_with_metadata():
    """Initialize Agent 1 and 2 with medical column metadata + domain dictionary."""
    global schema_pruner

    if METADATA_PATH.exists():
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        col_metadata = data.get("columns", [])
        profiles = introspector.metadata.column_profiles or {}

        # Inject medical domain dictionary
        from utils.domain_dictionary import ABBREVIATIONS, BUSINESS_TERM_HINTS, GROUP_KEYWORDS
        understanding_engine.abbreviations = ABBREVIATIONS
        # Also patch the imported module so Phase A picks them up
        import utils.domain_dictionary as dd
        dd.ABBREVIATIONS = ABBREVIATIONS
        dd.BUSINESS_TERM_HINTS = BUSINESS_TERM_HINTS
        dd.GROUP_KEYWORDS = GROUP_KEYWORDS
        logger.info(f"Medical domain dictionary injected: {len(ABBREVIATIONS)} abbrevs, "
                    f"{len(BUSINESS_TERM_HINTS)} business hints, {len(GROUP_KEYWORDS)} groups")

        understanding_engine.set_column_metadata(col_metadata, profiles)
        schema_pruner = SchemaPruner(str(METADATA_PATH), column_profiles=profiles)
        logger.info(f"Agents initialized: {len(col_metadata)} columns, {len(profiles)} profiles")
    else:
        logger.warning(f"column_metadata.json not found at {METADATA_PATH}")


# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    index_path = static_dir / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Medical NL2SQL</h1><p>Static files not found.</p>")


@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0 (Medical NL2SQL — Military Hospital)",
        "schema_loaded": app_state["schema_loaded"],
        "tables": len(app_state["tables"]),
        "total_rows": app_state["total_rows"],
        "query_count": app_state["query_count"],
        "db_exists": DB_PATH.exists(),
        "csv_exists": CSV_PATH.exists(),
        "cache": cache.stats,
        "model": {
            "reasoning": llm_manager.reasoning_model,
            "sql": llm_manager.sql_model,
        },
    }


@app.post("/api/load-unified-data")
async def load_unified_data():
    """Load pre-cleaned Postgres schema into the NL2SQL system."""
    logger.info("=" * 60)
    logger.info("LOADING UNIFIED MEDICAL DATA (POSTGRESQL)")
    logger.info("=" * 60)

    try:
        start = time.time()
        import psycopg2
        dsn = "dbname=military_hospital user=postgres password=postgres host=localhost"
        conn = psycopg2.connect(dsn)
        conn.autocommit = True

        with conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables_raw = cursor.fetchall()
            table_names = [t[0] for t in tables_raw]
            logger.info(f"Tables found: {table_names}")

            tables, columns, row_counts, col_profiles = [], {}, {}, {}

            for table in table_names:
                cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{table}'")
                pragma = cursor.fetchall()
                col_infos = []
                for p in pragma:
                    # Map PostgreSQL types to generic Text/Real/Integer
                    dtype = "INTEGER" if "int" in p[1].lower() else ("REAL" if "double" in p[1].lower() or "numeric" in p[1].lower() else "TEXT")
                    col_infos.append(ColumnInfo(name=p[0], dtype=dtype, is_pk=(p[0] == "row_id")))

                cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
                count = cursor.fetchone()[0]
                tables.append(table)
                columns[table] = col_infos
                row_counts[table] = count
                logger.info(f"  {table}: {count:,} rows, {len(col_infos)} columns")

        profiles = {}
        tables_to_profile = ["admissions"] if "admissions" in table_names else table_names
        for table in tables_to_profile:
             profiles.update(introspector._compute_column_profiles(conn, table, columns[table]))
        col_profiles.update(profiles)

        introspector.metadata = SchemaMetadata(
            tables=tables, columns=columns, row_counts=row_counts,
            column_profiles=col_profiles, db_connection=conn,
        )

        executor.set_connection(dsn)
        cache.clear()
        _init_agents_with_metadata()

        app_state["schema_loaded"] = True
        app_state["tables"] = tables
        app_state["total_rows"] = sum(row_counts.values())
        app_state["load_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

        elapsed = time.time() - start
        logger.info(f"DATA LOADED in {elapsed:.1f}s — {app_state['total_rows']:,} total rows")

        schema_summary = [{
            "table": t,
            "columns": [c.name for c in columns.get(t, [])],
            "column_count": len(columns.get(t, [])),
            "row_count": row_counts.get(t, 0),
        } for t in tables]

        return {
            "success": True,
            "tables": schema_summary,
            "total_rows": app_state["total_rows"],
            "load_time_seconds": round(elapsed, 1),
            "message": f"Loaded {len(tables)} table(s) with {app_state['total_rows']:,} rows in {elapsed:.1f}s",
        }
    except Exception as e:
        logger.error(f"Load error: {e}", exc_info=True)
        raise HTTPException(500, str(e))


@app.get("/api/download-csv")
async def download_csv():
    """Download the unified_admissions.csv."""
    if not CSV_PATH.exists():
        raise HTTPException(404, "CSV not found. Run data_cleaning_pipeline.py first.")
    return FileResponse(path=str(CSV_PATH), filename="unified_admissions.csv",
                        media_type="text/csv")


@app.post("/api/query", response_model=QueryResponse)
async def query_endpoint(req: QueryRequest):
    """Main query endpoint — 6-agent pipeline."""
    if not app_state["schema_loaded"]:
        raise HTTPException(400, "No database loaded. Click 'Load Unified Data' first.")

    total_start = time.time()
    app_state["query_count"] += 1
    question = req.question.strip()
    if not question:
        raise HTTPException(400, "Question cannot be empty")

    pipeline_trace = {}
    logger.info(f"{'='*60}")
    logger.info(f"QUERY #{app_state['query_count']}: {question[:120]}")
    logger.info(f"{'='*60}")

    # ── AGENT 0: Input Guardrails ────────────────────────────
    logger.info("[Agent 0] Input Guardrails...")
    guard_result = input_guardrails.validate(question)
    pipeline_trace["guardrails"] = guard_result
    if not guard_result["safe"]:
        logger.warning(f"[Agent 0] BLOCKED: {guard_result['blocked_reason']}")
        return QueryResponse(
            question=question, sql="", valid=False,
            results={"success": False, "columns": [], "rows": [],
                     "row_count": 0, "error": guard_result["blocked_reason"]},
            answer=f"⚠️ {guard_result['blocked_reason']}",
            confidence=0, cached=False,
            generation_time_ms=0,
            total_time_ms=int((time.time() - total_start) * 1000),
            attempts=0, model_used="guardrails",
            pipeline_trace=pipeline_trace,
        )
    sanitized = guard_result["sanitized_query"]
    logger.info(f"[Agent 0] PASSED ({len(sanitized)} chars)")

    # ── CACHE CHECK ──────────────────────────────────────────
    if req.use_cache:
        cached = cache.get(sanitized)
        if cached:
            cached["cached"] = True
            cached["total_time_ms"] = int((time.time() - total_start) * 1000)
            logger.info("[Cache] HIT — returning cached result")
            return QueryResponse(**cached)

    metadata = introspector.metadata
    table_name = "admissions" if "admissions" in metadata.tables else (
        metadata.tables[0] if metadata.tables else "admissions"
    )

    # ── META-QUERY HANDLER ───────────────────────────────────
    q_lower = sanitized.lower()
    meta_patterns = [
        "how many table", "what table", "list table", "show table",
        "how many column", "what column", "list column", "show column",
        "describe", "show schema", "what is the schema", "show structure",
    ]
    if any(p in q_lower for p in meta_patterns):
        logger.info("[Meta] Structural query — no LLM needed")
        return _handle_meta_query(question, sanitized, metadata, total_start)

    # ── AGENT 1: Query Understanding ─────────────────────────
    logger.info("[Agent 1] Query Understanding...")
    agent1_start = time.time()
    plan = await understanding_engine.understand(sanitized)
    agent1_ms = int((time.time() - agent1_start) * 1000)

    semantic_hints = plan.get("semantic_conditions", [])
    complexity = understanding_engine.classify_complexity(
        sanitized, plan.get("intent_types", [])
    )
    logger.info(f"[Agent 1] DONE in {agent1_ms}ms — intent={plan.get('intent_types')}, "
                f"complexity={complexity}, groups={plan.get('target_groups')}, "
                f"target_cols={len(plan.get('target_columns', []))}")

    pipeline_trace["agent1_understanding"] = {
        "intent": plan["intent_types"],
        "groups": plan["target_groups"],
        "fuzzy_matches": plan.get("fuzzy_matches", {}),
        "resolved": plan.get("resolved_question", ""),
        "semantic_hints": semantic_hints,
        "filter_conditions": plan.get("filter_conditions", []),
        "reasoning": plan.get("reasoning", ""),
        "target_columns": plan.get("target_columns", []),
        "complexity": complexity,
        "time_ms": agent1_ms,
    }

    # ── AGENT 6: Experience Memory ───────────────────────────
    agent1_cols = plan.get("target_columns", [])
    if not isinstance(agent1_cols, list):
        agent1_cols = []
    rlaif_rules = experience_memory.retrieve_rule(sanitized, query_columns=agent1_cols)
    if rlaif_rules:
        pipeline_trace["rlaif"] = {"rules_injected": True, "rules": rlaif_rules}
        logger.info("[Agent 6] Experience memory rules injected")

    # ── AGENT 2: Schema Pruning ──────────────────────────────
    logger.info("[Agent 2] Schema Pruning...")
    agent2_start = time.time()
    if schema_pruner:
        prune_result = schema_pruner.prune(sanitized, plan)
        schema_ddl = prune_result["schema_ddl"]
        selected_names = prune_result["selected_names"]
        enum_values = schema_pruner.get_enum_values(selected_names)
        agent2_ms = int((time.time() - agent2_start) * 1000)
        logger.info(f"[Agent 2] DONE in {agent2_ms}ms — "
                    f"{len(selected_names)}/{prune_result['total_columns']} columns selected")
        pipeline_trace["agent2_pruner"] = {
            "selected": len(selected_names),
            "total": prune_result["total_columns"],
            "groups": prune_result["groups_used"],
            "columns": selected_names,
            "time_ms": agent2_ms,
        }
    else:
        schema_ddl = introspector.get_schema_text()
        selected_names = [c.name for c in metadata.columns.get(table_name, [])]
        enum_values = ""
        logger.warning("[Agent 2] No pruner — using full schema")

    sample_rows = introspector.get_sample_rows(table_name, n=3)

    # ── AGENT 3: SQL Generation ──────────────────────────────
    logger.info(f"[Agent 3] SQL Generation (complexity={complexity})...")
    col_metadata_for_prompt = []
    if METADATA_PATH.exists():
        try:
            with open(METADATA_PATH, "r", encoding="utf-8") as f:
                meta_data = json.load(f)
            col_metadata_for_prompt = meta_data.get("columns", [])
        except Exception:
            pass

    col_profiles = introspector.metadata.column_profiles or {}

    gen_result = await generator.generate(
        original_query=question,
        resolved_question=plan.get("resolved_question", sanitized),
        schema_ddl=schema_ddl, table_name=table_name,
        enum_values=enum_values,
        reasoning=plan.get("reasoning", ""),
        target_columns=plan.get("target_columns"),
        filter_conditions=plan.get("filter_conditions"),
        aggregation=plan.get("aggregation"),
        group_by=plan.get("group_by"),
        order_by=plan.get("order_by"),
        limit=plan.get("limit"),
        business_hints=plan.get("business_hints"),
        rlaif_rules=rlaif_rules,
        valid_column_names=selected_names,
        sample_rows=sample_rows,
        intent_types=plan.get("intent_types", []),
        complexity=complexity,
        semantic_hints=semantic_hints,
        column_metadata=col_metadata_for_prompt,
        column_profiles=col_profiles,
    )

    sql = gen_result["sql"]
    generation_time = gen_result["generation_time_ms"]
    logger.info(f"[Agent 3] DONE in {generation_time}ms — SQL({len(sql)} chars)")
    logger.debug(f"[Agent 3] SQL: {sql}")
    pipeline_trace["agent3_generator"] = {
        "complexity": complexity,
        "time_ms": generation_time,
        "model": gen_result["model_used"],
        "sql_generated": sql,
    }

    # ── COLUMN SANITIZER ──────────────────────────────────────
    all_col_names_for_sanitizer = set()
    for t in metadata.tables:
        for c in metadata.columns.get(t, []):
            all_col_names_for_sanitizer.add(c.name)

    sql, sanitizer_corrections = sanitize_columns(
        sql, all_col_names_for_sanitizer, table_name
    )
    if sanitizer_corrections:
        logger.info(f"[Sanitizer] {len(sanitizer_corrections)} column name fixes applied")
    pipeline_trace["column_sanitizer"] = {
        "corrections": sanitizer_corrections,
        "num_fixes": len(sanitizer_corrections),
    }

    # ── 5-PASS VALIDATION ─────────────────────────────────────
    logger.info("[Validator] Running 5-pass validation...")
    pruned_columns_set = set(c.lower() for c in selected_names)
    all_col_names = set()
    for t in metadata.tables:
        for c in metadata.columns.get(t, []):
            all_col_names.add(c.name.lower())

    valid_tables_set = set(t.lower() for t in metadata.tables)
    col_types = {}
    for t in metadata.tables:
        for c in metadata.columns.get(t, []):
            col_types[c.name.lower()] = c.dtype

    validation = validate_sql(
        sql, all_col_names, valid_tables_set, col_types,
        column_profiles=col_profiles, pruned_columns=pruned_columns_set,
    )
    pipeline_trace["validation"] = {
        "passed": validation.passed, "error": validation.error,
        "pass_number": validation.pass_number,
        "corrections": validation.corrections,
    }

    if validation.passed and validation.fixed_sql:
        sql = validation.fixed_sql
        logger.info(f"[Validator] PASSED (corrections: {validation.corrections or 'none'})")

    # ── AGENT 4: Self-Correction ──────────────────────────────
    attempts = 1
    if not validation.passed:
        logger.info(f"[Agent 4] Refiner — validation failed at pass {validation.pass_number}: "
                    f"{validation.error}")

        def validate_fn(s):
            return validate_sql(s, all_col_names, valid_tables_set, col_types,
                                column_profiles=col_profiles,
                                pruned_columns=pruned_columns_set)

        refine_result = await refiner.refine(
            failed_sql=sql, error=validation.error,
            pass_number=validation.pass_number,
            question=question, schema_ddl=schema_ddl,
            table_name=table_name, column_names=selected_names,
            validate_fn=validate_fn,
        )

        pipeline_trace["agent4_refiner"] = {
            "fixed": refine_result["fixed"],
            "attempts": refine_result["attempts"],
            "errors": refine_result["errors"],
        }

        if refine_result["fixed"]:
            sql = refine_result["sql"]
            attempts += refine_result["attempts"]
            logger.info(f"[Agent 4] FIXED after {refine_result['attempts']} attempt(s)")
        else:
            logger.warning(f"[Agent 4] FAILED after {refine_result['attempts']} attempts")
            total_time = int((time.time() - total_start) * 1000)
            return QueryResponse(
                question=question,
                resolved_question=plan.get("resolved_question", ""),
                sql=sql, valid=False,
                results={"success": False, "columns": [], "rows": [],
                         "row_count": 0,
                         "error": f"Validation failed: {refine_result['errors'][-1]}"},
                answer=f"Could not generate valid SQL. Error: {refine_result['errors'][-1]}. Try rephrasing.",
                confidence=0, cached=False,
                generation_time_ms=generation_time, total_time_ms=total_time,
                attempts=attempts + refine_result["attempts"],
                model_used=gen_result["model_used"],
                pipeline_trace=pipeline_trace,
            )

    # ── SQL EXECUTION ────────────────────────────────────────
    logger.info(f"[Executor] Running: {sql[:150]}...")
    exec_result = executor.execute(sql)
    pipeline_trace["execution"] = {
        "success": exec_result["success"],
        "row_count": exec_result.get("row_count", 0),
        "time_ms": exec_result.get("execution_time_ms", 0),
    }
    logger.info(f"[Executor] success={exec_result['success']}, "
                f"rows={exec_result.get('row_count', 0)}")

    # ── AGENT 5: Answer Synthesis ────────────────────────────
    if exec_result.get("success"):
        answer = await synthesizer.synthesize(
            question=sanitized, sql=sql, results=exec_result, use_llm=False,
        )
    else:
        answer = f"Query error: {exec_result.get('error', 'Unknown')}. Please try rephrasing."

    total_time = int((time.time() - total_start) * 1000)
    logger.info(f"QUERY COMPLETE: {total_time}ms total, success={exec_result.get('success')}")

    response_data = {
        "question": question,
        "resolved_question": plan.get("resolved_question", ""),
        "sql": sql,
        "valid": exec_result.get("success", False),
        "results": exec_result,
        "answer": answer,
        "confidence": 0.85 if exec_result.get("success") else 0.0,
        "cached": False,
        "generation_time_ms": generation_time,
        "total_time_ms": total_time,
        "attempts": attempts,
        "model_used": gen_result["model_used"],
        "pipeline_trace": pipeline_trace,
    }

    if exec_result.get("success"):
        cache.put(sanitized, response_data)
        experience_memory.record_success(sanitized)

    query_logger.log({
        "question": question, "sql": sql,
        "success": exec_result.get("success", False),
        "total_time_ms": total_time,
        "row_count": exec_result.get("row_count", 0),
    })

    return QueryResponse(**response_data)


def _handle_meta_query(question, sanitized, metadata, total_start):
    q_lower = sanitized.lower()
    table_list = metadata.tables

    if any(w in q_lower for w in ["column", "schema", "describe", "structure"]):
        col_lines = []
        for t in table_list:
            cols = metadata.columns.get(t, [])
            col_names = [c.name for c in cols]
            col_lines.append(f"Table '{t}': {len(cols)} columns — {', '.join(col_names[:20])}")
            if len(col_names) > 20:
                col_lines.append(f"  ... and {len(col_names) - 20} more")
        answer = "\n".join(col_lines)
    else:
        answer = "\n".join(
            f"Table '{t}': {metadata.row_counts.get(t, 0):,} records"
            for t in table_list
        )

    meta_rows = [{"table_name": t, "record_count": metadata.row_counts.get(t, 0),
                  "column_count": len(metadata.columns.get(t, []))}
                 for t in table_list]

    return QueryResponse(
        question=question, resolved_question=sanitized,
        sql="(meta query — no SQL needed)", valid=True,
        results={"success": True, "columns": ["table_name", "record_count", "column_count"],
                 "rows": meta_rows, "row_count": len(table_list), "execution_time_ms": 0},
        answer=answer, confidence=1.0, cached=False,
        generation_time_ms=0,
        total_time_ms=int((time.time() - total_start) * 1000),
        attempts=0, model_used="code",
        pipeline_trace={"type": "meta_query"},
    )


@app.get("/api/schema")
async def get_schema():
    if not app_state["schema_loaded"]:
        return {"loaded": False, "tables": []}
    metadata = introspector.metadata
    return {
        "loaded": True,
        "tables": [{
            "name": t,
            "row_count": metadata.row_counts.get(t, 0),
            "columns": [{"name": c.name, "type": c.dtype,
                         "samples": c.sample_values[:3]}
                        for c in metadata.columns.get(t, [])],
        } for t in metadata.tables],
    }


@app.get("/api/tables/{table_name}/preview")
async def preview_table(table_name: str):
    if not app_state["schema_loaded"]:
        raise HTTPException(400, "No database loaded")
    return executor.get_table_preview(table_name, limit=10)


class DiagnoseRequest(BaseModel):
    user_query: str
    failed_sql: str
    correct_sql: str

@app.post("/api/feedback/diagnose")
async def diagnose_feedback(req: DiagnoseRequest):
    from core.offline_critic import offline_critic
    rule = await offline_critic.diagnose_and_learn(
        req.user_query, req.failed_sql, req.correct_sql
    )
    return {"success": True, "rule": rule}


@app.get("/api/feedback/stats")
async def feedback_stats():
    return {
        "cache": cache.stats,
        "experience_memory": experience_memory.get_stats(),
        "query_count": app_state["query_count"],
    }


if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Medical NL2SQL on {Config.HOST}:{Config.PORT}")
    uvicorn.run(app, host=Config.HOST, port=Config.PORT)
