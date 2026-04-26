# Prakshepan Intelligence Engine — Sikkim Landslide Prediction NL2SQL

**Air-gapped, fully offline Natural Language to SQL system for Sikkim Landslide Prediction data.**

Built for the Indian Army — zero internet dependency, local LLM inference via Ollama.

---

## Architecture

```
User Query
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  Agent 0: Input Guardrails (instant)                     │
│  SQL injection, prompt injection, length checks          │
├─────────────────────────────────────────────────────────┤
│  Agent 1: Query Understanding (1 LLM call)               │
│  Abbreviation expansion → Fuzzy matching → Intent        │
│  classification → CoT decomposition → Column validation  │
├─────────────────────────────────────────────────────────┤
│  Agent 6: Experience Memory (instant)                    │
│  RLHF rule retrieval via multi-signal RRF fusion         │
├─────────────────────────────────────────────────────────┤
│  Agent 2: Schema Pruner (instant, zero LLM)              │
│  Group-aware BM25 scoring → Adaptive top-K selection     │
│  56 columns → 25-40 relevant columns                    │
├─────────────────────────────────────────────────────────┤
│  Agent 3: SQL Generator (1 LLM call)                     │
│  DIN-SQL adaptive prompting (EASY/MEDIUM/HARD)           │
│  10 SQL pattern skeletons for complex queries            │
├─────────────────────────────────────────────────────────┤
│  Column Sanitizer (instant, AST-level)                   │
│  Fixes hallucinated column names before validation       │
├─────────────────────────────────────────────────────────┤
│  6-Pass Validator (instant, zero LLM)                    │
│  Syntax → Safety → Schema → Types → Ranges → Consistency│
├─────────────────────────────────────────────────────────┤
│  Agent 4: Self-Correction Refiner (conditional LLM)      │
│  Auto-fixes validation failures (up to 2 retries)        │
├─────────────────────────────────────────────────────────┤
│  SQL Executor (instant, read-only SQLite)                 │
├─────────────────────────────────────────────────────────┤
│  Agent 5: Answer Synthesizer (instant)                   │
│  Formats results into natural language                   │
└─────────────────────────────────────────────────────────┘
```

## Dataset: Sikkim Landslide Prediction

**Table:** `prediction` — 56 columns across 12 groups:

| Group | Columns | Key Fields |
|:------|:--------|:-----------|
| Identity | 3 | `id`, `encrypted_lat`, `encrypted_lon` |
| Prediction | 2 | `prediction` (0/1), `landslide_probability` (0-1) |
| Temporal | 1 | `prediction_date` (YYYY-MM-DD) |
| Terrain | 10 | `elevation`, `slope` (radians), `aspect`, `flowaccumulation`, etc. |
| Geological | 4 | `lithology`, `geomorphology`, `soiltexture`, `LULC` (categorical) |
| Precipitation | 7 | `Daily_Precipitation`, `Relative_Humidity`, cumulative 3d/7d |
| Soil Moisture | 6 | `Surface_Soil_Moisture`, `Root_Zone_Soil_Moisture`, cumulative |
| Evapotranspiration | 3 | `Daily_Evapotranspiration`, averages 3d/7d |
| Vegetation | 1 | `NDVI` (-1 to 1) |
| Seismic | 2 | `seismic_pga`, `seismic_count` |
| Meteorological | 16 | Wind, pressure, cloud, visibility, snow, lightning, fog, radar |
| Derived | 1 | `Rainfall_Slope_Interaction` |

## Quick Start

### Prerequisites
- Python 3.10+
- [Ollama](https://ollama.ai) running locally with `llama3.1:8b`

### Install & Run
```bash
# Install dependencies
pip install -r requirements.txt

# Start Ollama (separate terminal)
ollama serve
ollama pull llama3.1:8b

# Start the server
python app.py
```

Open **http://localhost:8000** → Click **"Load Landslide Dataset"** → Ask questions.

### Example Queries
```
Which locations had high landslide risk on December 25, 2025?
Show top 5 records with highest landslide probability
What is the average precipitation for areas with prediction = 1?
Show areas where slope is steeper than 30 degrees
Which dates had the most dangerous predictions?
```

## Key Design Decisions

| Decision | Rationale |
|:---------|:----------|
| **Pruning budget 25-40 / 56 cols** | 56 total columns allows generous context without overwhelming the 8B model |
| **Self-JOIN on encrypted coordinates** | `encrypted_lat = encrypted_lat` works for temporal comparisons even with opaque coordinates |
| **Only 2 output columns** | `prediction` (0/1) + `landslide_probability` (0-1) — cleaner than avalanche's 5 risk indicators |
| **Slope/aspect in radians** | Deterministic conversion injected: "30 degrees" → `> 0.5236 radians` |
| **Categorical columns flagged** | `lithology`, `geomorphology`, `soiltexture`, `LULC` use `=` not `>` comparisons |
| **Dataset-agnostic core** | All agents read from `column_metadata.json` — swap JSON to change domain |

## File Structure
```
├── app.py                      # FastAPI orchestrator (6-agent pipeline)
├── config.py                   # Model config, pruning budget, mandatory columns
├── requirements.txt            # Python dependencies
├── core/
│   ├── input_guardrails.py     # Agent 0: SQL/prompt injection protection
│   ├── query_understanding.py  # Agent 1: NL → structured plan (1 LLM call)
│   ├── schema_pruner.py        # Agent 2: BM25 column selection (zero LLM)
│   ├── sql_generator.py        # Agent 3: Adaptive SQL generation (1 LLM call)
│   ├── column_sanitizer.py     # AST-level column name correction
│   ├── sql_validator.py        # 6-pass deterministic validation
│   ├── sql_refiner.py          # Agent 4: Auto-correction (conditional LLM)
│   ├── sql_executor.py         # Read-only SQLite execution
│   ├── answer_synthesizer.py   # Agent 5: Result → natural language
│   ├── experience_memory.py    # Agent 6: RLHF rule learning & retrieval
│   ├── schema_introspector.py  # CSV → SQLite loader + profiling
│   ├── offline_critic.py       # Async rule extraction from corrections
│   ├── query_cache.py          # LRU query cache
│   └── query_logger.py         # Query audit trail
├── data/
│   ├── column_metadata.json    # 56 columns with groups, synonyms, units
│   └── experience_db.json      # Learned RLHF rules (starts empty)
├── models/
│   └── llm_manager.py          # Ollama client with dual-model fallback
├── utils/
│   ├── domain_dictionary.py    # Landslide domain abbreviations & hints
│   ├── fuzzy_matcher.py        # Jaro-Winkler column matching
│   └── text_utils.py           # Tokenization, BM25 scoring
├── static/
│   └── index.html              # Frontend UI
├── test_data/
│   └── sikkim_landslide_data.csv  # 100-row dummy dataset
└── test_e2e.py                 # End-to-end API test
```

## Models

| Role | Primary | Fallback |
|:-----|:--------|:---------|
| Reasoning (Agent 1) | `gpt_oss_120b:latest` | `llama3.1:8b` |
| SQL Generation (Agent 3) | `llama3.1:8b` | `llama3.1:8b` |

Auto-detection: if primary model isn't available in Ollama, falls back silently.

## API Endpoints

| Endpoint | Method | Description |
|:---------|:-------|:------------|
| `/` | GET | Frontend UI |
| `/api/health` | GET | System health + model status |
| `/api/load-army-data` | POST | Load bundled landslide dataset |
| `/api/upload` | POST | Upload custom CSV/ZIP |
| `/api/query` | POST | Submit NL query |
| `/api/schema` | GET | View loaded schema |
| `/api/feedback/diagnose` | POST | Submit SQL correction for learning |

## License

Internal use — Indian Army / DRDO.
