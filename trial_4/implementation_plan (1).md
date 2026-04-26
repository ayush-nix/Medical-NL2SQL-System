# Medical NL2SQL — Implementation Plan v2

> **1,653,146 rows** · 4 XLSX files (DASHBD21–24) · 93 union columns → unified schema

---

## Phase 1 — Column Triage: Drop, Keep, Rename

### 1.1 Columns to DROP (13 — only truly empty or admin junk)

| Column | Why Drop |
|--------|----------|
| `Field7` | 100% null every year |
| `Column1`, `Column2`, `Column3` | 98-100% null placeholders |
| `ward` | 100% null (2023 only) |
| `ICDEXCEL_NO_ID` / `ICDEXCEL_NO.ID` | Excel row counter — no query value |
| `Ser No`, `S No` | Excel/internal serial — no query value |
| `UNIT SUS NO` | Admin code — no NL query references this |
| `cat_ahr` | 64.7% null (2021 only), redundant with `category` (0% null) |
| `icd_cause_code2d` | 99.6% null (2021 only) |
| `diagnosis_code2d` | 95.5% null across years |

**Columns explicitly KEPT** (per user feedback):
- `persnl_sex` — queryable even though 100% null in 2023 (populated in 2021, 2024)
- `persnl_unit` — 97.6% null but has 9,639 values in 2021; queries possible
- `station` — queryable geography column
- `records_office` — 2022 only but queryable
- `religion`, `dist_origin`, `state_origin` — queryable even if only in 2021/2022
- `icd_cause_code1a` — 96% null but clinically meaningful W-code (external causes)

**Result**: 93 → ~51 core columns (before engineered features)

### 1.2 Column Renaming to snake_case

| Original | Canonical | Reasoning |
|----------|-----------|-----------|
| `Area/ Corps` | `area_corps` | Space+slash breaks SQL syntax |
| `MIL HOSP_ID` / `MIL HOSP.ID` | `mil_hosp_id` | Dot/underscore unification |
| `Sheet1_ID` / `Sheet1.ID` | `sheet1_id` | |
| `ICD NO` | `icd_no` | Space removal |
| `DIAGNOSIS` | `diagnosis` | → also called `diagnosis_group` in metadata |
| `ADMISSION` | `admission` | → `admission_broad` in metadata |
| `Command` | `command` | Case normalization |
| All 2024 ALLCAPS | lowercase | `MEDICAL_UNIT`→`medical_unit`, etc. |

### 1.3 Year-Specific Column Remapping

| Source | Year | Target | Evidence |
|--------|------|--------|----------|
| `DIAGNOSIS_CODE` | 2024 | `diagnosis_code1d` | Same ICD-10 discharge codes |
| `icd_code_final` | 2023 | `diagnosis_code1d` | **2023's diagnosis_code1d is 100% null** — 224,774 real values in icd_code_final |
| `diagnosis_final` | 2022 | Check vs `diagnosis_code1d`: where different → `icd_cause_code1d` | 98.9% match, diffs are external cause codes |

---

## Phase 2 — Value Standardisation (8 columns)

### 2.1 `nbb` — case fix + boolean

| Fix | Rows |
|-----|------|
| `y` → `Y` | 1,615 (2021) |

Convert to boolean `is_newborn` column. Keep raw `nbb` too.

### 2.2 `category` — case fix

| Fix | Rows |
|-----|------|
| `Officer` → `OFFICER` | 66 |
| [or](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/static/index.html#346-355) → `OR` | 2 |
| `CIVIL` — keep as-is | 5 (legitimate in 2024) |

Final set: `OR`, `JCO`, `OFFICER`, `RECRUIT`, `MNS`, `CADET`, `CIVIL`

### 2.3 `religion` — spelling fixes

| Variant | Canonical | Rows |
|---------|-----------|------|
| `BUDHIST`, `BUDDIST` | `BUDDHIST` | ~5,059 |
| `HIDU` | `HINDU` | 51 |
| `CHRISTAIN`, `CHRISTIA` | `CHRISTIAN` | 5 |
| `OTHERCOMM`, `OTHERS` | `OTHER COMMUNITIES` | 85 |
| `[SIKH)` | `SIKH` | 1 |

### 2.4 `disposal` — merge free-text variants

| Variant(s) | Canonical | Rows |
|-----------|-----------|------|
| `TRASFER`, `TRANSFFER`, `TRANSMER`, `Transferred to Military Hospital`, etc. | `TRANSFER` | ~6,208 |
| `Discharged to Home`, `Discharge To Home`, `DTH` | `DSCHRGHOME` | ~9,901 |
| `Discharged to Unit`, `Discharge to Unit`, `DTU`, `DISCHARGED TO UNIT` | `DSCHRGUNIT` | ~3,656 |
| `Sick Leave` | `S/L` | ~1,333 |
| `Death` (case) | `DEATH` | 110 |
| `MOUNDDEAD` | `FOUNDDEAD` | 14 |
| `TRANCIVIL` | `TRANSCIVIL` | 936 |
| `READMITY` | `READMITTED` | 19 |
| `-1` | NULL | 305 |

Keep `ABSENTIA` and `ABSCOND` separate (distinct meanings: left without notice vs fled).

### 2.5 `admsn_type` — typo fixes

| Variant | Canonical | Rows |
|---------|-----------|------|
| `TRANSFFER` | `TRANSFER` | 1,315 |
| `MRESH` | `FRESH` | 187 |
| `Fresh` (case) | `FRESH` | 6,572 |
| `MOUNDDEAD`, `FOUND DEAD` | `FOUNDDEAD` | 10 |
| `r`, `-1` | NULL | 3 |

### 2.6 `admission` — merge variants

| Fix | Canonical |
|-----|-----------|
| `OLD/REVIEW` (2021) → `OLD/REFERED` (2022-23) | `OLD/REFERRED` |

### 2.7 `marital_status` — case + merge

| Fix | Canonical |
|-----|-----------|
| `Married` → `MARRIED`, `Single`/`single` → `SINGLE` | case-fix |
| `WIDOWED` → `WIDOW` | merge |
| `Seperated` → `SEPARATED` | typo |
| `DIVORCEE` → `DIVORCED` | merge |
| `-1`, `M`, `UNKNOWN` → NULL | sentinel removal |

### 2.8 `rank` — only fix clear duplicates

| Variant | Canonical | Rows |
|---------|-----------|------|
| `Nk` | `NK` | ~8,989 |
| `HAV` | `Hav` | ~5,296 |
| `NAIK` | `NK` | ~4,989 |
| `SEPOY` | `Sep` | ~2,190 |

**Do NOT touch** the remaining 200+ legitimate military rank codes.

---

## Phase 3 — Structural Corrections (5 integrity fixes)

### 3.1 Date sanitization

| Issue | Affected | Action |
|-------|----------|--------|
| `dschrg_date` = 1970-01-01 | 16,788 rows (mostly 2024) | Set to NULL |
| `dschrg_date` = 1900-01-01 | 2 rows | Set to NULL |
| `admsn_date` = 2017 in 2021 file | 1 row | Investigate: if `dschrg_date` is 2021-range, it's a genuine 4yr stay; otherwise NULL |
| `admsn_date` = 2015 in 2024 file | 440 rows | Set to NULL |

### 3.2 LOS recomputation

- Drop stored `days` column entirely (100% null in 2023, 92.1% null elsewhere, negative values)
- Compute `los_days = dschrg_date - admsn_date` from sanitized dates
- Negative LOS → set to NULL + `las_data_quality_flag = 'INVALID'`
- Keep LOS=0 (legitimate same-day procedures: 16K-33K per year)

### 3.3 `nbb_weight` outlier fixing

| Rule | Rows |
|------|------|
| `nbb_weight > 6000` AND `nbb = Y` → divide by 10 | 1 (26,220 → 2,622g) |
| `nbb_weight = 0` AND `nbb = Y` → NULL | ~2,000 |
| `nbb_weight > 0` AND `nbb = N` → NULL | thousands (weight meaningless for non-newborns) |

### 3.4 `age_month` = 12 rollover

Check rows where `age_month = 12` — should roll into `age_year + 1, age_month = 0`. Affects infant records and downstream `age_group` computation.

### 3.5 `age_year` = 0 suspect flagging

Cross-check: `age_year = 0` AND `relation = 'SELF'` AND `rank` is adult rank → `age_data_quality_flag = 'SUSPECT_ZERO'`. A Havildar cannot be age 0.

---

## Phase 4 — Feature Engineering (16 new columns)

| Feature | Logic | NL2SQL Value |
|---------|-------|-------------|
| `data_year` | Source file year (2021/2022/2023/2024) | "admissions in 2023" |
| `age_group` | `INFANT(<1)`, `CHILD(1-10)`, `ADOLESCENT(11-18)`, `YOUNG_ADULT(19-30)`, `MID_CAREER(31-40)`, `SENIOR(41-50)`, `PRE_RETIREMENT(51-60)`, `ELDERLY(60+)` | "elderly patients" |
| `age_composite_days` | `age_year*365 + age_month*30 + age_days` | Precise neonatal queries |
| `los_category` | `SAME_DAY(0)`, `SHORT(1-3)`, `MEDIUM(4-10)`, `LONG(11-30)`, `VERY_LONG(31-90)`, `CHRONIC(90+)` | "long stay patients" |
| `icd_chapter` | First letter of `diagnosis_code1d` | Disease chapter grouping |
| `icd_chapter_name` | I→Circulatory, F→Mental, etc. | "heart disease" queries |
| `diagnosis_filled` | `COALESCE(diagnosis, mapped_from_ICD)` | Recovers ~32% null DIAGNOSIS |
| `is_death` | `disposal IN (DEATH, FOUNDDEAD)` | "mortality rate" |
| `is_medboard` | `admission='MED BD'` OR `admsn_type IN (RECAT,RMB,IMB,RSMB)` | Medical board queries |
| `is_newborn` | `nbb='Y'` → boolean | Newborn queries |
| `is_self` | `relation='SELF'` → boolean | "soldiers vs dependents" |
| `is_transfer_in` | `admsn_type IN (TRANSFER, TRANSCIVIL)` | Transfer analysis |
| `rank_tier` | `ENLISTED`, `JCO`, `OFFICER`, `SPECIALIST` | Rank group queries |
| `season` | `SUMMER(Apr-Jun)`, `MONSOON(Jul-Sep)`, `AUTUMN(Oct-Nov)`, `WINTER(Dec-Mar)` | Seasonal disease patterns |
| `admission_year` | Extract from `admsn_date` | GROUP BY year |
| `admission_month` | Extract from `admsn_date` | Monthly trends |
| `patient_key` | `CONCAT(and_no, '_', relationship, '_', age_year)` | Unique patient proxy (and_no alone is NOT unique — family members share it) |

---

## Phase 5 — Schema Design: 3 Tables

### Table 1: `admissions` (main fact table, ~67 columns)
All cleaned, enriched rows. ~1.65M records. Primary key: auto-increment `row_id`.

### Table 2: `hospital_lookup`
Built from distinct [(medical_unit, mil_hosp_id, mh, location, command, area_corps)](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/static/index.html#215-222). Used for: "all hospitals in Western Command" without scanning 1.65M rows.

### Table 3: `icd_lookup`
Built from distinct [(diagnosis_code1d → icd_no, diagnosis group, icd_chapter, icd_chapter_name)](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/static/index.html#215-222). Used for: "diabetes patients" → look up `E11` codes.

---

## Phase 6 — NL2SQL Adaptation

### The DIAGNOSIS null problem (32-48% null)
Always generate dual-column queries: `WHERE diagnosis = 'COVID-19' OR diagnosis_code1d = 'U07.1'`. Bake this pattern into system prompt.

### The `and_no` identity problem
`and_no` is the soldier's army number — wife, son, and soldier share it. Use `patient_key` for re-admission tracking.

### Synonym dictionary additions
| Term | Maps To |
|------|---------|
| `jawan` | `category='OR'` or `rank='Sep'` |
| `recat` | `admsn_type='RECAT'` |
| `invalided` | `disposal='INVALIDMNT'` |
| `PME` | `diagnosis_code1d='Z10.2'` |
| `sick attendant` | `diagnosis_code1d='Z76.3'` |

---

## Execution Order (strict sequence)

1. Add `data_year` to each DataFrame BEFORE any other operation
2. Rename all columns to canonical snake_case per-year
3. Apply year-specific remaps (2023 icd_code_final → diagnosis_code1d, 2024 ALLCAPS)
4. Drop the 13 confirmed junk columns
5. Value standardisation (8 columns)
6. Date sanitisation (1970/1900/2015 epochs → NULL)
7. LOS recomputation (drop stored `days`, compute from dates)
8. nbb_weight outlier fixes
9. Age corrections (month=12 rollover, suspect-zero flagging)
10. Add NULL columns for fields missing in specific years
11. `pd.concat()` all 4 years
12. Build & apply hospital lookup (recover 2024 metadata)
13. Feature engineering (16 columns) on unified table
14. Export to SQLite: admissions + hospital_lookup + icd_lookup

---

## Verification Plan

### Automated
- Row count: exactly 1,653,146 after merge
- No negative `los_days` remaining
- `diagnosis_code1d` non-null ≥ 85% overall
- `disposal` max 12 canonical values
- `category` max 7 canonical values
- `data_year` has exactly 4 values with correct per-year counts

### E2E Query Tests
1. `"Top 10 diseases by admissions"` → GROUP BY diagnosis
2. `"Deaths in 2023"` → `WHERE is_death=1 AND data_year=2023`
3. `"Average LOS for cardiac patients"` → `AVG(los_days) WHERE icd_chapter='I'`
4. `"Dengue cases in monsoon season"` → `WHERE diagnosis LIKE '%Dengue%' AND season='MONSOON'`
5. `"JCO vs OR admissions by hospital"` → GROUP BY category, mh

---

## Phase 7 — PostgreSQL Migration (Industry Scale)
### 7.1 Objective
Shift from SQLite/CSVs to a full **PostgreSQL** instance to natively support industry-level performance, indexing, and scalability for the 1.65 Million row dataset. This removes the need for large flat files while guaranteeing 100% data preservation through strong typing.

### 7.2 Implementation Steps
**Step 1: Install & Configure PostgreSQL**
- `sudo apt install postgresql` and start the service.
- Create role `medical_admin` and database `military_hospital`.

**Step 2: Update Data Pipeline**
- Modify [data_cleaning_pipeline.py](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/data_cleaning_pipeline.py) to target the Postgres DB using `SQLAlchemy` + `psycopg2`.
- Stream 1.65M rows directly into Postgres via `to_sql(chunksize=10000)` retaining all strict datatypes (TEXT, INTEGER, REAL).

**Step 3: Update FastApi & Core App Layer**
- Swap [medical/app.py](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/app.py) and [sql_executor.py](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/core/sql_executor.py) from `sqlite3` to `psycopg2` adapter pool.
- Update [schema_introspector.py](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/core/schema_introspector.py) to read core tables dynamically from Postgres' `information_schema` instead of `sqlite_master`.

**Step 4: Update LLM Dialect**
- In [medical_sql_generator.py](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/agents/medical_sql_generator.py) ensure the prompt specifies `PostgreSQL` so that it uses correct function syntax (e.g. `EXTRACT(YEAR FROM...)` instead of `strftime()`).

---

## Phase 8: Data Unification & 3-Tiered Medical NLP Mapping
Based on real-world military dataset limitations (where `diagnosis_code1d` is 40-100% null across years), we will unify the data streams employing a 3-Tiered NLP matching framework to bridge layman terms vs hardcore ICD-10 medical shorthand.

### Tier 1 (Offline Dataset Cleaning & Imputation)
As the backbone of the system, we cannot simply rely on query-time semantics if the actual base column (`diagnosis_code1d`) is null. During [data_cleaning_pipeline_v2.py](file:///home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main/medical/data_cleaning_pipeline_v2.py):
- We will execute a robust regex/dictionary imputation pass over `icd_remarks_d`.
- Shorthand like `CAD` maps to `I25.1`, `HTN`/`HT` maps to `I10`, `TB` maps to `A15`.
- This permanently enriches the PostgreSQL instance with clean ICD codes, significantly lowering the null rate.

### Tier 2 (Explicit Dictionary Router - NL2SQL Layer)
To prevent LLM hallucination and ensure 100% accuracy for the most common 50-100 layman queries ("heart attack", "high bp", "stroke"), we will map user synonyms directly to rigorous ICD-10 prefixes inside the prompt generation code.
- Example: `"high bp": {"icd_pattern": "I10%", "remarks_keywords": ["HYPERTENSION", "HTN"]}`.
- If invoked, the SQL directly filters using `diagnosis_code1d ILIKE 'I10%'` yielding perfect deterministic results.

### Tier 3 (Fuzzy Semantic Vector Search Fallback)
For edge cases (e.g., obscure diseases or misspellings not caught by the dictionary), we implement an offline Vector RAG mechanism.
- We vectorize the explicit medical PDF registry provided.
- If the user query does not trigger `Tier 2`, the agent performs offline cosine-similarity over the PDF embeddings, recovers the precise ICD-10 code, and feeds it dynamically into the SQL generator prompt! 

> [!IMPORTANT]
> Please review this 3-Layer framework! Our pipeline ensures the dataset is permanently cleaned offline (Tier 1), guarantees rapid determinism (Tier 2), while employing bleeding-edge ML scaling for obscure diseases (Tier 3). If approved, I will implement the regex cleaning sweep!
