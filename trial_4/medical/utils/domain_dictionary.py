"""
Medical Domain Dictionary — Military Hospital NL2SQL System
Maps military/medical abbreviations, domain terms, and vague user language
to precise SQL context hints. All offline, deterministic.

PIVOT LOG from landslide system:
  - REMOVED: All geoscience terms (NDVI, slope, seismic, precipitation, etc.)
  - ADDED: Military ranks, categories, disposal statuses, ICD-10 patterns,
    hospital terminology, ECHS/PME/PMB terms, LOS categories
  - CHANGED: GROUP_KEYWORDS from terrain/weather groups to
    patient/hospital/clinical/military groups matching column_metadata.json
"""
import logging

logger = logging.getLogger("medical.domain_dictionary")

# ── Military/Medical abbreviations → full forms ───────────────
ABBREVIATIONS = {
    # Military ranks
    "sep": "sepoy",
    "nk": "naik",
    "hav": "havildar",
    "nb sub": "naib subedar",
    "sub maj": "subedar major",
    "lt col": "lieutenant colonel",
    "brig": "brigadier",
    "maj gen": "major general",
    "lt gen": "lieutenant general",

    # Military terms
    "or": "other ranks",
    "jco": "junior commissioned officer",
    "mns": "military nursing service",
    "echs": "ex-servicemen contributory health scheme",
    "pmb": "permanent medical board",
    "rsmb": "release service medical board",
    "imb": "invalidment medical board",
    "rmb": "review medical board",
    "pme": "periodic medical examination",
    "med bd": "medical board",
    "s/l": "sick leave",

    # Medical terms
    "los": "length of stay",
    "icd": "international classification of diseases",
    "nbb": "newborn baby",
    "dschrg": "discharge",
    "admsn": "admission",
    "dth": "discharged to home",
    "dtu": "discharged to unit",
    "pii": "personally identifiable information",

    # Command abbreviations
    "wc": "western command",
    "ec": "eastern command",
    "nc": "northern command",
    "sc": "southern command",
    "swc": "south western command",
    "cc": "central command",
}

# ── Business/domain terms → SQL context hints for the LLM ────
# These guide Agent 3 (SQL Generator) on how to translate vague
# human language into precise WHERE/GROUP BY/ORDER BY clauses.
BUSINESS_TERM_HINTS = {
    # ── Mortality / Death ──
    "died": "Filter by is_death = 1",
    "mortality": "Filter by is_death = 1",
    "death rate": "Use CAST(SUM(is_death) AS REAL) / COUNT(*) for mortality rate",
    "deaths": "Filter by is_death = 1 (includes disposal = DEATH and FOUNDDEAD)",
    "found dead": "Filter by disposal = 'FOUNDDEAD'",
    "survived": "Filter by is_death = 0",

    # ── Patient identity ──
    "soldier": "Filter by relation = 'SELF'",
    "soldiers": "Filter by relation = 'SELF'",
    "jawan": "Filter by category = 'OR' (other ranks = enlisted soldiers)",
    "jawans": "Filter by category = 'OR'",
    "dependent": "Filter by relation = 'DEPENDENTS'",
    "dependents": "Filter by relation = 'DEPENDENTS'",
    "family member": "Filter by relation = 'DEPENDENTS'",
    "wife": "Filter by relationship = 'WIFE'",
    "son": "Filter by relationship = 'SON'",
    "daughter": "Filter by relationship = 'DAUGHTER'",
    "male": "Filter by sex = 'M'",
    "female": "Filter by sex = 'F'",
    "men": "Filter by sex = 'M'",
    "women": "Filter by sex = 'F'",

    # ── Newborn ──
    "newborn": "Filter by is_newborn = 1 (nbb = 'Y')",
    "baby": "Filter by is_newborn = 1",
    "neonatal": "Filter by age_group = 'NEONATE' (age_year = 0)",
    "infant": "Filter by age_group = 'INFANT' (age_year = 1)",

    # ── Age groups ──
    "child": "Filter by age_group = 'CHILD' (age 1-10)",
    "children": "Filter by age_group = 'CHILD'",
    "adolescent": "Filter by age_group = 'ADOLESCENT' (age 11-18)",
    "young": "Filter by age_group = 'YOUNG_ADULT' (age 19-30)",
    "elderly": "Filter by age_group = 'ELDERLY' (age 60+)",
    "old": "Filter by age_group IN ('PRE_RETIREMENT', 'ELDERLY')  (age 51+)",

    # ── Military category ──
    "officer": "Filter by category = 'OFFICER' or rank_tier = 'OFFICER'",
    "officers": "Filter by category = 'OFFICER'",
    "recruit": "Filter by category = 'RECRUIT'",
    "cadet": "Filter by category = 'CADET'",
    "civilian": "Filter by category = 'CIVIL'",

    # ── Medical board / invalidation ──
    "medical board": "Filter by is_medboard = 1 (admission = 'MED BD' or admsn_type IN ('RECAT','RMB','IMB','RSMB'))",
    "invalided": "Filter by disposal = 'INVALIDMNT'",
    "invalidment": "Filter by disposal = 'INVALIDMNT'",
    "recat": "Filter by admsn_type = 'RECAT' (recategorization medical board)",
    "fitness": "Look at admsn_type and disposal for medical board outcomes",

    # ── Admission / Discharge ──
    "admitted": "Count rows, each row = one admission",
    "discharged": "Filter by disposal IS NOT NULL",
    "sick leave": "Filter by disposal = 'S/L'",
    "transfer": "Filter by disposal = 'TRANSFER' or admsn_type = 'TRANSFER'",
    "transferred": "Filter by disposal = 'TRANSFER' or is_transfer_in = 1",
    "readmitted": "Filter by disposal = 'READMITTED'",
    "absconded": "Filter by disposal = 'ABSCOND'",
    "fresh admission": "Filter by admsn_type = 'FRESH'",
    "referred": "Filter by admission = 'OLD/REFERRED'",

    # ── Length of stay ──
    "long stay": "Filter by los_category IN ('LONG', 'VERY_LONG', 'CHRONIC') or los_days > 30",
    "short stay": "Filter by los_category = 'SHORT' (1-3 days)",
    "same day": "Filter by los_category = 'SAME_DAY' (los_days = 0)",
    "chronic": "Filter by los_category = 'CHRONIC' (los_days > 90)",
    "average stay": "Use AVG(los_days) function",
    "average length of stay": "Use AVG(los_days) function",

    # ── Diseases (search disease_standard_name first, then ICD codes) ──
    "disease": "Search disease_standard_name column with ILIKE for best results",
    "dengue": "Filter by disease_standard_name ILIKE '%dengue%' OR diagnosis_code1d LIKE 'A90%'",
    "malaria": "Filter by disease_standard_name ILIKE '%malaria%' OR diagnosis_code1d LIKE 'B5%'",
    "covid": "Filter by disease_standard_name ILIKE '%COVID%' OR diagnosis_code1d LIKE 'U07%'",
    "coronavirus": "Filter by disease_standard_name ILIKE '%COVID%' OR diagnosis_code1d LIKE 'U07%'",
    "tuberculosis": "Filter by disease_standard_name ILIKE '%tuberculosis%' OR diagnosis_code1d LIKE 'A15%'",
    "tb": "Filter by disease_standard_name ILIKE '%tuberculosis%' OR diagnosis_code1d LIKE 'A15%'",
    "diabetes": "Filter by disease_standard_name ILIKE '%diabetes%' OR diagnosis_code1d LIKE 'E1%'",
    "sugar": "Filter by disease_standard_name ILIKE '%diabetes%' (sugar = diabetes in Indian context)",
    "hypertension": "Filter by disease_standard_name ILIKE '%hypertension%' OR diagnosis_code1d = 'I10'",
    "high bp": "Filter by disease_standard_name ILIKE '%hypertension%' OR diagnosis_code1d = 'I10'",
    "bp": "Filter by disease_standard_name ILIKE '%hypertension%' OR diagnosis_code1d = 'I10'",
    "heart disease": "Filter by icd_chapter = 'I' (Circulatory System Diseases)",
    "heart attack": "Filter by disease_standard_name ILIKE '%myocardial infarction%' OR diagnosis_code1d LIKE 'I21%'",
    "cardiac": "Filter by icd_chapter = 'I'",
    "mental health": "Filter by icd_chapter = 'F' (Mental & Behavioural Disorders)",
    "psychiatric": "Filter by icd_chapter = 'F'",
    "depression": "Filter by disease_standard_name ILIKE '%depress%' OR diagnosis_code1d LIKE 'F32%'",
    "alcohol": "Filter by disease_standard_name ILIKE '%alcohol%' OR diagnosis_code1d LIKE 'F10%'",
    "drinking": "Filter by disease_standard_name ILIKE '%alcohol%' (ADS = Alcohol Dependence Syndrome)",
    "injury": "Filter by icd_chapter = 'S' (Injury Body Region)",
    "fracture": "Filter by disease_standard_name ILIKE '%fracture%'",
    "fall": "Filter by disease_standard_name ILIKE '%fall%' OR icd_chapter = 'W'",
    "respiratory": "Filter by icd_chapter = 'J' (Respiratory System)",
    "pneumonia": "Filter by disease_standard_name ILIKE '%pneumonia%' OR diagnosis_code1d LIKE 'J18%'",
    "appendicitis": "Filter by disease_standard_name ILIKE '%appendicitis%' OR diagnosis_code1d LIKE 'K35%'",
    "cancer": "Filter by icd_chapter = 'C' (Neoplasms)",
    "musculoskeletal": "Filter by icd_chapter = 'M'",
    "back pain": "Filter by disease_standard_name ILIKE '%back pain%' OR diagnosis_code1d LIKE 'M54%'",
    "pregnancy": "Filter by icd_chapter = 'O' (Pregnancy, Childbirth)",
    "skin disease": "Filter by icd_chapter = 'L' (Skin Diseases)",
    "digestive": "Filter by icd_chapter = 'K'",
    "eye disease": "Filter by icd_chapter = 'H' (Eye & Ear)",
    "kidney": "Filter by icd_chapter = 'N' (Genitourinary)",
    "kidney stone": "Filter by disease_standard_name ILIKE '%calculus%' OR disease_standard_name ILIKE '%kidney stone%'",
    "sick attendant": "Filter by disease_standard_name ILIKE '%accompanying sick%' OR diagnosis_code1d = 'Z76.3'",

    # ── Hospital / Command ──
    "hospital": "Group by or filter on medical_unit column",
    "hospitals": "Use GROUP BY medical_unit or SELECT DISTINCT medical_unit",
    "western command": "Filter by command = 'WC'",
    "eastern command": "Filter by command = 'EC'",
    "northern command": "Filter by command = 'NC'",
    "southern command": "Filter by command = 'SC'",

    # ── Seasonal / Temporal ──
    "monsoon": "Filter by season = 'MONSOON' (Jul-Sep)",
    "summer": "Filter by season = 'SUMMER' (Apr-Jun)",
    "winter": "Filter by season = 'WINTER' (Dec-Mar)",
    "yearly": "GROUP BY data_year for year-on-year comparison",
    "year wise": "GROUP BY data_year",
    "year on year": "GROUP BY data_year ORDER BY data_year",
    "monthly": "GROUP BY admission_month",
    "month wise": "GROUP BY admission_month ORDER BY admission_month",

    # ── Religion ──
    "hindu": "Filter by religion = 'HINDU'",
    "muslim": "Filter by religion = 'MUSLIM'",
    "sikh": "Filter by religion = 'SIKH'",
    "christian": "Filter by religion = 'CHRISTIAN'",
    "buddhist": "Filter by religion = 'BUDDHIST'",

    # ── Aggregation patterns ──
    "average": "Use AVG() function",
    "total": "Use SUM() function or COUNT(*)",
    "count": "Use COUNT(*) function",
    "maximum": "Use MAX() function",
    "minimum": "Use MIN() function",
    "trend": "GROUP BY data_year or admission_month ORDER BY data_year/admission_month",
    "top": "ORDER BY target_col DESC LIMIT N",
    "highest": "ORDER BY target_col DESC LIMIT N",
    "lowest": "ORDER BY target_col ASC LIMIT N",
    "most common": "GROUP BY target_col ORDER BY COUNT(*) DESC LIMIT N",
    "distribution": "GROUP BY target_col with COUNT(*)",
    "percentage": "Use CAST(SUM(condition) AS REAL) * 100.0 / COUNT(*)",
    "rate": "Use CAST(SUM(condition) AS REAL) / COUNT(*)",
}

# ── Column group keyword mapping ─────────────────────────────
# Used by Agent 1 to detect which column groups are relevant.
# Maps user-language keywords → column_metadata.json groups.
GROUP_KEYWORDS = {
    "provenance": ["year", "source", "data year", "which year", "2021", "2022", "2023", "2024"],
    "hospital": ["hospital", "medical unit", "mh", "location", "city", "command",
                 "eastern", "western", "northern", "southern", "corps", "area"],
    "patient_identity": ["patient", "name", "age", "sex", "gender", "male", "female",
                        "married", "single", "relationship", "self", "dependent",
                        "wife", "son", "daughter", "family", "and number"],
    "newborn": ["newborn", "baby", "neonatal", "birth weight", "nbb", "infant"],
    "military_identity": ["rank", "category", "officer", "jco", "jawan", "sepoy",
                          "havildar", "subedar", "cadet", "recruit", "mns", "civil",
                          "service years", "unit", "personnel", "soldier", "serviceman"],
    "geography": ["station", "formation", "religion", "district", "state", "origin",
                  "hindu", "muslim", "sikh", "christian", "records office", "posting"],
    "admission_discharge": ["admission", "discharge", "admitted", "disposed", "disposal",
                            "length of stay", "los", "transfer", "death", "sick leave",
                            "died", "mortality", "same day", "chronic", "long stay",
                            "fresh", "referred", "medical board", "invalidment"],
    "clinical_icd": ["diagnosis", "icd", "disease", "condition", "code", "chapter",
                     "dengue", "malaria", "covid", "tb", "tuberculosis", "diabetes",
                     "hypertension", "cardiac", "heart", "mental", "psychiatric",
                     "injury", "fracture", "cancer", "respiratory", "pneumonia",
                     "infectious", "surgical", "musculoskeletal", "back pain"],
    "derived_features": ["age group", "season", "monsoon", "summer", "winter",
                         "autumn", "monthly", "yearly", "trend", "rate",
                         "is death", "medboard", "transfer in"],
    "quality_flags": ["suspect", "data quality", "flag", "invalid"],
}

logger.info(f"Medical domain dictionary loaded: {len(ABBREVIATIONS)} abbreviations, "
            f"{len(BUSINESS_TERM_HINTS)} business hints, {len(GROUP_KEYWORDS)} groups")
