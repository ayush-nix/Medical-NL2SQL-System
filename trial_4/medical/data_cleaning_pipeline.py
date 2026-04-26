"""
Military Hospital Dataset — Unified Merge & Clean Pipeline
============================================================
Loads 4 yearly XLSX files (DASHBD21-24), unifies schema, cleans values,
recomputes LOS, recovers hospital metadata, engineers features, exports
to CSV + SQLite.

Run:
    cd /home/dell/Pictures/ayush/Prakshepan_Landslide-NL2SQL-System--main
    source venv/bin/activate
    python medical/data_cleaning_pipeline.py

Output:
    medical/cleaned_data/unified_admissions.csv
    medical/cleaned_data/unified_admissions.db  (SQLite: admissions + hospital_lookup + icd_lookup)
    medical/cleaned_data/merge_audit_report.txt
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from pathlib import Path
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "DATA"
OUTPUT_DIR = Path(__file__).parent / "cleaned_data"

INPUT_FILES = {
    2021: DATA_DIR / "DASHBD21.xlsx",
    2022: DATA_DIR / "DASHBD22.xlsx",
    2023: DATA_DIR / "DASHBD23.xlsx",
    2024: DATA_DIR / "DASHBD24.xlsx",
}

# ═══════════════════════════════════════════════════════════════
# COLUMNS TO DROP — Only truly empty or admin-junk
# Reasoning: If a user can conceivably ask an NL query about it, KEEP IT.
# ═══════════════════════════════════════════════════════════════

GLOBAL_DROPS = {
    # 100% null across ALL years — zero information
    'Field7':          '100% null every year — pure artifact',
    # 100% null placeholders from Excel export
    'Column1':         '100% null (2024 only)',
    'Column2':         '98.1% null (2023 only) — unnamed placeholder',
    'Column3':         '99.1% null (2023 only) — unnamed placeholder',
    'ward':            '100% null (2023 only)',
    # Excel/database row counters — no analytical value
    'ICDEXCEL_NO_ID':  'Excel row counter (underscore variant)',
    'ICDEXCEL_NO.ID':  'Excel row counter (dot variant)',
    'Ser No':          'Excel sequence number',
    'S No':            'Internal serial number',
    'UNIT SUS NO':     'Administrative unit suspension code — never queried',
    # Redundant with better alternatives
    'cat_ahr':         '64.7% null, 2021 only, fully redundant with category (0% null)',
    'patient_status':  '100% constant "fresh" in 2023 — zero-variance column',
}

# ═══════════════════════════════════════════════════════════════
# VALUE STANDARDISATION MAPS
# Only for CATEGORICAL columns — never for numeric values like age.
# Purpose: fix typos, case-variants, and free-text duplicates.
# ═══════════════════════════════════════════════════════════════

DISPOSAL_MAP = {
    # Typos
    'TRASFER': 'TRANSFER', 'TRANSFFER': 'TRANSFER', 'TRANSMER': 'TRANSFER',
    # Free-text → code
    'Transferred to Military Hospital': 'TRANSFER',
    'TRANSFERRED TO ANOTHER UNIT': 'TRANSFER',
    'Transferred to Another Military Hospital': 'TRANSFER',
    'Transfer': 'TRANSFER', 'transfer': 'TRANSFER',
    'Discharged to Home': 'DSCHRGHOME', 'Discharge To Home': 'DSCHRGHOME',
    'DTH': 'DSCHRGHOME', 'DISCHARGED HOME': 'DSCHRGHOME',
    'Discharged to Unit': 'DSCHRGUNIT', 'Discharge to Unit': 'DSCHRGUNIT',
    'DTU': 'DSCHRGUNIT', 'DISCHARGED TO UNIT': 'DSCHRGUNIT',
    'Sick Leave': 'S/L', 'SICK LEAVE': 'S/L', 'Sick leave': 'S/L',
    'Death': 'DEATH', 'death': 'DEATH', 'DIED': 'DEATH',
    'MOUNDDEAD': 'FOUNDDEAD',
    'TRANCIVIL': 'TRANSCIVIL',
    'READMITY': 'READMITTED',
    # Sentinels
    '-1': None,
}

RELIGION_MAP = {
    'BUDHIST': 'BUDDHIST', 'BUDDIST': 'BUDDHIST', 'BUDDHAIST': 'BUDDHIST',
    'HIDU': 'HINDU',
    'CHRISTAIN': 'CHRISTIAN', 'CHRISTIA': 'CHRISTIAN',
    'OTHERCOMM': 'OTHER COMMUNITIES', 'OTHERS': 'OTHER COMMUNITIES',
    '[SIKH)': 'SIKH',
    'JEWISH': 'JEWS',
}

CATEGORY_MAP = {
    'Officer': 'OFFICER', 'officer': 'OFFICER',
    'or': 'OR',
    # CIVIL stays as-is (legitimate 2024 value)
}

MARITAL_STATUS_MAP = {
    'Married': 'MARRIED', 'married': 'MARRIED', 'M': 'MARRIED',
    'Single': 'SINGLE', 'single': 'SINGLE',
    'Seperated': 'SEPARATED', 'SEPERATED': 'SEPARATED',
    'WIDOWED': 'WIDOW',
    'DIVORCEE': 'DIVORCED', 'Divorcee': 'DIVORCED',
    # WIDOWER stays as-is (male widower, distinct from WIDOW)
    '-1': None,
    'UNKNOWN': None, 'Unknown': None,
}

ADMSN_TYPE_MAP = {
    'TRANSFFER': 'TRANSFER',
    'MRESH': 'FRESH', 'Fresh': 'FRESH', 'fresh': 'FRESH',
    'MOUNDDEAD': 'FOUNDDEAD', 'FOUND DEAD': 'FOUNDDEAD', 'FOUND_DEAD': 'FOUNDDEAD',
    'TRANCIVIL': 'TRANSCIVIL',
    '-1': None,
    'r': None,
    'TRANSFER ': 'TRANSFER',  # trailing space variant
}

ADMISSION_MAP = {
    'OLD/REVIEW': 'OLD/REFERRED',
    'OLD/REFERED': 'OLD/REFERRED',
}

NBB_MAP = {'y': 'Y'}

RANK_MAP = {
    'Nk': 'NK', 'NAIK': 'NK', 'naik': 'NK',
    'HAV': 'Hav', 'HAVILDAR': 'Hav',
    'SEPOY': 'Sep', 'sepoy': 'Sep',
}

# ═══════════════════════════════════════════════════════════════
# ICD CHAPTER MAPPING
# ═══════════════════════════════════════════════════════════════

ICD_CHAPTER_NAMES = {
    'A': 'Infectious & Parasitic Diseases',
    'B': 'Infectious & Parasitic Diseases',
    'C': 'Neoplasms',
    'D': 'Blood & Immune Disorders / Neoplasms',
    'E': 'Endocrine, Nutritional & Metabolic',
    'F': 'Mental & Behavioural Disorders',
    'G': 'Nervous System Diseases',
    'H': 'Eye & Ear Diseases',
    'I': 'Circulatory System Diseases',
    'J': 'Respiratory System Diseases',
    'K': 'Digestive System Diseases',
    'L': 'Skin & Subcutaneous Diseases',
    'M': 'Musculoskeletal & Connective Tissue',
    'N': 'Genitourinary System Diseases',
    'O': 'Pregnancy, Childbirth & Puerperium',
    'P': 'Perinatal Conditions',
    'Q': 'Congenital Malformations',
    'R': 'Symptoms & Abnormal Findings',
    'S': 'Injury (Body Region)',
    'T': 'Injury (Poisoning, Effects)',
    'U': 'Special Purpose Codes (COVID etc.)',
    'V': 'External Causes - Transport',
    'W': 'External Causes - Falls/Exposure',
    'X': 'External Causes - Other',
    'Y': 'External Causes - Medical/Surgical',
    'Z': 'Health Services & Examinations',
}

RANK_TIER_MAP = {
    # ENLISTED (Other Ranks)
    'Sep': 'ENLISTED', 'NK': 'ENLISTED', 'Hav': 'ENLISTED',
    'L/NK': 'ENLISTED', 'L/HAV': 'ENLISTED',
    'Rfn': 'ENLISTED', 'Cfn': 'ENLISTED', 'Spr': 'ENLISTED',
    'Sigmn': 'ENLISTED', 'Dvr': 'ENLISTED', 'Gnr': 'ENLISTED',
    'SWR': 'ENLISTED', 'Sap': 'ENLISTED', 'Pnr': 'ENLISTED',
    'HMT': 'ENLISTED', 'CMH': 'ENLISTED', 'DFR': 'ENLISTED',
    # JCO
    'Nb/Sub': 'JCO', 'Sub': 'JCO', 'S/Maj': 'JCO', 'Sub Maj': 'JCO',
    # OFFICER
    'Lt': 'OFFICER', 'Capt': 'OFFICER', 'Major': 'OFFICER', 'Maj': 'OFFICER',
    'Lt Col': 'OFFICER', 'Col': 'OFFICER', 'Brig': 'OFFICER',
    'Maj Gen': 'OFFICER', 'Lt Gen': 'OFFICER', 'Gen': 'OFFICER',
    # SPECIALIST
    'Cadet': 'SPECIALIST', 'Rect': 'SPECIALIST', 'MNS': 'SPECIALIST',
    'other': 'SPECIALIST',
}

# ═══════════════════════════════════════════════════════════════
# AUDIT LOGGING
# ═══════════════════════════════════════════════════════════════

AUDIT = []

def log(msg=""):
    print(msg)
    AUDIT.append(str(msg))

def section(title):
    log(f"\n{'='*70}")
    log(f"  {title}")
    log(f"{'='*70}")

# ═══════════════════════════════════════════════════════════════
# STEP 1: LOAD + ADD data_year + RENAME + DROP JUNK
# (Per-year, BEFORE any merge — preserves year-conditional logic)
# ═══════════════════════════════════════════════════════════════

def _common_drops(df):
    """Drop globally-identified junk columns."""
    to_drop = [c for c in GLOBAL_DROPS if c in df.columns]
    if to_drop:
        df.drop(columns=to_drop, inplace=True)
        log(f"    Dropped {len(to_drop)} junk columns: {to_drop}")
    return df

def _common_renames(df):
    """Rename columns present across multiple years to canonical snake_case."""
    rename_map = {}
    for old, new in [
        ('RELATION', 'relation'), ('ADMISSION', 'admission'),
        ('MH', 'mh'), ('LOCATION', 'location'), ('Command', 'command'),
        ('Area/ Corps', 'area_corps'), ('ICD NO', 'icd_no'),
        ('DIAGNOSIS', 'diagnosis'), ('CATEGORY', 'category'),
    ]:
        if old in df.columns:
            rename_map[old] = new
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
    return df


def load_year(year, path):
    section(f"LOADING {year} — {path.name}")
    df = pd.read_excel(path, engine='openpyxl')
    log(f"  Raw shape: {df.shape[0]:,} rows × {df.shape[1]} cols")

    # ── Step 1a: Add data_year FIRST (before any transforms) ──
    df['data_year'] = year

    # ── Step 1b: Year-specific pre-processing ──
    if year == 2024:
        # 2024 uses ALLCAPS — lowercase everything
        df.columns = [c.lower() if c != 'data_year' else c for c in df.columns]
        log("    2024: lowercased all column names")

    # ── Step 1c: Drop global junk ──
    # Case-insensitive drop matching (since 2024 is now lowercase)
    cols_lower = {c.lower(): c for c in df.columns}
    to_drop = []
    for junk_col in GLOBAL_DROPS:
        junk_lower = junk_col.lower()
        if junk_lower in cols_lower:
            to_drop.append(cols_lower[junk_lower])
    if to_drop:
        df.drop(columns=to_drop, inplace=True, errors='ignore')
        log(f"    Dropped {len(to_drop)} junk columns")

    # ── Step 1d: Year-specific column remapping ──
    if year == 2021:
        df.rename(columns={
            'ID1': 'id1',
            'Sheet1_ID': 'sheet1_id',
            'MIL HOSP_ID': 'mil_hosp_id',
        }, inplace=True)
        _common_renames(df)
        # Add columns absent in 2021
        for col in ['persnl_marital_status', 'records_office']:
            if col not in df.columns:
                df[col] = np.nan
        # Drop diagnosis_final if present (processed in 2022, junk in other years)
        if 'diagnosis_final' in df.columns:
            df.drop(columns=['diagnosis_final'], inplace=True)

    elif year == 2022:
        # diagnosis_final: 98.9% matches diagnosis_code1d.
        # Where different → it's the external cause code → use as icd_cause_code1d
        if 'diagnosis_final' in df.columns and 'diagnosis_code1d' in df.columns:
            mask = df['diagnosis_final'].notna() & (df['diagnosis_final'] != df['diagnosis_code1d'])
            n_diff = mask.sum()
            # Only override icd_cause_code1d where diagnosis_final differs
            if 'icd_cause_code1d' not in df.columns:
                df['icd_cause_code1d'] = np.nan
            df.loc[mask, 'icd_cause_code1d'] = df.loc[mask, 'diagnosis_final']
            df.drop(columns=['diagnosis_final'], inplace=True)
            log(f"    2022: diagnosis_final → icd_cause_code1d for {n_diff:,} differing rows, then dropped")
        else:
            if 'diagnosis_final' in df.columns:
                df.drop(columns=['diagnosis_final'], inplace=True)

        df.rename(columns={
            'ICDEXCEL_NO.ID': '_drop_icd', 'ID1': 'id1',
            'Sheet1.ID': 'sheet1_id', 'MIL HOSP.ID': 'mil_hosp_id',
        }, inplace=True)
        _common_renames(df)
        df.drop(columns=[c for c in df.columns if c.startswith('_drop')], inplace=True, errors='ignore')
        for col in ['station', 'persnl_sex', 'admsn_dschrg_flag_d']:
            if col not in df.columns:
                df[col] = np.nan

    elif year == 2023:
        # CRITICAL: diagnosis_code1d is 100% null in 2023.
        # Real ICD discharge codes are in icd_code_final (224,774 non-null).
        if 'icd_code_final' in df.columns:
            null_before = df['diagnosis_code1d'].isna().sum() if 'diagnosis_code1d' in df.columns else 0
            df['diagnosis_code1d'] = df['icd_code_final']
            df.drop(columns=['icd_code_final'], inplace=True)
            log(f"    2023: icd_code_final → diagnosis_code1d (was {null_before:,} nulls, "
                f"now {df['diagnosis_code1d'].isna().sum():,} nulls)")

        # 2023 has duplicate ID columns — keep the populated one
        if 'Sheet1.ID' in df.columns:
            df.drop(columns=['Sheet1.ID'], inplace=True)  # 100% null version
        if 'Sheet1_ID' in df.columns:
            df.rename(columns={'Sheet1_ID': 'sheet1_id'}, inplace=True)
        if 'MIL HOSP.ID' in df.columns:
            df.drop(columns=['MIL HOSP.ID'], inplace=True)  # 100% null in 2023
        if 'MIL HOSP_ID' in df.columns:
            df.rename(columns={'MIL HOSP_ID': 'mil_hosp_id'}, inplace=True)

        # Drop remaining Excel ID duplicates
        for c in ['ICDEXCEL_NO.ID', 'ICDEXCEL_NO_ID']:
            if c in df.columns:
                df.drop(columns=[c], inplace=True)

        _common_renames(df)
        for col in ['id1', 'religion', 'dist_origin', 'state_origin',
                     'persnl_marital_status', 'records_office',
                     'admsn_dschrg_flag_d', 'diagnosis_code1a',
                     'icd_cause_code1a', 'icd_cause_code1d',
                     'icd_cause_code2d', 'diagnosis_code2d']:
            if col not in df.columns:
                df[col] = np.nan

    elif year == 2024:
        # After lowercasing, rename 2024-specific names → canonical
        df.rename(columns={
            'diagnosis_code': 'diagnosis_code1d',
            'sheet1.id': 'sheet1_id',
            'mil hosp.id': 'mil_hosp_id',
            'area/ corps': 'area_corps',
            'icd no': 'icd_no',
        }, inplace=True)
        # 2024 has 'relationship' but 2021-23 use 'relationship' — already matches after lowercase
        for col in ['id1', 'age_month', 'age_days', 'marital_status',
                     'station', 'arm_corps', 'religion', 'dist_origin', 'state_origin',
                     'records_office', 'persnl_marital_status', 'persnl_unit_desc',
                     'admsn_dschrg_flag_d', 'diagnosis_code1a',
                     'icd_cause_code1a', 'icd_cause_code1d',
                     'icd_cause_code2d', 'diagnosis_code2d']:
            if col not in df.columns:
                df[col] = np.nan

    log(f"  After normalise: {df.shape[0]:,} rows × {df.shape[1]} cols")
    log(f"  Columns: {sorted(df.columns.tolist())}")
    return df


# ═══════════════════════════════════════════════════════════════
# STEP 2: VALUE STANDARDISATION (categorical columns ONLY)
# ═══════════════════════════════════════════════════════════════

def _apply_map(series, mapping, col_name):
    """Apply a mapping dict, log changes."""
    if series is None or len(series) == 0:
        return series
    mask = series.isin(mapping.keys())
    n_changed = mask.sum()
    if n_changed > 0:
        series = series.replace(mapping)
        log(f"    {col_name}: standardised {n_changed:,} values")
    return series

def standardise_values(df):
    section("VALUE STANDARDISATION")

    # disposal
    if 'disposal' in df.columns:
        df['disposal'] = _apply_map(df['disposal'], DISPOSAL_MAP, 'disposal')

    # religion
    if 'religion' in df.columns:
        df['religion'] = _apply_map(df['religion'], RELIGION_MAP, 'religion')

    # category
    if 'category' in df.columns:
        df['category'] = _apply_map(df['category'], CATEGORY_MAP, 'category')

    # marital_status
    if 'marital_status' in df.columns:
        df['marital_status'] = _apply_map(df['marital_status'], MARITAL_STATUS_MAP, 'marital_status')

    # admsn_type
    if 'admsn_type' in df.columns:
        df['admsn_type'] = _apply_map(df['admsn_type'], ADMSN_TYPE_MAP, 'admsn_type')

    # admission
    if 'admission' in df.columns:
        df['admission'] = _apply_map(df['admission'], ADMISSION_MAP, 'admission')

    # nbb
    if 'nbb' in df.columns:
        df['nbb'] = _apply_map(df['nbb'], NBB_MAP, 'nbb')

    # rank (only clear duplicates)
    if 'rank' in df.columns:
        df['rank'] = _apply_map(df['rank'], RANK_MAP, 'rank')

    # admsn_dschrg_flag_d — W19 and S4D are ICD codes entered in wrong field
    if 'admsn_dschrg_flag_d' in df.columns:
        bad_mask = df['admsn_dschrg_flag_d'].isin(['W19', 'S4D'])
        n_bad = bad_mask.sum()
        if n_bad > 0:
            df.loc[bad_mask, 'admsn_dschrg_flag_d'] = np.nan
            log(f"    admsn_dschrg_flag_d: {n_bad} bad values (W19/S4D) → NULL")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 3: DATE SANITISATION
# ═══════════════════════════════════════════════════════════════

def sanitise_dates(df):
    section("DATE SANITISATION")

    df['admsn_date'] = pd.to_datetime(df['admsn_date'], errors='coerce')
    df['dschrg_date'] = pd.to_datetime(df['dschrg_date'], errors='coerce')

    # Discharge dates: anything before 2000 is a database artifact
    # (1970-01-01 = UNIX epoch, 1900-01-01 = Excel epoch)
    bad_dschrg = df['dschrg_date'] < pd.Timestamp('2000-01-01')
    n_bad_dschrg = bad_dschrg.sum()
    if n_bad_dschrg > 0:
        df.loc[bad_dschrg, 'dschrg_date'] = pd.NaT
        log(f"  dschrg_date < 2000: {n_bad_dschrg:,} rows → NaT "
            f"(1970 epoch artifacts, 1900 Excel epoch)")

    # Admission dates: anything before 2019 is suspicious
    # Exception: a 2017 admission in 2021 file might be a genuine long-stay
    bad_admsn = df['admsn_date'] < pd.Timestamp('2019-01-01')
    n_bad_admsn = bad_admsn.sum()
    if n_bad_admsn > 0:
        # Check if dschrg_date is reasonable — if so, keep (genuine long stay)
        genuine_long_stay = bad_admsn & df['dschrg_date'].notna() & (df['dschrg_date'] >= pd.Timestamp('2020-01-01'))
        truly_bad = bad_admsn & ~genuine_long_stay
        n_kept = genuine_long_stay.sum()
        n_nulled = truly_bad.sum()
        df.loc[truly_bad, 'admsn_date'] = pd.NaT
        log(f"  admsn_date < 2019: {n_bad_admsn:,} total — "
            f"{n_kept:,} kept (genuine long-stay), {n_nulled:,} → NaT")

    # Future discharge dates (after 2025-06-01) — data entry errors
    bad_future = df['dschrg_date'] > pd.Timestamp('2025-06-01')
    n_future = bad_future.sum()
    if n_future > 0:
        df.loc[bad_future, 'dschrg_date'] = pd.NaT
        log(f"  dschrg_date > 2025-06: {n_future:,} rows → NaT (future dates)")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 4: LOS RECOMPUTATION
# ═══════════════════════════════════════════════════════════════

def compute_los(df):
    section("LOS RECOMPUTATION")

    # Drop stored 'days' column — unreliable (100% null in 2023, absent in 2022/2024,
    # 264 negatives in 2021, methodology may differ per year)
    if 'days' in df.columns:
        non_null = df['days'].notna().sum()
        log(f"  Dropped original 'days' column ({non_null:,} non-null values were present)")
        df.drop(columns=['days'], inplace=True)

    # Compute fresh from sanitised dates
    df['los_days'] = (df['dschrg_date'] - df['admsn_date']).dt.days

    # Negative LOS → set to NULL (don't delete rows — rest of data is valid)
    neg_mask = df['los_days'] < 0
    n_neg = neg_mask.sum()
    if n_neg > 0:
        df.loc[neg_mask, 'los_days'] = np.nan
        log(f"  Negative LOS: {n_neg:,} rows → NULL")

    # Stats
    valid = df['los_days'].dropna()
    log(f"  LOS stats (valid {len(valid):,} rows):")
    log(f"    min={valid.min():.0f}  max={valid.max():.0f}  "
        f"mean={valid.mean():.1f}  median={valid.median():.0f}")
    log(f"    Zero (same-day): {(valid == 0).sum():,}")
    log(f"    NULL (no discharge / bad date): {df['los_days'].isna().sum():,}")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 5: nbb_weight OUTLIER FIXING
# ═══════════════════════════════════════════════════════════════

def fix_nbb_weight(df):
    section("NBB_WEIGHT OUTLIER FIXING")

    if 'nbb_weight' not in df.columns or 'nbb' not in df.columns:
        log("  Skipped — columns not present")
        return df

    # Ensure numeric
    df['nbb_weight'] = pd.to_numeric(df['nbb_weight'], errors='coerce')

    # Rule 1: nbb_weight > 6000 AND nbb=Y → divide by 10 (misplaced decimal)
    big_newborn = (df['nbb_weight'] > 6000) & (df['nbb'] == 'Y')
    n_big = big_newborn.sum()
    if n_big > 0:
        df.loc[big_newborn, 'nbb_weight'] = df.loc[big_newborn, 'nbb_weight'] / 10
        log(f"  nbb_weight > 6000g & nbb=Y: {n_big:,} rows divided by 10 (decimal fix)")

    # Rule 2: nbb_weight = 0 AND nbb=Y → NULL (missing, not zero-weight baby)
    # Reasoning: zero weight for a newborn makes no clinical sense — these are missing entries
    zero_newborn = (df['nbb_weight'] == 0) & (df['nbb'] == 'Y')
    n_zero = zero_newborn.sum()
    if n_zero > 0:
        df.loc[zero_newborn, 'nbb_weight'] = np.nan
        log(f"  nbb_weight = 0 & nbb=Y: {n_zero:,} rows → NULL (zero = missing, not dead baby)")

    # Rule 3: ALL nbb_weight for nbb != Y → NULL entirely
    # Reasoning: weight has no meaning for non-newborns. Both 0 and >0 values should be NULL.
    non_newborn = df['nbb'] != 'Y'
    n_nn = (non_newborn & df['nbb_weight'].notna()).sum()
    if n_nn > 0:
        df.loc[non_newborn, 'nbb_weight'] = np.nan
        log(f"  nbb_weight for nbb!=Y: {n_nn:,} rows → NULL (weight meaningless for non-newborns)")

    log(f"  Final nbb_weight: {df['nbb_weight'].notna().sum():,} valid values remain")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 6: AGE CORRECTIONS
# ═══════════════════════════════════════════════════════════════

def fix_ages(df):
    section("AGE CORRECTIONS")

    # Convert age_year to numeric
    df['age_year'] = pd.to_numeric(df['age_year'], errors='coerce')
    if 'age_month' in df.columns:
        df['age_month'] = pd.to_numeric(df['age_month'], errors='coerce')
    if 'age_days' in df.columns:
        df['age_days'] = pd.to_numeric(df['age_days'], errors='coerce')

    # Fix age_month = 12 rollover (should be age_year + 1, age_month = 0)
    if 'age_month' in df.columns:
        rollover = df['age_month'] == 12
        n_roll = rollover.sum()
        if n_roll > 0:
            df.loc[rollover, 'age_year'] = df.loc[rollover, 'age_year'] + 1
            df.loc[rollover, 'age_month'] = 0
            log(f"  age_month=12 rollover: {n_roll:,} rows corrected")

    # Negative age_days → NULL (2 rows in 2023)
    if 'age_days' in df.columns:
        neg_days = df['age_days'] < 0
        n_neg = neg_days.sum()
        if n_neg > 0:
            df.loc[neg_days, 'age_days'] = np.nan
            log(f"  Negative age_days: {n_neg:,} rows → NULL")

    # Negative persnl_age_year → NULL (1 row in 2022)
    if 'persnl_age_year' in df.columns:
        df['persnl_age_year'] = pd.to_numeric(df['persnl_age_year'], errors='coerce')
        neg_persnl = df['persnl_age_year'] < 0
        if neg_persnl.sum() > 0:
            df.loc[neg_persnl, 'persnl_age_year'] = np.nan
            log(f"  Negative persnl_age_year: {neg_persnl.sum():,} rows → NULL")

    # service_years outlier: max=132 in 2024 — clearly wrong
    if 'service_years' in df.columns:
        df['service_years'] = pd.to_numeric(df['service_years'], errors='coerce')
        bad_svc = df['service_years'] > 50  # Max military service is ~40 years
        n_bad = bad_svc.sum()
        if n_bad > 0:
            df.loc[bad_svc, 'service_years'] = np.nan
            log(f"  service_years > 50: {n_bad:,} rows → NULL (impossible values)")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 7: STATION NORMALISATION
# APO/field posting addresses → canonical format
# ═══════════════════════════════════════════════════════════════

import re as _re

def normalise_stations(df):
    section("STATION NORMALISATION")

    if 'station' not in df.columns:
        log("  Skipped — column not present")
        return df

    # Clean up whitespace and case
    df['station'] = df['station'].astype(str).str.strip().str.upper()
    df.loc[df['station'].isin(['NAN', 'NONE', '', '-1', 'NA']), 'station'] = np.nan

    # APO code normalisation: "C/O 56 APO" → "CO_56_APO"
    apo_pattern = _re.compile(r'C/?O\s*(\d+)\s*APO', _re.IGNORECASE)
    valid_mask = df['station'].notna()
    apo_fixed = df.loc[valid_mask, 'station'].str.replace(
        apo_pattern, lambda m: f"CO_{m.group(1)}_APO", regex=True
    )
    n_apo = (apo_fixed != df.loc[valid_mask, 'station']).sum()
    df.loc[valid_mask, 'station'] = apo_fixed

    # Consolidate common HQ variants
    hq_map = {
        'HQ SOUTHERN COMMAND': 'HQ_SC',
        'HQ WESTERN COMMAND': 'HQ_WC',
        'HQ EASTERN COMMAND': 'HQ_EC',
        'HQ NORTHERN COMMAND': 'HQ_NC',
        'HQ CENTRAL COMMAND': 'HQ_CC',
    }
    n_hq = 0
    for variant, canonical in hq_map.items():
        mask = df['station'] == variant
        n_hq += mask.sum()
        df.loc[mask, 'station'] = canonical

    log(f"  APO codes normalised: {n_apo:,} values")
    log(f"  HQ variants normalised: {n_hq:,} values")
    log(f"  Unique stations after cleanup: {df['station'].nunique():,}")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 8: HOSPITAL METADATA RECOVERY (for null years)
# medical_unit is 0% null — it's a deterministic key to hospital info
# ═══════════════════════════════════════════════════════════════

def recover_hospital_metadata(df):
    section("HOSPITAL METADATA RECOVERY")

    # Build lookup from ALL rows where data exists
    hosp_cols = ['command', 'area_corps', 'mh', 'location', 'mil_hosp_id']
    lookup_src = df[['medical_unit'] + hosp_cols].dropna(subset=['medical_unit'])

    # For each medical_unit, take the mode (most frequent) value for each field
    lookup = {}
    for col in hosp_cols:
        valid = lookup_src[['medical_unit', col]].dropna()
        if len(valid) > 0:
            modes = valid.groupby('medical_unit')[col].agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else np.nan)
            lookup[col] = modes

    log(f"  Built lookup from {lookup_src['medical_unit'].nunique():,} unique medical_units")

    # Fill nulls using lookup
    for col in hosp_cols:
        if col in lookup:
            before = df[col].isna().sum()
            filled = df['medical_unit'].map(lookup[col])
            df[col] = df[col].fillna(filled)
            after = df[col].isna().sum()
            recovered = before - after
            if recovered > 0:
                log(f"  {col}: recovered {recovered:,} nulls → {after:,} remain")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 8: FEATURE ENGINEERING (16 derived columns)
# ═══════════════════════════════════════════════════════════════

def engineer_features(df):
    section("FEATURE ENGINEERING")

    # 1. age_group
    bins = [-1, 0, 1, 10, 18, 30, 40, 50, 60, 200]
    labels = ['NEONATE', 'INFANT', 'CHILD', 'ADOLESCENT', 'YOUNG_ADULT',
              'MID_CAREER', 'SENIOR', 'PRE_RETIREMENT', 'ELDERLY']
    df['age_group'] = pd.cut(df['age_year'], bins=bins, labels=labels, right=True)
    df['age_group'] = df['age_group'].astype(str).replace('nan', np.nan)
    log(f"  age_group: {df['age_group'].notna().sum():,} classified")

    # 2. age_composite_days (precise age for neonatal queries)
    age_y = df['age_year'].fillna(0)
    age_m = df['age_month'].fillna(0) if 'age_month' in df.columns else 0
    age_d = df['age_days'].fillna(0) if 'age_days' in df.columns else 0
    df['age_composite_days'] = (age_y * 365 + age_m * 30 + age_d).astype('Int64')
    # NULL out where age_year itself is null
    df.loc[df['age_year'].isna(), 'age_composite_days'] = pd.NA
    log(f"  age_composite_days: computed for {df['age_composite_days'].notna().sum():,} rows")

    # 3. los_category
    def los_cat(d):
        if pd.isna(d): return np.nan
        if d == 0: return 'SAME_DAY'
        if d <= 3: return 'SHORT'
        if d <= 10: return 'MEDIUM'
        if d <= 30: return 'LONG'
        if d <= 90: return 'VERY_LONG'
        return 'CHRONIC'
    df['los_category'] = df['los_days'].apply(los_cat)
    log(f"  los_category: {df['los_category'].notna().sum():,} classified")

    # 4. icd_chapter (first letter of diagnosis_code1d)
    df['icd_chapter'] = df['diagnosis_code1d'].astype(str).str[0].str.upper()
    df.loc[df['diagnosis_code1d'].isna(), 'icd_chapter'] = np.nan
    # Filter to only valid ICD letters
    valid_chapters = set(ICD_CHAPTER_NAMES.keys())
    df.loc[~df['icd_chapter'].isin(valid_chapters), 'icd_chapter'] = np.nan
    log(f"  icd_chapter: {df['icd_chapter'].notna().sum():,} extracted")

    # 5. icd_chapter_name
    df['icd_chapter_name'] = df['icd_chapter'].map(ICD_CHAPTER_NAMES)
    log(f"  icd_chapter_name: {df['icd_chapter_name'].notna().sum():,} mapped")

    # 6. diagnosis_filled — best-effort fill for the 32-48% null DIAGNOSIS
    # Strategy: Use diagnosis as-is where available, otherwise map from ICD code
    # Build a code→diagnosis lookup from existing matched rows
    matched = df[df['diagnosis'].notna() & df['icd_no'].notna()][['icd_no', 'diagnosis']]
    if len(matched) > 0:
        icd_to_diag = matched.groupby('icd_no')['diagnosis'].agg(
            lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else np.nan
        )
        log(f"  Built ICD→diagnosis lookup: {len(icd_to_diag):,} unique codes")
        df['diagnosis_filled'] = df['diagnosis'].fillna(df['icd_no'].map(icd_to_diag))
    else:
        df['diagnosis_filled'] = df['diagnosis']
    before_null = df['diagnosis'].isna().sum()
    after_null = df['diagnosis_filled'].isna().sum()
    log(f"  diagnosis_filled: recovered {before_null - after_null:,} of {before_null:,} nulls "
        f"({(before_null-after_null)/max(before_null,1)*100:.1f}%)")

    # 7. is_death
    df['is_death'] = df['disposal'].isin(['DEATH', 'FOUNDDEAD']).astype('Int8')
    df.loc[df['disposal'].isna(), 'is_death'] = pd.NA
    log(f"  is_death: {(df['is_death'] == 1).sum():,} deaths flagged")

    # 8. is_medboard
    medboard_admission = df['admission'] == 'MED BD'
    medboard_type = df['admsn_type'].isin(['RECAT', 'RMB', 'IMB', 'RSMB']) if 'admsn_type' in df.columns else pd.Series(False, index=df.index)
    df['is_medboard'] = (medboard_admission | medboard_type).astype('Int8')
    log(f"  is_medboard: {(df['is_medboard'] == 1).sum():,} rows")

    # 9. is_newborn
    df['is_newborn'] = (df['nbb'] == 'Y').astype('Int8')
    df.loc[df['nbb'].isna(), 'is_newborn'] = pd.NA
    log(f"  is_newborn: {(df['is_newborn'] == 1).sum():,} newborns")

    # 10. is_self
    df['is_self'] = (df['relation'] == 'SELF').astype('Int8')
    df.loc[df['relation'].isna(), 'is_self'] = pd.NA
    log(f"  is_self: {(df['is_self'] == 1).sum():,} self / {(df['is_self'] == 0).sum():,} dependents")

    # 11. is_transfer_in
    transfer_types = ['TRANSFER', 'TRANSCIVIL']
    df['is_transfer_in'] = df['admsn_type'].isin(transfer_types).astype('Int8') if 'admsn_type' in df.columns else pd.NA
    log(f"  is_transfer_in: {(df['is_transfer_in'] == 1).sum():,} transfers")

    # 12. rank_tier
    df['rank_tier'] = df['rank'].map(RANK_TIER_MAP)
    # Unmapped ranks → 'OTHER'
    df.loc[df['rank'].notna() & df['rank_tier'].isna(), 'rank_tier'] = 'OTHER'
    log(f"  rank_tier: {df['rank_tier'].notna().sum():,} classified")

    # 13. season_of_admission
    month = df['admsn_date'].dt.month
    conditions = [
        month.between(4, 6),   # SUMMER
        month.between(7, 9),   # MONSOON
        month.between(10, 11), # AUTUMN
        month.isin([12, 1, 2, 3]),  # WINTER
    ]
    choices = ['SUMMER', 'MONSOON', 'AUTUMN', 'WINTER']
    df['season'] = np.select(conditions, choices, default='')
    df.loc[df['admsn_date'].isna(), 'season'] = np.nan
    df.loc[df['season'] == '', 'season'] = np.nan
    log(f"  season: {df['season'].notna().sum():,} classified")

    # 14. admission_year
    df['admission_year'] = df['admsn_date'].dt.year.astype('Int64')
    log(f"  admission_year: {df['admission_year'].notna().sum():,} extracted")

    # 15. admission_month
    df['admission_month'] = df['admsn_date'].dt.month.astype('Int64')
    log(f"  admission_month: {df['admission_month'].notna().sum():,} extracted")

    # 16. patient_key (proxy unique patient ID — and_no alone is NOT unique)
    df['patient_key'] = (
        df['and_no'].astype(str).str.strip() + '_' +
        df['relationship'].astype(str).str.strip() + '_' +
        df['age_year'].astype(str).str.strip()
    )
    # NULL out where and_no is null
    df.loc[df['and_no'].isna(), 'patient_key'] = np.nan
    log(f"  patient_key: {df['patient_key'].notna().sum():,} generated "
        f"({df['patient_key'].nunique():,} unique patients)")

    # Data quality flags
    df['flag_age_suspect'] = 0
    suspect = (df['age_year'] == 0) & (df['relation'] == 'SELF') & df['rank'].notna()
    df.loc[suspect, 'flag_age_suspect'] = 1
    n_suspect = suspect.sum()
    if n_suspect > 0:
        log(f"  flag_age_suspect: {n_suspect:,} rows (age=0 but SELF with rank)")

    return df


# ═══════════════════════════════════════════════════════════════
# STEP 9: EXPORT TO CSV + SQLite (3 tables)
# ═══════════════════════════════════════════════════════════════

def export(df):
    section("EXPORT")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── CSV ──
    csv_path = OUTPUT_DIR / "unified_admissions.csv"
    df.to_csv(csv_path, index=False)
    log(f"  CSV saved: {csv_path} ({os.path.getsize(csv_path)/1024/1024:.1f} MB)")

    # ── SQLite (3 tables) ──
    db_path = OUTPUT_DIR / "unified_admissions.db"
    conn = sqlite3.connect(str(db_path))

    # Table 1: admissions (main fact table)
    df.to_sql('admissions', conn, if_exists='replace', index=False)
    log(f"  SQLite admissions table: {len(df):,} rows × {len(df.columns)} cols")

    # Table 2: hospital_lookup (dimension)
    hosp_cols = ['medical_unit', 'mil_hosp_id', 'mh', 'location', 'command', 'area_corps']
    existing_hosp_cols = [c for c in hosp_cols if c in df.columns]
    hospital = (
        df[existing_hosp_cols]
        .dropna(subset=['medical_unit'])
        .drop_duplicates(subset=['medical_unit'])
        .reset_index(drop=True)
    )
    # Cast mil_hosp_id to string to avoid mixed-type sort errors
    if 'mil_hosp_id' in hospital.columns:
        hospital['mil_hosp_id'] = hospital['mil_hosp_id'].astype(str).replace('nan', '')
    hospital = hospital.sort_values('medical_unit', ignore_index=True)
    hospital.to_sql('hospital_lookup', conn, if_exists='replace', index=False)
    log(f"  SQLite hospital_lookup table: {len(hospital):,} hospitals")

    # Table 3: icd_lookup (dimension)
    icd_cols_present = [c for c in ['diagnosis_code1d', 'icd_no', 'diagnosis', 'icd_chapter', 'icd_chapter_name'] if c in df.columns]
    if len(icd_cols_present) >= 2:
        icd = (
            df[icd_cols_present]
            .dropna(subset=['diagnosis_code1d'])
            .drop_duplicates(subset=['diagnosis_code1d'])
            .reset_index(drop=True)
        )
        icd = icd.sort_values('diagnosis_code1d', ignore_index=True)
        icd.to_sql('icd_lookup', conn, if_exists='replace', index=False)
        log(f"  SQLite icd_lookup table: {len(icd):,} unique ICD codes")

    conn.close()
    log(f"  SQLite saved: {db_path} ({os.path.getsize(db_path)/1024/1024:.1f} MB)")

    return csv_path, db_path


# ═══════════════════════════════════════════════════════════════
# STEP 10: FINAL AUDIT
# ═══════════════════════════════════════════════════════════════

def final_audit(df):
    section("FINAL AUDIT")

    log(f"  Total rows    : {len(df):,}")
    log(f"  Total columns : {len(df.columns)}")
    log(f"\n  Year breakdown:")
    for yr, cnt in df['data_year'].value_counts().sort_index().items():
        log(f"    {int(yr)}: {cnt:,} rows")

    log(f"\n  Column list ({len(df.columns)}):")
    for i, col in enumerate(sorted(df.columns)):
        null_pct = df[col].isna().mean() * 100
        nunique = df[col].nunique()
        flag = " ⚠️ HIGH NULL" if null_pct > 50 else ""
        log(f"    {i+1:2d}. {col:<30} null={null_pct:5.1f}%  unique={nunique:>8,}{flag}")

    # Category validation
    for col, expected_max in [('category', 10), ('command', 8), ('sex', 4), ('relation', 4)]:
        if col in df.columns:
            unique_vals = df[col].dropna().unique()
            log(f"\n  {col} unique values ({len(unique_vals)}): {sorted(unique_vals)[:20]}")


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def main():
    log("=" * 70)
    log("  MILITARY HOSPITAL — UNIFIED MERGE & CLEAN PIPELINE")
    log(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)

    # ── Step 1: Load each year independently ──
    dfs = {}
    for year, path in INPUT_FILES.items():
        if not path.exists():
            log(f"\n  ⚠️ FILE NOT FOUND: {path}")
            continue
        dfs[year] = load_year(year, path)

    if not dfs:
        log("\nERROR: No files loaded!")
        return None

    # ── Step 2: Stack all years ──
    section("MERGING ALL YEARS")
    # First ensure all DataFrames have the same columns
    all_cols = set()
    for df in dfs.values():
        all_cols.update(df.columns)
    log(f"  Union of all columns: {len(all_cols)}")

    for year, df in dfs.items():
        missing = all_cols - set(df.columns)
        for col in missing:
            df[col] = np.nan
        if missing:
            log(f"  [{year}] Added {len(missing)} NULL columns for missing fields")

    unified = pd.concat(dfs.values(), ignore_index=True)
    log(f"  Merged shape: {len(unified):,} rows × {len(unified.columns)} cols")

    # ── Step 2b: Drop any leaked columns ──
    for leak_col in ['diagnosis_final']:
        if leak_col in unified.columns:
            unified.drop(columns=[leak_col], inplace=True)
            log(f"  Dropped leaked column: {leak_col}")

    # ── Step 3: Value standardisation ──
    unified = standardise_values(unified)

    # ── Step 4: Date sanitisation ──
    unified = sanitise_dates(unified)

    # ── Step 5: LOS recomputation ──
    unified = compute_los(unified)

    # ── Step 6: nbb_weight fixes ──
    unified = fix_nbb_weight(unified)

    # ── Step 7: Age corrections ──
    unified = fix_ages(unified)

    # ── Step 8: Station normalisation ──
    unified = normalise_stations(unified)

    # ── Step 9: Hospital metadata recovery ──
    unified = recover_hospital_metadata(unified)

    # ── Step 10: Feature engineering ──
    unified = engineer_features(unified)

    # ── Step 10: Final audit ──
    final_audit(unified)

    # ── Step 11: Export ──
    csv_path, db_path = export(unified)

    # ── Save audit report ──
    audit_path = OUTPUT_DIR / "merge_audit_report.txt"
    with open(audit_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(AUDIT))
    log(f"\n  Audit report: {audit_path}")

    log(f"\n{'='*70}")
    log("  PIPELINE COMPLETE")
    log(f"{'='*70}")

    return unified


if __name__ == '__main__':
    df = main()
