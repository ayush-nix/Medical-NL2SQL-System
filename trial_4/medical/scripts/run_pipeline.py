"""
Military Hospital — Full Data Cleaning & PostgreSQL Export Pipeline
Loads 4 XLSX (2021-2024), unifies schema, cleans, engineers features,
exports to PostgreSQL.
"""
import pandas as pd
import numpy as np
import os, re, sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "DATA1"
PG_URL = "postgresql://postgres:postgres@localhost:5432/military_hospital"

INPUT_FILES = {
    2021: DATA_DIR / "DASHBD21 (1).xlsx",
    2022: DATA_DIR / "DASHBD22 (1).xlsx",
    2023: DATA_DIR / "DASHBD23.xlsx",
    2024: DATA_DIR / "DASHBD24.xlsx",
}

# Only drop TRUE junk — 100% null or Excel artifacts
JUNK_DROPS = [
    'Field7', 'Column1', 'Column2', 'Column3', 'ward',
    'ICDEXCEL_NO_ID', 'ICDEXCEL_NO.ID', 'Ser No', 'S No',
    'UNIT SUS NO', 'cat_ahr', 'patient_status',
]

# ═══════════════════════════════════════════════════════════════
# VALUE MAPS
# ═══════════════════════════════════════════════════════════════
DISPOSAL_MAP = {
    'TRASFER':'TRANSFER','TRANSFFER':'TRANSFER','TRANSMER':'TRANSFER',
    'Transferred to Military Hospital':'TRANSFER',
    'TRANSFERRED TO ANOTHER UNIT':'TRANSFER',
    'Transferred to Another Military Hospital':'TRANSFER',
    'Transfer':'TRANSFER','transfer':'TRANSFER',
    'TR TO ECHS EMANELLED HOSPITAL':'TRANSFER',
    'Discharged to Home':'DSCHRGHOME','Discharge To Home':'DSCHRGHOME',
    'DTH':'DSCHRGHOME','DISCHARGED HOME':'DSCHRGHOME',
    'Discharged to Unit':'DSCHRGUNIT','Discharge to Unit':'DSCHRGUNIT',
    'DTU':'DSCHRGUNIT','DISCHARGED TO UNIT':'DSCHRGUNIT',
    'Sick Leave':'S/L','SICK LEAVE':'S/L','Sick leave':'S/L',
    'Death':'DEATH','death':'DEATH','DIED':'DEATH',
    'MOUNDDEAD':'FOUNDDEAD','STILL ADMITTED':None,
    'TRANCIVIL':'TRANSCIVIL','READMITY':'READMITTED','-1':None,
}

CATEGORY_MAP = {'Officer':'OFFICER','officer':'OFFICER','or':'OR'}
ADMSN_TYPE_MAP = {
    'TRANSFFER':'TRANSFER','MRESH':'FRESH','Fresh':'FRESH','fresh':'FRESH',
    'MOUNDDEAD':'FOUNDDEAD','FOUND DEAD':'FOUNDDEAD','FOUND_DEAD':'FOUNDDEAD',
    'TRANCIVIL':'TRANSCIVIL','-1':None,'r':None,'TRANSFER ':'TRANSFER',
}
ADMISSION_MAP = {'OLD/REVIEW':'OLD/REFERRED','OLD/REFERED':'OLD/REFERRED'}
MARITAL_MAP = {
    'Married':'MARRIED','married':'MARRIED','M':'MARRIED',
    'Single':'SINGLE','single':'SINGLE','Seperated':'SEPARATED',
    'SEPERATED':'SEPARATED','WIDOWED':'WIDOW','DIVORCEE':'DIVORCED',
    'Divorcee':'DIVORCED','-1':None,'UNKNOWN':None,'Unknown':None,
}
RELIGION_MAP = {
    'BUDHIST':'BUDDHIST','BUDDIST':'BUDDHIST','BUDDHAIST':'BUDDHIST',
    'HIDU':'HINDU','CHRISTAIN':'CHRISTIAN','CHRISTIA':'CHRISTIAN',
    'OTHERCOMM':'OTHER COMMUNITIES','OTHERS':'OTHER COMMUNITIES',
    '[SIKH)':'SIKH','JEWISH':'JEWS',
}
RANK_MAP = {'Nk':'NK','NAIK':'NK','naik':'NK','HAV':'Hav',
            'HAVILDAR':'Hav','SEPOY':'Sep','sepoy':'Sep','SEP':'Sep',
            'LT COL':'Lt Col','MAJ':'Maj','SIGNAL MAN':'Sigmn'}
NBB_MAP = {'y':'Y'}

ICD_CHAPTER_NAMES = {
    'A':'Infectious & Parasitic','B':'Infectious & Parasitic',
    'C':'Neoplasms','D':'Blood & Immune / Neoplasms',
    'E':'Endocrine, Nutritional & Metabolic','F':'Mental & Behavioural',
    'G':'Nervous System','H':'Eye & Ear','I':'Circulatory System',
    'J':'Respiratory System','K':'Digestive System',
    'L':'Skin & Subcutaneous','M':'Musculoskeletal',
    'N':'Genitourinary','O':'Pregnancy & Childbirth',
    'P':'Perinatal','Q':'Congenital Malformations',
    'R':'Symptoms & Abnormal Findings','S':'Injury (Body Region)',
    'T':'Injury (Poisoning)','U':'Special Purpose (COVID)',
    'V':'External - Transport','W':'External - Falls',
    'X':'External - Other','Y':'External - Medical','Z':'Health Services',
}

RANK_TIER_MAP = {
    'Sep':'ENLISTED','NK':'ENLISTED','Hav':'ENLISTED','L/NK':'ENLISTED',
    'L/HAV':'ENLISTED','Rfn':'ENLISTED','Cfn':'ENLISTED','Spr':'ENLISTED',
    'Sigmn':'ENLISTED','Dvr':'ENLISTED','Gnr':'ENLISTED','SWR':'ENLISTED',
    'Sap':'ENLISTED','Pnr':'ENLISTED',
    'Nb/Sub':'JCO','Sub':'JCO','S/Maj':'JCO','Sub Maj':'JCO',
    'Lt':'OFFICER','Capt':'OFFICER','Major':'OFFICER','Maj':'OFFICER',
    'Lt Col':'OFFICER','Col':'OFFICER','Brig':'OFFICER',
    'Maj Gen':'OFFICER','Lt Gen':'OFFICER','Gen':'OFFICER',
    'Cadet':'SPECIALIST','Rect':'SPECIALIST','MNS':'SPECIALIST',
}

AUDIT = []
def log(msg=""): 
    msg = str(msg).replace('\u2192', '->').replace('\u00f7', '/') 
    try: print(msg)
    except: print(msg.encode('ascii', 'replace').decode())
    AUDIT.append(msg)
def section(t): log(f"\n{'='*60}\n  {t}\n{'='*60}")

# ═══════════════════════════════════════════════════════════════
# STEP 1: LOAD + NORMALIZE PER YEAR
# ═══════════════════════════════════════════════════════════════
def load_year(year, path):
    section(f"LOADING {year} — {path.name}")
    df = pd.read_excel(path, engine='openpyxl')
    log(f"  Raw: {df.shape[0]:,} rows × {df.shape[1]} cols")
    df['data_year'] = year

    # 2024: lowercase all columns
    if year == 2024:
        df.columns = [c.lower() if c != 'data_year' else c for c in df.columns]

    # Drop junk (case-insensitive)
    cols_lower = {c.lower(): c for c in df.columns}
    to_drop = [cols_lower[j.lower()] for j in JUNK_DROPS if j.lower() in cols_lower]
    if to_drop:
        df.drop(columns=to_drop, inplace=True, errors='ignore')
        log(f"  Dropped {len(to_drop)} junk cols")

    # Common renames
    rename = {}
    for old, new in [('RELATION','relation'),('ADMISSION','admission'),
                     ('MH','mh'),('LOCATION','location'),('Command','command'),
                     ('Area/ Corps','area_corps'),('ICD NO','icd_no'),
                     ('DIAGNOSIS','diagnosis'),('CATEGORY','category')]:
        if old in df.columns: rename[old] = new
    if rename: df.rename(columns=rename, inplace=True)

    # Year-specific fixes
    if year == 2021:
        df.rename(columns={'ID1':'id1','Sheet1_ID':'sheet1_id',
                          'MIL HOSP_ID':'mil_hosp_id'}, inplace=True)

    elif year == 2022:
        if 'diagnosis_final' in df.columns and 'diagnosis_code1d' in df.columns:
            mask = df['diagnosis_final'].notna() & (df['diagnosis_final'] != df['diagnosis_code1d'])
            if 'icd_cause_code1d' not in df.columns: df['icd_cause_code1d'] = np.nan
            df.loc[mask, 'icd_cause_code1d'] = df.loc[mask, 'diagnosis_final']
            df.drop(columns=['diagnosis_final'], inplace=True)
            log(f"  2022: diagnosis_final diffs -> icd_cause_code1d ({mask.sum():,} rows)")
        df.rename(columns={'ID1':'id1','Sheet1.ID':'sheet1_id',
                          'MIL HOSP.ID':'mil_hosp_id'}, inplace=True)

    elif year == 2023:
        if 'icd_code_final' in df.columns:
            df['diagnosis_code1d'] = df['icd_code_final'].combine_first(
                df.get('diagnosis_code1d', pd.Series(dtype='object')))
            df.drop(columns=['icd_code_final'], inplace=True)
            log(f"  2023: icd_code_final -> diagnosis_code1d")
        for c in ['Sheet1.ID','MIL HOSP.ID']:
            if c in df.columns: df.drop(columns=[c], inplace=True, errors='ignore')
        df.rename(columns={'Sheet1_ID':'sheet1_id','MIL HOSP_ID':'mil_hosp_id'},
                  inplace=True, errors='ignore')

    elif year == 2024:
        df.rename(columns={
            'diagnosis_code':'diagnosis_code1d',
            'sheet1.id':'sheet1_id','mil hosp.id':'mil_hosp_id',
            'area/ corps':'area_corps','icd no':'icd_no',
        }, inplace=True, errors='ignore')

    # Add missing columns as NULL for uniform schema
    all_possible = [
        'id1','sheet1_id','mil_hosp_id','age_month','age_days',
        'marital_status','station','arm_corps','religion','dist_origin',
        'state_origin','records_office','persnl_marital_status',
        'persnl_sex','persnl_unit_desc','admsn_dschrg_flag_d',
        'diagnosis_code1a','icd_cause_code1a','icd_cause_code1d',
        'diagnosis_code2d','nbb','nbb_weight','icd_remarks_a',
    ]
    for col in all_possible:
        if col not in df.columns: df[col] = np.nan

    log(f"  After normalize: {df.shape[0]:,} × {df.shape[1]} cols")
    return df

# ═══════════════════════════════════════════════════════════════
# STEP 2: VALUE STANDARDISATION
# ═══════════════════════════════════════════════════════════════
def apply_map(s, m, name):
    mask = s.isin(m.keys())
    n = mask.sum()
    if n > 0: s = s.replace(m); log(f"    {name}: fixed {n:,}")
    return s

def standardise(df):
    section("VALUE STANDARDISATION")
    for col, m in [('disposal',DISPOSAL_MAP),('category',CATEGORY_MAP),
                   ('admsn_type',ADMSN_TYPE_MAP),('admission',ADMISSION_MAP),
                   ('marital_status',MARITAL_MAP),('religion',RELIGION_MAP),
                   ('rank',RANK_MAP),('nbb',NBB_MAP)]:
        if col in df.columns: df[col] = apply_map(df[col], m, col)
    return df

# ═══════════════════════════════════════════════════════════════
# STEP 3: DATE & LOS
# ═══════════════════════════════════════════════════════════════
def fix_dates_los(df):
    section("DATES & LOS")
    df['admsn_date'] = pd.to_datetime(df['admsn_date'], errors='coerce')
    df['dschrg_date'] = pd.to_datetime(df['dschrg_date'], errors='coerce')

    bad = df['dschrg_date'] < pd.Timestamp('2000-01-01')
    if bad.sum(): df.loc[bad,'dschrg_date'] = pd.NaT; log(f"  Bad dschrg < 2000: {bad.sum():,} -> NaT")
    bad = df['dschrg_date'] > pd.Timestamp('2025-06-01')
    if bad.sum(): df.loc[bad,'dschrg_date'] = pd.NaT; log(f"  Future dschrg: {bad.sum():,} -> NaT")

    if 'days' in df.columns: df.drop(columns=['days'], inplace=True)
    df['los_days'] = (df['dschrg_date'] - df['admsn_date']).dt.days
    neg = df['los_days'] < 0
    if neg.sum(): df.loc[neg,'los_days'] = np.nan; log(f"  Negative LOS: {neg.sum():,} -> NULL")
    log(f"  LOS computed: {df['los_days'].notna().sum():,} valid")
    return df

# ═══════════════════════════════════════════════════════════════
# STEP 4: AGE & NBB FIXES
# ═══════════════════════════════════════════════════════════════
def fix_ages_nbb(df):
    section("AGE & NBB FIXES")
    for c in ['age_year','age_month','age_days','persnl_age_year','service_years']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce')

    if 'age_month' in df.columns:
        roll = df['age_month'] == 12
        if roll.sum():
            df.loc[roll,'age_year'] = df.loc[roll,'age_year'] + 1
            df.loc[roll,'age_month'] = 0
            log(f"  age_month=12 rollover: {roll.sum():,}")

    if 'service_years' in df.columns:
        bad = df['service_years'] > 50
        if bad.sum(): df.loc[bad,'service_years'] = np.nan; log(f"  service_years>50: {bad.sum():,} -> NULL")

    if 'nbb_weight' in df.columns and 'nbb' in df.columns:
        df['nbb_weight'] = pd.to_numeric(df['nbb_weight'], errors='coerce')
        big = (df['nbb_weight'] > 6000) & (df['nbb'] == 'Y')
        if big.sum(): df.loc[big,'nbb_weight'] /= 10; log(f"  nbb_weight>6000: {big.sum():,} / 10")
        zero = (df['nbb_weight'] == 0) & (df['nbb'] == 'Y')
        if zero.sum(): df.loc[zero,'nbb_weight'] = np.nan
        non = df['nbb'] != 'Y'
        df.loc[non & df['nbb_weight'].notna(), 'nbb_weight'] = np.nan
    return df

# ═══════════════════════════════════════════════════════════════
# STEP 5: HOSPITAL METADATA RECOVERY
# ═══════════════════════════════════════════════════════════════
def recover_hospital(df):
    section("HOSPITAL METADATA RECOVERY")
    hosp_cols = ['command','area_corps','mh','location','mil_hosp_id']
    for col in hosp_cols:
        if col not in df.columns: continue
        valid = df[['medical_unit',col]].dropna()
        if len(valid) == 0: continue
        modes = valid.groupby('medical_unit')[col].agg(
            lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else np.nan)
        before = df[col].isna().sum()
        df[col] = df[col].fillna(df['medical_unit'].map(modes))
        recovered = before - df[col].isna().sum()
        if recovered: log(f"  {col}: recovered {recovered:,}")
    return df

# ═══════════════════════════════════════════════════════════════
# STEP 6: ICD CODE FILLING + STANDARD DISEASE NAME
# Uses icd_master_lookup.json (from PDF extraction) to:
#   1. Fill NULL diagnosis_code1d from icd_remarks_d keywords
#   2. Map every ICD code -> standard WHO disease name
# ═══════════════════════════════════════════════════════════════
import json

# Doctor shorthand -> ICD code mapping for common abbreviations
DOCTOR_SHORTHAND = {
    'CAD': 'I25.1', 'HTN': 'I10', 'HT': 'I10', 'DM': 'E11.9',
    'NIDDM': 'E11.9', 'IDDM': 'E10.9', 'TB': 'A15.0', 'PTB': 'A15.0',
    'COPD': 'J44.9', 'ASTHMA': 'J45.9', 'ACS': 'I20.0',
    'AMI': 'I21.9', 'MI': 'I21.9', 'CVA': 'I64', 'STROKE': 'I64',
    'CKD': 'N18.9', 'AKI': 'N17.9', 'UTI': 'N39.0',
    'LRTI': 'J22', 'URTI': 'J06.9', 'AGE': 'A09.9',
    'ANC': 'Z34.9', 'LSCS': 'O82', 'NVD': 'O80',
    'BPH': 'N40', 'HERNIA': 'K40.9', 'APPENDICITIS': 'K35.9',
    'FRACTURE': 'T14.2', 'RTA': 'V89.2', 'DENGUE': 'A90',
    'MALARIA': 'B54', 'TYPHOID': 'A01.0', 'JAUNDICE': 'R17',
    'EPILEPSY': 'G40.9', 'DEPRESSION': 'F32.9', 'ADS': 'F10.2',
    'PNEUMONIA': 'J18.9', 'COVID': 'U07.1', 'CELLULITIS': 'L03.9',
    'CATARACT': 'H26.9', 'CHOLELITHIASIS': 'K80.2',
    'CHOLECYSTITIS': 'K81.0', 'PANCREATITIS': 'K85.9',
    'RENAL CALCULI': 'N20.0', 'KIDNEY STONE': 'N20.0',
    'LUMBAR SPONDYLOSIS': 'M47.8', 'CERVICAL SPONDYLOSIS': 'M47.8',
    'OSTEOARTHRITIS': 'M19.9', 'RHEUMATOID': 'M06.9',
    'HYPERTENSION': 'I10', 'DIABETES': 'E11.9',
    'SICK ATTENDANT': 'Z76.3', 'NBB': 'Z38.0', 'NEW BORN': 'Z38.0',
    'TONSILLITIS': 'J03.9', 'SINUSITIS': 'J32.9',
    'PILES': 'K64.9', 'HAEMORRHOIDS': 'K64.9',
    'INGUINAL HERNIA': 'K40.9', 'HYDROCELE': 'N43.3',
    'VARICOCELE': 'I86.1', 'VARICOSE': 'I83.9',
    'PSORIASIS': 'L40.9', 'ECZEMA': 'L30.9', 'DERMATITIS': 'L30.9',
    'ANXIETY': 'F41.9', 'SCHIZOPHRENIA': 'F20.9',
    'BIPOLAR': 'F31.9', 'OCD': 'F42.9',
    'BRAIN TUMOR': 'C71.9', 'LUNG CANCER': 'C34.9',
    'BREAST CANCER': 'C50.9', 'LEUKEMIA': 'C95.9',
    'ANAEMIA': 'D64.9', 'ANEMIA': 'D64.9',
    'THYROID': 'E07.9', 'HYPOTHYROID': 'E03.9', 'HYPERTHYROID': 'E05.9',
    'GOUT': 'M10.9', 'SLE': 'M32.9',
    'GASTRITIS': 'K29.7', 'PEPTIC ULCER': 'K27.9', 'GERD': 'K21.0',
    'IBS': 'K58.9', 'CIRRHOSIS': 'K74.6',
    'GLAUCOMA': 'H40.9', 'RETINAL': 'H35.9',
    'OTITIS': 'H66.9', 'DNS': 'J34.2',
    'SCIATICA': 'M54.3', 'BACKACHE': 'M54.5', 'BACK PAIN': 'M54.5',
    'KNEE PAIN': 'M25.5', 'JOINT PAIN': 'M25.5',
}

def enrich_icd(df):
    """Fill NULL diagnosis codes + add standard disease name from ICD book."""
    section("ICD CODE ENRICHMENT")

    # Load ICD master lookup
    icd_json_path = Path(__file__).parent.parent / "data" / "icd_master_lookup.json"
    if not icd_json_path.exists():
        log(f"  WARNING: {icd_json_path} not found. Skipping ICD enrichment.")
        df['disease_standard_name'] = None
        return df

    with open(icd_json_path, 'r', encoding='utf-8') as f:
        icd_book = json.load(f)  # {code: {name, parent_code, parent_name, chapter}}
    log(f"  Loaded ICD book: {len(icd_book):,} codes")

    # Build code->name flat map
    code_to_name = {code: info['name'] for code, info in icd_book.items()}

    # ── Step A: Fill NULL diagnosis_code1d from icd_remarks_d ──
    null_mask = df['diagnosis_code1d'].isna() & df['icd_remarks_d'].notna()
    null_count_before = df['diagnosis_code1d'].isna().sum()
    log(f"  diagnosis_code1d NULL: {null_count_before:,} rows")
    log(f"  Rows with NULL code but non-null icd_remarks_d: {null_mask.sum():,}")

    filled = 0
    if null_mask.sum() > 0:
        remarks = df.loc[null_mask, 'icd_remarks_d'].astype(str).str.upper()
        for keyword, icd_code in DOCTOR_SHORTHAND.items():
            # Match keyword as whole word or at start of remark
            kw_match = remarks.str.contains(r'\b' + re.escape(keyword) + r'\b', regex=True, na=False)
            # Only fill rows that are still NULL
            still_null = df.loc[null_mask, 'diagnosis_code1d'].isna()
            to_fill = kw_match & still_null
            n = to_fill.sum()
            if n > 0:
                df.loc[null_mask & to_fill.reindex(df.index, fill_value=False), 'diagnosis_code1d'] = icd_code
                filled += n

    null_count_after = df['diagnosis_code1d'].isna().sum()
    log(f"  Filled {filled:,} codes from icd_remarks_d keywords")
    log(f"  diagnosis_code1d NULL after fill: {null_count_after:,} ({null_count_after/len(df)*100:.1f}%)")

    # ── Step B: Map ICD code -> standard disease name from book ──
    def lookup_name(code):
        if pd.isna(code): return None
        code = str(code).strip()
        # Try exact match first
        if code in code_to_name: return code_to_name[code]
        # Try parent (remove decimal)
        parent = code.split('.')[0]
        if parent in code_to_name: return code_to_name[parent]
        return None

    df['disease_standard_name'] = df['diagnosis_code1d'].apply(lookup_name)
    matched = df['disease_standard_name'].notna().sum()
    log(f"  disease_standard_name: {matched:,} matched ({matched/len(df)*100:.1f}%)")
    log(f"  Sample mappings:")
    sample = df[df['disease_standard_name'].notna()][['diagnosis_code1d','disease_standard_name']].drop_duplicates().head(10)
    for _, row in sample.iterrows():
        log(f"    {row['diagnosis_code1d']:10s} -> {row['disease_standard_name']}")

    return df

# ═══════════════════════════════════════════════════════════════
# STEP 7: FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════
def engineer(df):
    section("FEATURE ENGINEERING")

    # service_branch from and_no
    df['service_branch'] = df['and_no'].astype(str).str.split('/').str[0].str.upper()
    df.loc[~df['service_branch'].isin(['AR','DS','DA','MC']), 'service_branch'] = None
    log(f"  service_branch: {df['service_branch'].value_counts().to_dict()}")

    # age_group
    bins = [-1,0,1,10,18,30,40,50,60,200]
    labels = ['NEONATE','INFANT','CHILD','ADOLESCENT','YOUNG_ADULT',
              'MID_CAREER','SENIOR','PRE_RETIREMENT','ELDERLY']
    df['age_group'] = pd.cut(df['age_year'], bins=bins, labels=labels, right=True)
    df['age_group'] = df['age_group'].astype(str).replace('nan', np.nan)

    # los_category
    def los_cat(d):
        if pd.isna(d): return None
        if d == 0: return 'SAME_DAY'
        if d <= 3: return 'SHORT'
        if d <= 10: return 'MEDIUM'
        if d <= 30: return 'LONG'
        if d <= 90: return 'VERY_LONG'
        return 'CHRONIC'
    df['los_category'] = df['los_days'].apply(los_cat)

    # ICD chapter
    df['icd_chapter'] = df['diagnosis_code1d'].astype(str).str[0].str.upper()
    df.loc[df['diagnosis_code1d'].isna(), 'icd_chapter'] = None
    valid_ch = set(ICD_CHAPTER_NAMES.keys())
    df.loc[~df['icd_chapter'].isin(valid_ch), 'icd_chapter'] = None
    df['icd_chapter_name'] = df['icd_chapter'].map(ICD_CHAPTER_NAMES)

    # Boolean flags
    df['is_death'] = df['disposal'].isin(['DEATH','FOUNDDEAD']).astype(int)
    df.loc[df['disposal'].isna(), 'is_death'] = None
    df['is_self'] = (df['relation'] == 'SELF').astype(int)
    df['is_newborn'] = (df['nbb'] == 'Y').astype(int) if 'nbb' in df.columns else None

    # rank_tier
    df['rank_tier'] = df['rank'].map(RANK_TIER_MAP)
    df.loc[df['rank'].notna() & df['rank_tier'].isna(), 'rank_tier'] = 'OTHER'

    # season
    month = df['admsn_date'].dt.month
    conds = [month.between(4,6), month.between(7,9), month.between(10,11), month.isin([12,1,2,3])]
    df['season'] = np.select(conds, ['SUMMER','MONSOON','AUTUMN','WINTER'], default='')
    df.loc[df['admsn_date'].isna(), 'season'] = None
    df.loc[df['season'] == '', 'season'] = None

    # temporal
    df['admission_year'] = df['admsn_date'].dt.year
    df['admission_month'] = df['admsn_date'].dt.month

    log(f"  Features done. Total cols: {df.shape[1]}")
    return df

# ═══════════════════════════════════════════════════════════════
# STEP 7: EXPORT TO POSTGRESQL
# ═══════════════════════════════════════════════════════════════
def export_pg(df):
    section("POSTGRESQL EXPORT")
    
    # Save CSV first (fast)
    csv_dir = BASE_DIR / "cleaned_data"
    csv_dir.mkdir(exist_ok=True)
    csv_path = csv_dir / "unified_admissions.csv"
    df.to_csv(csv_path, index=False)
    log(f"  CSV saved: {csv_path} ({os.path.getsize(csv_path)/1024/1024:.0f} MB)")
    
    # Use SQLAlchemy for schema creation + small tables only
    engine = create_engine(PG_URL)
    
    # Drop existing tables
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS admissions CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS hospital_lookup CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS icd_lookup CASCADE"))
        conn.commit()
    
    # Create admissions table from first 0 rows (schema only)
    df.head(0).to_sql('admissions', engine, if_exists='replace', index=False)
    log(f"  Schema created for admissions ({len(df.columns)} cols)")
    
    # Bulk load via psycopg2 COPY (100x faster than to_sql)
    import psycopg2
    conn_pg = psycopg2.connect("dbname=military_hospital user=postgres password=postgres host=localhost")
    cur = conn_pg.cursor()
    
    log(f"  COPY loading {len(df):,} rows...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        # Skip header
        next(f)
        cur.copy_expert(
            f"COPY admissions FROM STDIN WITH (FORMAT csv, NULL '', HEADER false)",
            f
        )
    conn_pg.commit()
    
    # Verify
    cur.execute("SELECT COUNT(*) FROM admissions")
    count = cur.fetchone()[0]
    log(f"  admissions: {count:,} rows loaded")
    
    # Hospital lookup (small table - to_sql is fine)
    hosp_cols = [c for c in ['medical_unit','mil_hosp_id','mh','location','command','area_corps'] if c in df.columns]
    hospital = df[hosp_cols].dropna(subset=['medical_unit']).drop_duplicates('medical_unit')
    hospital.to_sql('hospital_lookup', engine, if_exists='replace', index=False)
    log(f"  hospital_lookup: {len(hospital):,} hospitals")
    
    # ICD lookup (small table)
    icd_cols = [c for c in ['diagnosis_code1d','icd_no','diagnosis','icd_chapter','icd_chapter_name','disease_standard_name'] if c in df.columns]
    icd = df[icd_cols].dropna(subset=['diagnosis_code1d']).drop_duplicates('diagnosis_code1d')
    icd.to_sql('icd_lookup', engine, if_exists='replace', index=False)
    log(f"  icd_lookup: {len(icd):,} unique codes")
    
    # Create indexes
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_adm_diag ON admissions(diagnosis_code1d)",
        "CREATE INDEX IF NOT EXISTS idx_adm_year ON admissions(admission_year)",
        "CREATE INDEX IF NOT EXISTS idx_adm_branch ON admissions(service_branch)",
        "CREATE INDEX IF NOT EXISTS idx_adm_type ON admissions(admsn_type)",
        "CREATE INDEX IF NOT EXISTS idx_adm_relation ON admissions(relation)",
        "CREATE INDEX IF NOT EXISTS idx_adm_category ON admissions(category)",
        "CREATE INDEX IF NOT EXISTS idx_adm_command ON admissions(command)",
        "CREATE INDEX IF NOT EXISTS idx_adm_disposal ON admissions(disposal)",
        "CREATE INDEX IF NOT EXISTS idx_adm_disease ON admissions(disease_standard_name)",
    ]:
        cur.execute(idx_sql)
    conn_pg.commit()
    cur.close()
    conn_pg.close()
    log("  Indexes created")
    engine.dispose()

# ═══════════════════════════════════════════════════════════════
# STEP 8: AUDIT
# ═══════════════════════════════════════════════════════════════
def audit(df):
    section("FINAL AUDIT")
    log(f"  Total: {len(df):,} rows × {len(df.columns)} cols")
    for yr, cnt in df['data_year'].value_counts().sort_index().items():
        log(f"    {int(yr)}: {cnt:,}")
    log(f"\n  Columns ({len(df.columns)}):")
    for col in sorted(df.columns):
        null_pct = df[col].isna().mean()*100
        log(f"    {col:35s} null={null_pct:5.1f}%  uniq={df[col].nunique():>8,}")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    log(f"{'='*60}\n  MILITARY HOSPITAL PIPELINE\n  {datetime.now()}\n{'='*60}")

    # Load each year
    dfs = {}
    for year, path in INPUT_FILES.items():
        if not path.exists():
            log(f"  FILE NOT FOUND: {path}"); continue
        dfs[year] = load_year(year, path)

    if not dfs: log("NO FILES!"); return

    # Unify columns across all years
    section("MERGING")
    all_cols = set()
    for d in dfs.values(): all_cols.update(d.columns)
    for year, d in dfs.items():
        missing = all_cols - set(d.columns)
        for c in missing: d[c] = np.nan
        if missing: log(f"  [{year}] Added {len(missing)} NULL cols")

    unified = pd.concat(dfs.values(), ignore_index=True)
    log(f"  Merged: {len(unified):,} rows × {len(unified.columns)} cols")

    # Clean
    unified = standardise(unified)
    unified = fix_dates_los(unified)
    unified = fix_ages_nbb(unified)
    unified = recover_hospital(unified)
    unified = enrich_icd(unified)
    unified = engineer(unified)

    # Audit
    audit(unified)

    # Export
    export_pg(unified)

    # Save audit
    out_dir = BASE_DIR / "cleaned_data"
    out_dir.mkdir(exist_ok=True)
    with open(out_dir / "audit_report.txt", 'w', encoding='utf-8') as f:
        f.write('\n'.join(AUDIT))
    log(f"\n  PIPELINE COMPLETE")
    return unified

if __name__ == '__main__':
    main()
