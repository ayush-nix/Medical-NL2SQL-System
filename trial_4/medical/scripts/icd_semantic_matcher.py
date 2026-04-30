import psycopg2
from psycopg2.extras import execute_batch
import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'dbname': 'military_hospital',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def phase1_autofill_from_existing(conn):
    """
    Find unmapped remarks that actually have an ICD code mapped in OTHER rows,
    and build a deterministic dictionary to fill them safely.
    """
    logger.info("Phase 1: Auto-fill from existing known mappings...")
    cur = conn.cursor()
    
    query = """
        WITH mapped AS (
            SELECT COALESCE(icd_remarks_d, icd_remarks_a) as remark, diagnosis_code1d
            FROM admissions 
            WHERE diagnosis_code1d IS NOT NULL 
              AND (icd_remarks_d IS NOT NULL OR icd_remarks_a IS NOT NULL)
        )
        SELECT remark, diagnosis_code1d, count(*) as cnt
        FROM mapped
        GROUP BY 1, 2
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    remark_to_code = {}
    remark_counts = {}
    for remark, code, cnt in rows:
        remark = remark.strip().upper()
        if remark not in remark_counts or cnt > remark_counts[remark]:
            remark_counts[remark] = cnt
            remark_to_code[remark] = code
            
    logger.info(f"Built direct lookup for {len(remark_to_code)} unique remarks.")
    
    # Test how many unmapped rows this solves
    cur.execute("""
        SELECT id1, COALESCE(icd_remarks_d, icd_remarks_a) as remark
        FROM admissions
        WHERE diagnosis_code1d IS NULL 
          AND (icd_remarks_d IS NOT NULL OR icd_remarks_a IS NOT NULL)
    """)
    unmapped_rows = cur.fetchall()
    
    matches = 0
    update_data = []
    for rid, remark in unmapped_rows:
        remark_clean = remark.strip().upper()
        if remark_clean in remark_to_code:
            update_data.append((remark_to_code[remark_clean], rid))
            matches += 1
            
    logger.info(f"Direct mapping solves {matches} out of {len(unmapped_rows)} missing rows!")
    
    if update_data:
        logger.info("Applying direct mapping updates to DB...")
        cur.execute("SELECT diagnosis_code1d, disease_standard_name FROM icd_lookup")
        lookup_map = {row[0]: row[1] for row in cur.fetchall()}
        
        full_update_data = []
        for code, rid in update_data:
            std_name = lookup_map.get(code)
            full_update_data.append((code, std_name, rid))
            
        update_query = """
            UPDATE admissions 
            SET diagnosis_code1d = %s, disease_standard_name = %s
            WHERE id1 = %s
        """
        execute_batch(cur, update_query, full_update_data, page_size=10000)
        conn.commit()
        logger.info("Phase 1 update complete.")
        
    return matches

def build_faiss_index(conn):
    """Embed all WHO standard disease names into FAISS."""
    from sentence_transformers import SentenceTransformer
    import faiss
    
    logger.info("Loading sentence-transformers model (all-MiniLM-L6-v2)...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    cur = conn.cursor()
    cur.execute("SELECT diagnosis_code1d, disease_standard_name FROM icd_lookup WHERE disease_standard_name IS NOT NULL")
    rows = cur.fetchall()
    
    icd_codes = []
    disease_names = []
    for code, name in rows:
        icd_codes.append(code)
        disease_names.append(name)
        
    logger.info(f"Embedding {len(disease_names)} standard diseases...")
    embeddings = model.encode(disease_names, show_progress_bar=True)
    
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)  # Inner Product (Cosine similarity if normalized)
    faiss.normalize_L2(embeddings)
    index.add(embeddings)
    
    logger.info("FAISS index built successfully.")
    return model, index, icd_codes, disease_names

def phase2_semantic_matching(conn, model, index, icd_codes, disease_names):
    """Expand shortforms using LLM and match against FAISS index."""
    import faiss
    
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(icd_remarks_d, icd_remarks_a) as remark, count(*) as cnt
        FROM admissions
        WHERE diagnosis_code1d IS NULL 
          AND (icd_remarks_d IS NOT NULL OR icd_remarks_a IS NOT NULL)
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    unmapped_groups = cur.fetchall()
    
    if not unmapped_groups:
        logger.info("No more unmapped rows remaining!")
        return
        
    logger.info(f"Remaining unique unmapped remarks: {len(unmapped_groups)}")
    
    # Process the top 1000 most frequent remarks
    top_groups = unmapped_groups[:1000]
    total_rows_covered = sum(cnt for _, cnt in top_groups)
    logger.info(f"Processing Top 1000 remarks (covers {total_rows_covered} rows)...")
    
    update_data = []
    batch_size = 50
    for i in range(0, len(top_groups), batch_size):
        batch = top_groups[i:i+batch_size]
        remarks = [r[0].strip().upper() for r in batch]
        
        # 1. LLM Expansion
        prompt = "You are a medical expert. Convert the following list of doctor shorthand remarks into their full, standard medical disease names. Only output the full name, one per line. If a remark is too vague (e.g., 'SICK ATTENDANT', 'PME', 'NONE'), output 'VAGUE'.\n\n"
        for idx, r in enumerate(remarks):
            prompt += f"{idx+1}. {r}\n"
            
        try:
            response = requests.post("http://localhost:11434/api/generate", json={
                "model": "llama3.1:8b",
                "prompt": prompt,
                "stream": False,
                "temperature": 0.0
            })
            
            expanded_text = response.json().get('response', '')
            expanded_lines = [line.split('. ', 1)[-1].strip() for line in expanded_text.strip().split('\n') if line.strip()]
            if len(expanded_lines) != len(remarks):
                expanded_lines = remarks
                
        except Exception as e:
            logger.error(f"LLM error: {e}")
            expanded_lines = remarks
            
        # 2. Vector Search
        emb = model.encode(expanded_lines)
        faiss.normalize_L2(emb)
        k = 1
        distances, indices = index.search(emb, k)
        
        # 3. Apply Threshold and Prepare Update
        for j, (orig_remark, cnt) in enumerate(batch):
            score = distances[j][0]
            idx = indices[j][0]
            expanded = expanded_lines[j]
            
            if expanded.upper() == 'VAGUE' or score < 0.70:
                continue # Leave as NULL
                
            matched_code = icd_codes[idx]
            matched_std = disease_names[idx]
            
            logger.info(f"[{score:.2f}] {orig_remark} -> {expanded} -> {matched_std} ({matched_code})")
            update_data.append((matched_code, matched_std, orig_remark))
            
    if update_data:
        logger.info(f"Applying {len(update_data)} semantic mappings to DB...")
        update_query = """
            UPDATE admissions 
            SET diagnosis_code1d = %s, disease_standard_name = %s
            WHERE diagnosis_code1d IS NULL 
              AND COALESCE(icd_remarks_d, icd_remarks_a) = %s
        """
        execute_batch(cur, update_query, update_data, page_size=1000)
        conn.commit()
        logger.info("Semantic update complete.")

def main():
    conn = get_db_connection()
    try:
        # Phase 1: Direct Mapping
        phase1_autofill_from_existing(conn)
        
        # Phase 2: Vector Search & LLM Expansion
        model, index, icd_codes, disease_names = build_faiss_index(conn)
        phase2_semantic_matching(conn, model, index, icd_codes, disease_names)
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
