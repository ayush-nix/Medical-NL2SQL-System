"""
Extract all 12,500+ ICD-10 codes from the PDF into a JSON lookup.
Format: {icd_code: standard_disease_name}
"""
import re, json, sys
try:
    import PyPDF2
except:
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'PyPDF2', '-q'])
    import PyPDF2

PDF_PATH = r"C:\Users\anant\Downloads\NL2SQL-System-main\NL2SQL-System-main\ICD CODE & ICD DIAG _ REVISED LIST.pdf"
OUT_PATH = r"C:\Users\anant\Downloads\NL2SQL-System-main\NL2SQL-System-main\trial_4\medical\data\icd_master_lookup.json"

# Pattern: "123 A00.0 Disease name here"
LINE_RE = re.compile(r'^\s*(\d+)\s+([A-Z]\d{2}(?:\.\d{1,2})?)\s+(.+?)(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})?$')

entries = {}
with open(PDF_PATH, 'rb') as f:
    reader = PyPDF2.PdfReader(f)
    print(f"Pages: {len(reader.pages)}")
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        if not text: continue
        for line in text.split('\n'):
            line = line.strip()
            m = LINE_RE.match(line)
            if m:
                code = m.group(2).strip()
                name = m.group(3).strip()
                # Clean trailing IP/timestamp artifacts
                name = re.sub(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*$', '', name).strip()
                name = re.sub(r'\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2}$', '', name).strip()
                if code and name and len(name) > 2:
                    entries[code] = name

print(f"Extracted {len(entries)} ICD codes")
print(f"Sample: {dict(list(entries.items())[:10])}")

# Add ICD chapter/block info
icd_blocks = {}
for code, name in entries.items():
    ch = code[0]
    # Codes without dots are block headers (e.g., "A00" = block header for A00.x)
    if '.' not in code:
        icd_blocks[code] = name

# Build enriched lookup
lookup = {}
for code, name in entries.items():
    ch = code[0]
    # Find parent block (code without dot)
    parent = code.split('.')[0]
    parent_name = icd_blocks.get(parent, "")
    lookup[code] = {
        "name": name,
        "parent_code": parent,
        "parent_name": parent_name,
        "chapter": ch,
    }

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    json.dump(lookup, f, indent=2, ensure_ascii=False)

print(f"Saved to {OUT_PATH}")
print(f"Chapters covered: {sorted(set(c[0] for c in entries.keys()))}")
