from fastapi import FastAPI, UploadFile, File
import pandas as pd
import requests
import json
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

# ================= DATABASE =================
DATABASE_URL = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

API_URL = os.getenv("API_URL")
TOKEN = os.getenv("DVARA_TOKEN")

# ================= CONTROLLED LISTS =================
LOAN_PURPOSES = [
    "education", "home renovation", "car",
    "business", "personal", "medical"
]

EMPLOYMENT_TYPES = [
    "salaried", "self employed", "unemployed"
]

# =========================================================
# CREATE TABLE
# =========================================================
def create_table():
    query = """
    CREATE TABLE IF NOT EXISTS loan_applicants (
        applicant_id VARCHAR(50) PRIMARY KEY,
        applicant_name VARCHAR(255),
        phone_number VARCHAR(20),
        email VARCHAR(255),
        aadhaar_number VARCHAR(20),
        pan_number VARCHAR(20),
        loan_amount DECIMAL(12,2),
        loan_purpose VARCHAR(255),
        employment_type VARCHAR(100),
        monthly_income DECIMAL(12,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()

# =========================================================
# VALIDATORS
# =========================================================
def valid_id(v):
    return bool(re.match(r"^A\d+$", str(v).strip()))

def valid_email(v):
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$", str(v).strip()))

def valid_phone(v):
    s = str(v).strip()
    return s.isdigit() and len(s) == 10 and s[0] in "6789"

def valid_aadhaar(v):
    s = str(v).strip()
    return s.isdigit() and len(s) == 12

def valid_pan(v):
    return bool(re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", str(v).strip().upper()))

def valid_loan_amount(v):
    try:
        return 500000 <= int(str(v).strip()) <= 10000000
    except:
        return False

def valid_monthly_income(v):
    try:
        return 25000 <= int(str(v).strip()) <= 1000000
    except:
        return False

def valid_name(v):
    parts = str(v).strip().split()
    return (
        len(parts) >= 2
        and bool(re.match(r"^[A-Za-z ]+$", str(v).strip()))
        and all(len(p) >= 2 for p in parts)
    )

# =========================================================
# HELPERS
# =========================================================
def normalize_number(v):
    try:
        if "E+" in str(v) or "e+" in str(v):
            return str(int(float(v)))
        return v
    except:
        return v

def is_null(v):
    return str(v).strip() in ("nan", "None", "NaT", "none", "null", "")

# =========================================================
# GET NEXT AVAILABLE ID (collision-safe across batch)
# =========================================================
_used_ids = set()

def next_id():
    """Return next available applicant_id not in DB and not used in current batch."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT applicant_id FROM loan_applicants")).fetchall()
            db_ids = {int(r[0][1:]) for r in rows if re.match(r"^A[0-9]+$", r[0])}
    except Exception:
        db_ids = set()
    all_used = db_ids | _used_ids
    # Start above the highest known ID to avoid gaps being filled with unexpected values
    start = max(all_used) + 1 if all_used else 101
    n = start
    while n in all_used:
        n += 1
    _used_ids.add(n)
    return f"A{n}"

# =========================================================
# LLM FIELD MAPPING
# =========================================================
def call_llm(cols, fields, rows):
    task = {
        "excel_columns": cols,
        "database_fields": fields,
        "data_rows": rows
    }

    r = requests.post(
        API_URL,
        headers={"Authorization": f"Bearer {TOKEN}"},
        data={"task": json.dumps(task)},
        timeout=60
    )

    raw = r.json()

    # DEBUG: see actual response structure
    print("LLM RAW RESPONSE:", raw)

    mp = {}

    # Case 1: nested result.result
    if "result" in raw:
        res = raw["result"]

        if isinstance(res, dict):
            # sometimes mapping is directly inside
            if "mapping" in res:
                mp = res["mapping"]

            # most common: result inside result
            elif "result" in res:
                mp = res["result"]

    # Case 2: mapping at root
    if "mapping" in raw:
        mp = raw["mapping"]

    # If mapping is string → parse JSON
    if isinstance(mp, str):
        try:
            mp = json.loads(mp)
        except:
            mp = {}

    # Ensure dict
    if not isinstance(mp, dict):
        mp = {}

    return {k: v for k, v in mp.items() if k != "is_valid"}

# =========================================================
# FIELD-LEVEL VALIDATORS — used in both invalidation & repair
# Maps each DB field to its validator function
# =========================================================
FIELD_VALIDATORS = {
    "applicant_id":   valid_id,
    "applicant_name": valid_name,
    "phone_number":   valid_phone,
    "email":          valid_email,
    "aadhaar_number": valid_aadhaar,
    "pan_number":     valid_pan,
    "loan_amount":    valid_loan_amount,
    "loan_purpose":   lambda v: str(v).strip().lower() in LOAN_PURPOSES,
    "employment_type":lambda v: str(v).strip().lower() in EMPLOYMENT_TYPES,
    "monthly_income": valid_monthly_income,
}

# =========================================================
# PASS 1 — INVALIDATION
# Wipe cells that provably fail their field's format.
# Numeric fields (loan_amount, monthly_income) are NOT wiped here
# because their ranges overlap — we handle them via column-trust below.
# =========================================================
def invalidate(df):
    skip = {"loan_amount", "monthly_income"}  # handled by column-trust
    for i in df.index:
        for field, validator in FIELD_VALIDATORS.items():
            if field in skip:
                continue
            val = str(df.at[i, field]).strip()
            if is_null(val) or not validator(val):
                df.at[i, field] = None
    return df

# =========================================================
# PASS 2 — REPAIR
#
# Strategy:
#   - COLUMN-TRUSTED fields: loan_amount, monthly_income
#     The LLM correctly identified WHICH column is which.
#     We trust the column position and just validate the value.
#     If the value in the loan column is a valid number → keep it.
#     We do NOT try to infer loan vs income from value alone.
#
#   - FORMAT-DETECTED fields: everything else
#     Scan all raw values and assign by format (priority order).
#     These are unambiguous by format (email, phone, PAN, etc.)
#     and the LLM often gets the column wrong for messy data.
# =========================================================
def repair(df, original_df, col_map):
    field_to_excel = {v: k for k, v in col_map.items()}

    # Prevent ID collision
    for aid in df["applicant_id"].dropna():
        if valid_id(str(aid).strip()):
            _used_ids.add(int(str(aid).strip()[1:]))

    for i in df.index:
        orig_row = original_df.loc[i]

        raw_vals = []

        # -------------------------
        # STEP 1 — Normalize values
        # -------------------------
        for v in orig_row.values:
            if is_null(v):
                continue

            val = str(v).strip()

            # Fix scientific notation (Aadhaar etc.)
            try:
                if "e+" in val.lower():
                    val = str(int(float(val)))
            except:
                pass

            raw_vals.append(val)

        # -------------------------
        # STEP 2 — Classification
        # -------------------------
        bucket = {
            "id": [],
            "email": [],
            "phone": [],
            "aadhaar": [],
            "pan": [],
            "name": [],
            "employment": [],
            "purpose": [],
            "numeric": []
        }

        for v in raw_vals:
            lv = v.lower()

            if valid_id(v):
                bucket["id"].append(v)
                continue

            if valid_email(v):
                bucket["email"].append(v)
                continue

            if valid_pan(v):
                bucket["pan"].append(v.upper())
                continue

            if valid_aadhaar(v):
                bucket["aadhaar"].append(v)
                continue

            if valid_phone(v):
                bucket["phone"].append(v)
                continue

            if lv in EMPLOYMENT_TYPES:
                bucket["employment"].append(lv.title())
                continue

            if lv in LOAN_PURPOSES:
                bucket["purpose"].append(lv.title())
                continue

            # IMPORTANT: only pure numeric AND not phone
            if v.isdigit():
                bucket["numeric"].append(int(v))
                continue

            if len(v) >= 3 and not any(c.isdigit() for c in v):
                bucket["name"].append(v.title())

        # -------------------------
        # STEP 3 — Assign fields
        # -------------------------
        df.at[i, "applicant_id"] = bucket["id"][0] if bucket["id"] else next_id()
        _used_ids.add(int(df.at[i, "applicant_id"][1:]))

        if bucket["email"]:
            df.at[i, "email"] = bucket["email"][0]

        if bucket["pan"]:
            df.at[i, "pan_number"] = bucket["pan"][0]

        if bucket["aadhaar"]:
            df.at[i, "aadhaar_number"] = bucket["aadhaar"][0]

        if bucket["phone"]:
            df.at[i, "phone_number"] = bucket["phone"][0]

        if bucket["employment"]:
            df.at[i, "employment_type"] = bucket["employment"][0]

        if bucket["purpose"]:
            df.at[i, "loan_purpose"] = bucket["purpose"][0]

        if bucket["name"]:
            df.at[i, "applicant_name"] = bucket["name"][0]

        # -------------------------
        # STEP 4 — Numeric split rule
        # Income < 5L
        # Loan > 5L
        # -------------------------
        nums = sorted(bucket["numeric"])

        loan = None
        income = None

        for n in nums:
            # skip values that look like phone numbers
            if valid_phone(str(n)):
                continue

            if n < 500000 and not income:
                income = n
            elif n > 500000 and not loan:
                loan = n

        if income:
            df.at[i, "monthly_income"] = str(income)

        if loan:
            df.at[i, "loan_amount"] = str(loan)

    return df
# =========================================================
# UPSERT
# =========================================================
def upsert(df):
    ins, upd = 0, 0
    with engine.begin() as conn:
        for _, r in df.iterrows():
            d = {k: (None if is_null(str(v)) else v) for k, v in r.to_dict().items()}

            exists = conn.execute(
                text("SELECT COUNT(*) FROM loan_applicants WHERE applicant_id=:id"),
                {"id": d["applicant_id"]}
            ).scalar()

            if exists:
                conn.execute(text("""
                UPDATE loan_applicants SET
                  applicant_name=:applicant_name, phone_number=:phone_number,
                  email=:email, aadhaar_number=:aadhaar_number,
                  pan_number=:pan_number, loan_amount=:loan_amount,
                  loan_purpose=:loan_purpose, employment_type=:employment_type,
                  monthly_income=:monthly_income
                WHERE applicant_id=:applicant_id
                """), d)
                upd += 1
            else:
                conn.execute(text("""
INSERT INTO loan_applicants (
  applicant_id, applicant_name, phone_number, email,
  aadhaar_number, pan_number, loan_amount,
  loan_purpose, employment_type, monthly_income, created_at
) VALUES (
  :applicant_id, :applicant_name, :phone_number, :email,
  :aadhaar_number, :pan_number, :loan_amount,
  :loan_purpose, :employment_type, :monthly_income, NOW()
)
"""), d)
                ins += 1
    return ins, upd

# =========================================================
# ENSURE COLUMNS
# =========================================================
def ensure_columns(df):
    required = [
        "applicant_id", "applicant_name", "phone_number", "email",
        "aadhaar_number", "pan_number", "loan_amount",
        "loan_purpose", "employment_type", "monthly_income"
    ]
    for col in required:
        if col not in df.columns:
            df[col] = None
    return df[required]

# =========================================================
# VALIDATE ENDPOINT
# =========================================================
@app.post("/validate/")
async def validate(file: UploadFile = File(...)):
    _used_ids.clear()

    original_df = pd.read_excel(file.file, dtype=str)
    original_df.reset_index(drop=True, inplace=True)

    create_table()

    fields = [
        "applicant_id", "applicant_name", "phone_number", "email",
        "aadhaar_number", "pan_number", "loan_amount", "loan_purpose",
        "employment_type", "monthly_income"
    ]

    mp = call_llm(original_df.columns.tolist(), fields, original_df.to_dict("records"))

    df = original_df.copy()
    df.rename(columns=mp, inplace=True)
    df = ensure_columns(df)
    df = repair(df, original_df, mp)

    return {
        "status": "validated",
        "mapping": mp,
        "preview": df.head(20).fillna("").to_dict("records")
    }

# =========================================================
# UPLOAD ENDPOINT
# =========================================================
@app.post("/upload/")
async def upload(file: UploadFile = File(...)):
    _used_ids.clear()

    original_df = pd.read_excel(file.file, dtype=str)
    original_df.reset_index(drop=True, inplace=True)

    create_table()

    fields = [
        "applicant_id", "applicant_name", "phone_number", "email",
        "aadhaar_number", "pan_number", "loan_amount", "loan_purpose",
        "employment_type", "monthly_income"
    ]

    mp = call_llm(original_df.columns.tolist(), fields, original_df.to_dict("records"))

    df = original_df.copy()
    df.rename(columns=mp, inplace=True)
    df = ensure_columns(df)
    df = repair(df, original_df, mp)

    ins, upd = upsert(df)

    return {
        "status": "success",
        "inserted": ins,
        "updated": upd
    }

# =========================================================
# ROOT
# =========================================================
@app.get("/")
def root():
    return {"msg": "Loan Applicant AI Ingestion System"}