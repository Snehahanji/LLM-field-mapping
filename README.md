# 🏦 Loan Applicant AI Ingestion System

> **Intelligent Excel → Database pipeline** powered by LLM field mapping, smart data repair, and a beautiful Streamlit dashboard.

---

## ✨ What It Does

Upload any messy Excel file of loan applicant data — even with misnamed columns, scrambled fields, or inconsistent formats — and this system will:

1. 🧠 **AI-map** your Excel columns to the correct database fields using an LLM
2. 🔧 **Auto-repair** invalid or misplaced values (phone numbers in wrong columns, scientific-notation Aadhaar numbers, etc.)
3. 👁️ **Preview** a before/after comparison before committing anything
4. 💾 **Upsert** clean, validated records into MySQL
5. 📥 **Download** the cleaned Excel file for your records

---

## 🖼️ Screenshots & Dashboard Preview

### 🏠 Main Dashboard
> Upload your Excel file and choose to validate or push directly to the database.

![Main Dashboard](01_main_dashboard.png)
*The main Streamlit UI — drag-and-drop Excel upload with Validate and Upload buttons.*

---

### 🗺️ LLM Field Mapping
> The AI reads your column names and maps them to the correct database fields automatically.

![Field Mapping](02_field_mapping.png)
*LLM-generated column mapping table — Excel column names on the left, DB fields on the right.*

---

### 📊 Mapping Confidence Meter
> A confidence score shows how reliably the LLM matched your columns.

![Confidence Meter](03_confidence_meter.png)
*Green = reliable mapping (≥70%). Yellow = review recommended (<70%).*

---

### 🔁 Before vs. After Data Preview
> Side-by-side view of raw uploaded data vs. the cleaned, repaired output.

![Before After Preview](03_confidence_meter.png)
*Left: original messy Excel. Right: AI-repaired, validated data ready for the database.*

---

### ✅ Validation Complete
> After validation, a success banner appears with all insights.

![Validation Success](03_confidence_meter.png)
*Success state showing mapping table, confidence score, before/after preview, and download button.*

---

### 🚀 Upload & Insert to Database
> After confirming the preview, upload to MySQL with a single click.

![Upload Success](04_upload_success.png)
*Upload result showing number of newly inserted records and updated records.*

---

### 🩺 API Health Check
> Live status of the FastAPI backend shown at the bottom of the dashboard.

![API Health](05_api_health.png)
*FastAPI health endpoint response rendered inline in the Streamlit app.*

---

> 📸 **To add real screenshots:** Run the app, take screenshots of each view, and save them to `docs/screenshots/` with the filenames shown above.

---

| Feature | Description |
|--------|-------------|
| 🗺️ Field Mapping View | See exactly how the LLM mapped each Excel column |
| 📊 Confidence Meter | Visual score for mapping reliability |
| 🔁 Before/After Preview | Side-by-side comparison of raw vs. cleaned data |
| ⚠️ Unmapped Column Warnings | Catch columns that were ignored |
| ⬇️ Download Cleaned File | Export repaired data as `.xlsx` |
| 🩺 API Health Check | Live FastAPI status in the UI |

---

## 🏗️ Architecture

```
Excel Upload
     │
     ▼
┌─────────────┐     ┌──────────────────┐
│  Streamlit  │────▶│  FastAPI Backend │
│  Dashboard  │     └────────┬─────────┘
└─────────────┘              │
                    ┌────────▼─────────┐
                    │   LLM (Column    │
                    │   Field Mapper)  │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  Invalidation +  │
                    │  Repair Engine   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │   MySQL Database │
                    │  (Upsert Logic)  │
                    └──────────────────┘
```

---

## 📋 Database Schema

| Field | Type | Validation |
|-------|------|-----------|
| `applicant_id` | VARCHAR(50) | Format: `A<number>` |
| `applicant_name` | VARCHAR(255) | ≥ 2 parts, letters only |
| `phone_number` | VARCHAR(20) | 10-digit, starts with 6-9 |
| `email` | VARCHAR(255) | Standard email format |
| `aadhaar_number` | VARCHAR(20) | 12-digit numeric |
| `pan_number` | VARCHAR(20) | `AAAAA9999A` format |
| `loan_amount` | DECIMAL(12,2) | ₹1,000 – ₹1,00,00,000 |
| `loan_purpose` | VARCHAR(255) | Controlled list |
| `employment_type` | VARCHAR(100) | Controlled list |
| `monthly_income` | DECIMAL(12,2) | ₹1,000 – ₹1,00,00,000 |

---

## 🎯 Supported Controlled Values

**Loan Purposes:** `education` · `home renovation` · `car` · `business` · `personal` · `medical`

**Employment Types:** `salaried` · `self employed` · `unemployed`

---

## 🚀 Getting Started

### 1. Clone & Install

```bash
git clone https://github.com/your-org/loan-ai-ingestion.git
cd loan-ai-ingestion
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=loan_db

API_URL=https://your-llm-api-endpoint/v1/chat
DVARA_TOKEN=your_api_token_here
```

### 3. Start the Backend

```bash
uvicorn main:app --reload --port 8000
```

### 4. Launch the Dashboard

```bash
streamlit run app.py
```

Then open **http://localhost:8501** in your browser.

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `POST` | `/validate/` | Preview mapped & repaired data (no DB write) |
| `POST` | `/upload/` | Map, repair, and upsert to database |

### Example: Validate via cURL

```bash
curl -X POST "http://localhost:8000/validate/" \
  -F "file=@applicants.xlsx"
```

### Example Response

```json
{
  "status": "validated",
  "mapping": {
    "Full Name": "applicant_name",
    "Mob No": "phone_number",
    ...
  },
  "preview": [...]
}
```

---

## 🧠 How the Repair Engine Works

The system uses a **two-pass strategy** to maximize data quality:

### Pass 1 — Invalidation
Scans every mapped cell against its field's validator. Invalid values are wiped to `NULL`, ready for repair.

### Pass 2 — Repair (Format Detection)
Each row's raw values are classified into buckets by format:

| Bucket | Detection Rule |
|--------|---------------|
| 📧 Email | `@` + domain pattern |
| 📱 Phone | 10-digit, starts with 6–9 |
| 🆔 Aadhaar | 12-digit numeric |
| 🪪 PAN | `AAAAA9999A` regex |
| 🏦 Loan Amount | Numeric > ₹5,00,000 |
| 💰 Monthly Income | Numeric < ₹5,00,000 |
| 👤 Name | ≥ 2 alphabetic parts |

This handles common real-world problems like columns being swapped, extra whitespace, and scientific notation in numeric IDs.

---

## 📁 Project Structure

```
loan-ai-ingestion/
├── main.py          # FastAPI backend (mapping, repair, upsert)
├── app.py           # Streamlit frontend dashboard
├── .env             # Environment variables (not committed)
├── requirements.txt # Python dependencies
└── README.md
```

---

## 📦 Requirements

```txt
fastapi
uvicorn
streamlit
pandas
openpyxl
sqlalchemy
pymysql
python-dotenv
requests
```

---

## 🛡️ Data Quality Guarantees

- ✅ No duplicate IDs — collision-safe ID generation across batch and DB
- ✅ Upsert logic — re-uploading the same file updates, not duplicates
- ✅ Scientific notation normalization (e.g., `1.23E+11` → `123000000000`)
- ✅ LLM mapping + format-based fallback for maximum accuracy

---

## 📸 Adding Screenshots to This README

To populate the screenshot placeholders with real images:

```bash
# Create the screenshots folder
mkdir -p docs/screenshots
```

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<p align="center">Built with ❤️ using FastAPI · Streamlit · MySQL · LLM Intelligence</p>
