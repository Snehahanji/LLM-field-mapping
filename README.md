# 📊 LLM-Based Excel to Database Field Mapper API

A FastAPI backend service that:

- 📂 Uploads Excel files  
- 🤖 Uses an external LLM API to automatically map Excel columns to database fields  
- 🗄️ Inserts mapped data into a MySQL database (optional)  
- 🔄 Dynamically creates tables if they don’t exist  

---

## 🚀 Features

- Upload Excel file via API
- Automatically detect Excel column names
- Fetch actual database table fields
- Call external LLM API for intelligent column mapping
- Rename columns dynamically
- Optional database insertion
- Health check endpoint
- Auto table creation

---

## 🛠️ Tech Stack

- FastAPI  
- Pandas  
- MySQL  
- SQLAlchemy  
- Requests  
- Python-dotenv  
- External LLM API  

---

## 📁 Project Structure

```
project/
│
├── backend.py
├── .env
├── requirements.txt
└── README.md
```

---

## ⚙️ Environment Variables

Create a `.env` file in the root directory:

```
DB_USER=your_mysql_username
DB_PASSWORD=your_mysql_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database_name

API_URL=https://your-llm-api-url
DVARA_TOKEN=your_api_token
```

---

## 📦 Installation

### 1️⃣ Clone the repository

```
git clone <your-repo-url>
cd <project-folder>
```

### 2️⃣ Create virtual environment

```
python -m venv venv
```

Activate virtual environment:

**Windows:**
```
venv\Scripts\activate
```

**Mac/Linux:**
```
source venv/bin/activate
```

### 3️⃣ Install dependencies

```
pip install fastapi uvicorn pandas sqlalchemy pymysql python-dotenv requests openpyxl
```

---

## ▶️ Running the Application

```
uvicorn backend:app --reload
```

Application will run at:

```
http://127.0.0.1:8000
```

Swagger Documentation:

```
http://127.0.0.1:8000/docs
```

---

## 📚 API Endpoints

### 🏠 Root Endpoint

```
GET /
```

Returns API status and available endpoints.

---

### 📤 Upload Excel File

```
POST /upload/
```

### Query Parameters:

| Parameter     | Type    | Default      | Description |
|--------------|---------|-------------|-------------|
| table_name   | string  | llm_mapping | Target database table |
| insert_to_db | boolean | false       | Whether to insert into database |

### Example:

```
POST /upload/?table_name=customers&insert_to_db=true
```

Upload the Excel file using Swagger UI.

---

### ❤️ Health Check

```
GET /health
```

Checks:

- Database connectivity  
- Token availability  

---

## 🔄 How It Works

### Step 1 – Upload Excel
Pandas reads the file:

```
df = pd.read_excel(file.file)
```

### Step 2 – Get Database Fields
Table is created if it doesn't exist.

```
DESCRIBE table_name;
```

### Step 3 – Call LLM API
Excel columns and DB fields are sent as form-data:

```
{
  "excel_columns": ["Name", "Email", "Phone"],
  "database_fields": ["customer_id", "full_name", "email_address", "mobile_number"]
}
```

### Step 4 – Receive Mapping

Example response from LLM:

```
{
  "Name": "full_name",
  "Email": "email_address",
  "Phone": "mobile_number"
}
```

### Step 5 – Rename Columns

```
df.rename(columns=mapping, inplace=True)
```

### Step 6 – Optional Insert to Database

```
df.to_sql(table_name, engine, if_exists='append', index=False)
```

---

## 📊 Example API Response

```
{
  "status": "success",
  "original_columns": ["Name", "Email", "Phone"],
  "database_fields": ["customer_id", "full_name", "email_address", "mobile_number"],
  "mapping": {
    "Name": "full_name",
    "Email": "email_address",
    "Phone": "mobile_number"
  },
  "renamed_columns": ["full_name", "email_address", "mobile_number"],
  "total_rows": 50,
  "rows_inserted": 50
}
```
## 📊 Database Result

<p align="center">
  <img src="images/mysql_result.png" width="800">
</p>

---

## 🛡️ Error Handling

- 403 → Token expired  
- 500 → LLM API failure  
- 500 → Database insert failure  
- Invalid JSON handled gracefully  

---

## 🔮 Future Improvements

- Support CSV files  
- Add authentication  
- Add logging system  
- Add Docker support  
- Add unit tests  

---

## 👩‍💻 Author

Sneha Hanji  
FastAPI + LLM Integration Project
