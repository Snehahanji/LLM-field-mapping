# 🚀 Loan Applicant Field Mapping API (LLM Powered)

This project is a **FastAPI backend service** that automatically maps Excel fields to database fields using an LLM API and optionally inserts the mapped data into a MySQL database.

---

## 📌 Features

* Upload Excel files
* Automatically detect column names
* Fetch database schema dynamically
* Use LLM to generate intelligent field mapping
* Rename columns automatically
* Insert mapped data into MySQL
* Preview processed data
* Health check endpoint

---

## 🛠️ Tech Stack

* **Backend:** FastAPI
* **Database:** MySQL
* **ORM:** SQLAlchemy
* **Data Processing:** Pandas
* **LLM Integration:** REST API
* **Environment Config:** python-dotenv

---

## 📂 Project Structure

```
project/
│── backend.py
│── requirements.txt
│── .env
│── README.md
```

---

## ⚙️ Environment Variables (.env)

Create a `.env` file in your root folder and add the following:

### 🗄️ Database Configuration

```
DB_USER=root
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=loan_db
```

---

### 🤖 LLM API Configuration

```
API_URL=https://your-llm-api-endpoint
DVARA_TOKEN=your_api_token_here
```

---

## 🗄️ Database Table Schema

The system automatically creates a table called:

### 📌 `loan_applicants`

### Fields:

| Field Name      | Type          | Description           |
| --------------- | ------------- | --------------------- |
| applicant_id    | VARCHAR(50)   | Primary key ID        |
| applicant_name  | VARCHAR(255)  | Applicant full name   |
| phone_number    | VARCHAR(20)   | Mobile number         |
| email           | VARCHAR(255)  | Email address         |
| aadhaar_number  | VARCHAR(20)   | Aadhaar ID            |
| pan_number      | VARCHAR(20)   | PAN card number       |
| loan_amount     | DECIMAL(12,2) | Requested loan amount |
| loan_purpose    | VARCHAR(255)  | Purpose of loan       |
| employment_type | VARCHAR(100)  | Job type              |
| monthly_income  | DECIMAL(12,2) | Income                |
| loan_status     | VARCHAR(50)   | Default = PENDING     |
| created_at      | TIMESTAMP     | Auto timestamp        |

---

## 📦 Installation

### 1️⃣ Clone Repository

```
git clone https://github.com/yourusername/loan-mapping-api.git
cd loan-mapping-api
```

---

### 2️⃣ Install Dependencies

```
pip install -r requirements.txt
```

---

### 3️⃣ Run Server

```
uvicorn backend:app --reload
```

Server URL:

```
http://127.0.0.1:8000
```

---

## 📡 API Endpoints

---

### 🟢 Upload Excel & Auto Map Fields

**POST** `/upload/`

#### Request Parameters

| Parameter    | Type    | Required | Description               |
| ------------ | ------- | -------- | ------------------------- |
| file         | File    | Yes      | Excel file                |
| table_name   | String  | No       | Default = loan_applicants |
| insert_to_db | Boolean | No       | Insert data into DB       |

---

### 🟢 Example (Postman)

**POST URL**

```
http://localhost:8000/upload/
```

**Body → form-data**

```
file: upload_excel_file.xlsx
insert_to_db: true
```

---

### 🟢 Health Check

GET `/health`

Returns database connection status.

---

### 🟢 API Documentation

Swagger UI:

```
http://localhost:8000/docs
```

---

## 🧠 How The Mapping Works

1️⃣ Excel file is uploaded
2️⃣ Pandas extracts column names
3️⃣ Database schema fields are fetched
4️⃣ Both lists are sent to LLM API
5️⃣ LLM returns JSON mapping
6️⃣ DataFrame columns renamed automatically
7️⃣ Data optionally inserted into database

---

## 📊 Sample API Response (Updated for Loan Applicant Fields)

```json
{
  "status": "success",
  "mapping": {
    "Customer ID": "applicant_id",
    "Customer Name": "applicant_name",
    "Phone Number": "phone_number",
    "Email ID": "email",
    "Aadhaar No": "aadhaar_number",
    "PAN No": "pan_number",
    "Loan Amount": "loan_amount",
    "Loan Purpose": "loan_purpose",
    "Employment Type": "employment_type",
    "Monthly Income": "monthly_income"
  },
  "rows": 50,
  "rows_inserted": 50,
  "preview": [
    {
      "applicant_id": "CUST101",
      "applicant_name": "Rahul Sharma",
      "phone_number": "9876543210",
      "email": "rahul@gmail.com",
      "aadhaar_number": "123412341234",
      "pan_number": "ABCDE1234F",
      "loan_amount": 250000,
      "loan_purpose": "Business Expansion",
      "employment_type": "Self-employed",
      "monthly_income": 45000
    }
  ]
}
```

---

## 🧪 Testing Options

You can test using:

* Postman
* Swagger UI
* cURL

---

## 📸 RESULTS / OUTPUT SCREENSHOTS



```
<img width="1435" height="143" alt="image" src="https://github.com/user-attachments/assets/d7ca0e8e-7c86-49b7-ab9a-491b645df946" />

```

---

## 📸 LLM FIELD MAPPING RESULTS

(Add mapping JSON screenshots)

```
Paste mapping results here
```

---

## 📸 DATABASE INSERT RESULTS

(Add MySQL table screenshots)

```
Paste DB screenshots here
```

---

## 🚨 Common Errors & Fixes

### ❌ Token Expired

Update `.env`:

```
DVARA_TOKEN=new_token
```

---

### ❌ Database Connection Failed

Check:

* MySQL running
* Credentials correct
* Database exists

---

### ❌ Empty Mapping Returned

Ensure:

* Excel file has headers
* LLM API is active
* Token is valid

---

## 👩‍💻 Author

Sneha Hanji

---

## ⭐ Future Improvements

* CSV & XML upload support
* UI Dashboard
* Mapping history storage
* Field validation rules
* Multi-table mapping

---

## 📜 License

For educational and internal use.
