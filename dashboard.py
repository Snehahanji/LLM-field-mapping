import streamlit as st
import requests
import pandas as pd

# ================= CONFIG =================
API_BASE = "http://localhost:8000"

st.set_page_config(page_title="Loan Applicant Dashboard", layout="wide")

# ================= HELPER FUNCTION =================
def fix_arrow(df):
    """
    Convert all object columns to string to avoid
    Streamlit PyArrow serialization errors.
    """
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
    return df


# ================= TITLE =================
st.title("🏦 Loan Applicant Field Mapping Dashboard")

# ================= SIDEBAR =================
st.sidebar.header("⚙️ Settings")

table_name = st.sidebar.text_input("Table Name", "loan_applicants")
insert_to_db = st.sidebar.checkbox("Insert into Database", True)

st.sidebar.divider()

# ================= HEALTH CHECK =================
st.sidebar.subheader("❤️ API Health")

if st.sidebar.button("Check Health"):

    try:
        res = requests.get(f"{API_BASE}/health")

        if res.status_code == 200:
            st.sidebar.success("API Connected")
            st.sidebar.json(res.json())
        else:
            st.sidebar.error("API not reachable")

    except Exception as e:
        st.sidebar.error(str(e))


# ================= FILE UPLOAD =================
st.header("📤 Upload Excel File")

uploaded_file = st.file_uploader(
    "Choose Excel file",
    type=["xlsx", "xls"]
)

# ================= PROCESS BUTTON =================
if uploaded_file:

    st.info("File uploaded successfully")

    if st.button("🚀 Upload & Process"):

        with st.spinner("Processing file and calling API..."):

            try:
                files = {"file": uploaded_file}

                params = {
                    "table_name": table_name,
                    "insert_to_db": insert_to_db
                }

                response = requests.post(
                    f"{API_BASE}/upload/",
                    files=files,
                    params=params,
                    timeout=120
                )

                # ================= SUCCESS =================
                if response.status_code == 200:

                    data = response.json()

                    st.success("✅ Upload Successful!")

                    # ================= METRICS =================
                    st.subheader("📊 Processing Summary")

                    col1, col2, col3 = st.columns(3)

                    col1.metric(
                        "Total Rows",
                        data["total_rows_in_file"]
                    )

                    col2.metric(
                        "Rows Inserted",
                        data["rows_inserted"]
                    )

                    col3.metric(
                        "Duplicates Skipped",
                        data["duplicates_skipped"]
                    )

                    st.divider()

                    # ================= FIELD MAPPING =================
                    st.subheader("🔗 Field Mapping")

                    mapping_df = pd.DataFrame(
                        list(data["mapping"].items()),
                        columns=["Excel Column", "Database Field"]
                    )

                    mapping_df = fix_arrow(mapping_df)

                    st.dataframe(mapping_df, use_container_width=True)

                    st.divider()

                    # ================= DATA PREVIEW =================
                    st.subheader("👀 Data Preview")

                    preview_df = pd.DataFrame(data["preview"])

                    preview_df = fix_arrow(preview_df)

                    st.dataframe(preview_df, use_container_width=True)

                # ================= ERROR FROM API =================
                else:
                    st.error("API Error")
                    st.text(response.text)

            except Exception as e:
                st.error(f"Error: {str(e)}")


# ================= FOOTER =================
st.divider()
st.caption("Loan Applicant Mapping System • Powered by FastAPI + Streamlit")
