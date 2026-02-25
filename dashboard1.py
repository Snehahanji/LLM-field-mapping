import streamlit as st
import requests
import pandas as pd
from io import BytesIO

FASTAPI_URL = "http://localhost:8000"   # change if deployed

st.set_page_config(
    page_title="Loan Applicant AI Dashboard",
    page_icon="🏦",
    layout="wide"
)

st.title("🏦 Loan Applicant AI Ingestion Dashboard")
st.caption("Upload Excel → AI Mapping → Repair → Preview → Save to DB")

uploaded_file = st.file_uploader("📤 Upload Excel file", type=["xlsx"])

col1, col2 = st.columns(2)

def mapping_confidence(mapping: dict, excel_cols: list):
    """
    Simple heuristic confidence:
    % of DB fields that were mapped by LLM
    """
    mapped_fields = set(mapping.values())
    expected_fields = {
        "applicant_id", "applicant_name", "phone_number", "email",
        "aadhaar_number", "pan_number", "loan_amount",
        "loan_purpose", "employment_type", "monthly_income"
    }
    score = len(mapped_fields & expected_fields) / len(expected_fields)
    return round(score * 100, 2)

with col1:
    if st.button("✅ Validate (Preview Only)", use_container_width=True):
        if uploaded_file is None:
            st.error("❌ Upload an Excel file first")
        else:
            with st.spinner("🧠 AI is mapping + repairing your data..."):
                files = {"file": uploaded_file.getvalue()}
                res = requests.post(f"{FASTAPI_URL}/validate/", files=files)

            if res.status_code == 200:
                data = res.json()

                st.success("🎉 Validation complete!")

                # -------------------------
                # FIELD MAPPING VIEW
                # -------------------------
                st.markdown("## 🗺️ Field Mapping (LLM Output)")
                mapping_df = pd.DataFrame(
                    list(data["mapping"].items()),
                    columns=["Excel Column", "Mapped DB Field"]
                )
                st.dataframe(mapping_df, use_container_width=True)

                # -------------------------
                # CONFIDENCE METER
                # -------------------------
                confidence = mapping_confidence(data["mapping"], mapping_df["Excel Column"].tolist())
                st.markdown("## 📊 Mapping Confidence")
                st.progress(confidence / 100)
                st.write(f"Confidence Score: **{confidence}%**")

                if confidence < 70:
                    st.warning("⚠️ Low mapping confidence. Please review mapping carefully.")
                else:
                    st.success("✅ Mapping looks reliable.")

                # -------------------------
                # BEFORE vs AFTER PREVIEW
                # -------------------------
                st.markdown("## 🔁 Before vs After (Side by Side Preview)")

                orig_df = pd.read_excel(uploaded_file)
                cleaned_df = pd.DataFrame(data["preview"])

                c1, c2 = st.columns(2)
                with c1:
                    st.subheader("📥 Original (Top 20 rows)")
                    st.dataframe(orig_df.head(20), use_container_width=True)

                with c2:
                    st.subheader("✨ Cleaned (Top 20 rows)")
                    st.dataframe(cleaned_df, use_container_width=True)

                # -------------------------
                # UNMAPPED WARNING
                # -------------------------
                st.markdown("## ⚠️ Unmapped Excel Columns")
                unmapped = set(orig_df.columns) - set(data["mapping"].keys())
                if unmapped:
                    st.warning(f"These columns were NOT used: {list(unmapped)}")
                else:
                    st.success("All Excel columns were mapped or used.")

                # -------------------------
                # DOWNLOAD CLEANED FILE
                # -------------------------
                st.markdown("## 📥 Download Cleaned Excel")
                buffer = BytesIO()
                cleaned_df.to_excel(buffer, index=False)
                st.download_button(
                    label="⬇️ Download Cleaned Excel",
                    data=buffer.getvalue(),
                    file_name="cleaned_loan_applicants.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            else:
                st.error("❌ Validation failed")
                st.code(res.text)

with col2:
    if st.button("🚀 Upload to Database", use_container_width=True):
        if uploaded_file is None:
            st.error("❌ Upload an Excel file first")
        else:
            with st.spinner("💾 Uploading clean data to DB..."):
                files = {"file": uploaded_file.getvalue()}
                res = requests.post(f"{FASTAPI_URL}/upload/", files=files)

            if res.status_code == 200:
                data = res.json()

                st.success("🎉 Upload successful!")
                c1, c2 = st.columns(2)
                c1.metric("🆕 Inserted", data["inserted"])
                c2.metric("🔄 Updated", data["updated"])
            else:
                st.error("❌ Upload failed")
                st.code(res.text)

# -------------------------
# API HEALTH CHECK
# -------------------------
st.markdown("---")
st.markdown("### 🩺 API Health Check")

try:
    health = requests.get(f"{FASTAPI_URL}/")
    if health.status_code == 200:
        st.success("FastAPI is running ✔️")
        st.json(health.json())
    else:
        st.warning("FastAPI not responding correctly")
except:
    st.error("FastAPI server is OFF ❌")