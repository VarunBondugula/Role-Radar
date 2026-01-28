import os
import subprocess
import streamlit as st
import pandas as pd
from sqlalchemy import text
from src.db.engine import get_engine

st.title("Jobs")

engine = get_engine()

col1, col2 = st.columns([1, 2])
with col1:
    if st.button("🔄 Refresh Jobs (run ingest)"):
        # Runs your Phase 1 ingest script
        result = subprocess.run(["python", "-m", "src.ingestion.ingest_jobs"], capture_output=True, text=True)
        if result.returncode == 0:
            st.success("Ingest completed.")
            st.code(result.stdout)
        else:
            st.error("Ingest failed.")
            st.code(result.stderr)

with col2:
    # Latest ingest summary
    with engine.begin() as conn:
        row = conn.execute(text("""
          SELECT run_id, started_at, finished_at, jobs_fetched, jobs_new, jobs_updated, errors, report_path
          FROM ingest_runs
          ORDER BY started_at DESC
          LIMIT 1
        """)).mappings().first()

    if row:
        st.write("**Latest ingest run:**")
        st.json(dict(row))
    else:
        st.warning("No ingest runs found yet. Click Refresh Jobs.")

# Filters
with engine.begin() as conn:
    companies = conn.execute(text("SELECT DISTINCT company_name FROM jobs_raw ORDER BY company_name")).scalars().all()

f1, f2, f3 = st.columns([2, 1, 1])

with f1:
    company = st.selectbox("Company", ["(All)"] + companies)

with f2:
    us_only = st.toggle("US-only", value=True)

with f3:
    role_family = st.selectbox("Role family", ["(All)", "DS", "DE", "MLE"])

query = """
SELECT
  n.job_key, r.company_name, r.company_slug, n.title,
  n.primary_location, n.is_us, n.updated_at
FROM jobs_normalized n
JOIN jobs_raw r ON r.job_key = n.job_key
WHERE 1=1
"""
params = {}

if company != "(All)":
    query += " AND r.company_name = :company"
    params["company"] = company

if us_only:
    query += " AND n.is_us = true"

if role_family == "DS":
    query += " AND (LOWER(n.title) LIKE '%data scientist%' OR LOWER(n.title) LIKE '%data science%' OR LOWER(n.title) LIKE '%analytics%')"
elif role_family == "DE":
    query += " AND (LOWER(n.title) LIKE '%data engineer%' OR LOWER(n.title) LIKE '%data engineering%')"
elif role_family == "MLE":
    query += " AND (LOWER(n.title) LIKE '%machine learning%' OR LOWER(n.title) LIKE '%ml engineer%' OR LOWER(n.title) LIKE '%mle%')"

query += " ORDER BY n.updated_at DESC LIMIT 500"

with engine.begin() as conn:
    df = pd.read_sql(text(query), conn, params=params)

st.write(f"Showing **{len(df)}** jobs")
st.dataframe(df, use_container_width=True)

# Detail view
job_key = st.selectbox("Open job_key", ["(None)"] + df["job_key"].tolist() if len(df) else ["(None)"])
if job_key != "(None)":
    with engine.begin() as conn:
        job = conn.execute(text("""
          SELECT r.company_name, r.url, n.title, n.primary_location, n.job_text, n.description_text
          FROM jobs_normalized n
          JOIN jobs_raw r ON r.job_key = n.job_key
          WHERE n.job_key = :job_key
        """), {"job_key": job_key}).mappings().first()

    st.subheader(job["title"])
    st.caption(f"{job['company_name']} • {job['primary_location']}")
    if job["url"]:
        st.link_button("Open posting", job["url"])

    st.markdown("### Job Text (canonical)")
    st.text(job["job_text"][:10000])