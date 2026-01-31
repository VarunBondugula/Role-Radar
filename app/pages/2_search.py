import streamlit as st
import pandas as pd

from src.embeddings.pgvector_search import semantic_search
from sqlalchemy import text
from src.db.engine import get_engine

engine = get_engine()
with engine.begin() as conn:
    companies = conn.execute(text("SELECT DISTINCT company_name FROM jobs_raw ORDER BY company_name")).scalars().all()

f1, f2, f3 = st.columns([1, 1, 2])
with f1:
    us_only = st.toggle("US-only", value=True)
with f2:
    role_family = st.selectbox("Role family", ["(All)", "DS", "DE", "MLE"])
with f3:
    company = st.selectbox("Company", ["(All)"] + companies)

st.title("RoleRadar • Semantic Search")

q = st.text_input("Search jobs (semantic)", value="entry level data engineer python")
k = st.slider("Top K", 5, 50, 20, 5)

if st.button("Search"):
    rows = semantic_search(q, k=k, us_only=us_only, role_family=role_family, company=company)
    df = pd.DataFrame(rows, columns=["job_key", "company_name", "title", "primary_location", "cosine_sim"])
    st.dataframe(df, use_container_width=True)

    if len(df):
        jk = st.selectbox("Open result job_key", ["(None)"] + df["job_key"].tolist())
        if jk != "(None)":
            st.write("Selected:", jk)