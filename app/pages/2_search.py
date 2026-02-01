import streamlit as st
import pandas as pd

from src.embeddings.pgvector_search import semantic_search
from sqlalchemy import text
from src.db.engine import get_engine
from src.retrieval.tfidf_search import tfidf_search

engine = get_engine()
with engine.begin() as conn:
    companies = conn.execute(text("SELECT DISTINCT company_name FROM jobs_raw ORDER BY company_name")).scalars().all()

c1, c2, c3, c4 = st.columns([1, 1, 2, 1.2])
with c1:
    us_only = st.toggle("US-only", value=True)
with c2:
    role_family = st.selectbox("Role family", ["(All)", "DS", "DE", "MLE"])
with c3:
    company = st.selectbox("Company", ["(All)"] + companies)
with c4:
    mode = st.selectbox("Mode", ["Semantic", "TF-IDF", "Compare"])

st.title("RoleRadar • Search")

q = st.text_input("Search query", value="entry level data engineer python")
k = st.slider("Top K", 5, 50, 20, 5)

def show_results(df: pd.DataFrame, label: str):
    st.subheader(label)
    st.dataframe(df, use_container_width=True)
    if len(df):
        jk = st.selectbox(f"Open result job_key ({label})", ["(None)"] + df["job_key"].tolist(), key=f"open_{label.lower().replace(' ', '_').replace('(', '').replace(')', '')}")
        if jk != "(None)":
            st.write("Selected:", jk)

if st.button("Search"):
    if mode == "Semantic":
        rows = semantic_search(q, k=k, us_only=us_only, role_family=role_family, company=company)
        df = pd.DataFrame(rows, columns=["job_key", "company_name", "title", "primary_location", "score"])
        show_results(df, "Semantic (pgvector)")

    elif mode == "TF-IDF":
        rows = tfidf_search(q, k=k)
        df = pd.DataFrame(rows, columns=["job_key", "company_name", "title", "primary_location", "score"])
        show_results(df, "TF-IDF baseline")

    else:
        left, right = st.columns(2)

        with left:
            rows = semantic_search(q, k=k, us_only=us_only, role_family=role_family, company=company)
            df1 = pd.DataFrame(rows, columns=["job_key", "company_name", "title", "primary_location", "score"])
            show_results(df1, "Semantic (pgvector)")

        with right:
            rows = tfidf_search(q, k=k)
            df2 = pd.DataFrame(rows, columns=["job_key", "company_name", "title", "primary_location", "score"])
            show_results(df2, "TF-IDF baseline")