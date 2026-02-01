import os
from pathlib import Path
import numpy as np
import joblib
from scipy import sparse
from sqlalchemy import text
from sklearn.feature_extraction.text import TfidfVectorizer

from src.db.engine import get_engine

ART_DIR = Path("artifacts/tfidf")
ART_DIR.mkdir(parents=True, exist_ok=True)

def fetch_corpus(us_only=True, role_family="(All)", company="(All)"):
    engine = get_engine()
    where = ["n.job_text IS NOT NULL", "n.job_text <> ''"]
    params = {}

    if us_only:
        where.append("n.is_us = true")
    if company != "(All)":
        where.append("r.company_name = :company")
        params["company"] = company

    if role_family == "DS":
        where.append("(LOWER(n.title) LIKE '%data scientist%' OR LOWER(n.title) LIKE '%data science%' OR LOWER(n.title) LIKE '%analytics%')")
    elif role_family == "DE":
        where.append("(LOWER(n.title) LIKE '%data engineer%' OR LOWER(n.title) LIKE '%data engineering%')")
    elif role_family == "MLE":
        where.append("(LOWER(n.title) LIKE '%machine learning%' OR LOWER(n.title) LIKE '%ml engineer%' OR LOWER(n.title) LIKE '%mle%')")

    where_sql = " AND ".join(where)

    with engine.begin() as conn:
        rows = conn.execute(text(f"""
            SELECT n.job_key, n.job_text
            FROM jobs_normalized n
            JOIN jobs_raw r ON r.job_key = n.job_key
            WHERE {where_sql}
        """), params).fetchall()

    job_keys = [r[0] for r in rows]
    texts = [r[1] for r in rows]
    return job_keys, texts

def main():
    us_only = True
    role_family = "(All)"
    company = "(All)"

    job_keys, texts = fetch_corpus(us_only=us_only, role_family=role_family, company=company)
    print("Docs:", len(texts))

    vectorizer = TfidfVectorizer(
        stop_words="english",
        max_features=60000,
        ngram_range=(1, 2),
        min_df=2,
    )

    X = vectorizer.fit_transform(texts)  
    joblib.dump(vectorizer, ART_DIR / "vectorizer.joblib")
    sparse.save_npz(ART_DIR / "matrix.npz", X)
    np.save(ART_DIR / "job_keys.npy", np.array(job_keys, dtype=object))

    print("✅ TF-IDF index built:", ART_DIR)

if __name__ == "__main__":
    main()