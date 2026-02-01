from pathlib import Path
import numpy as np
import joblib
from scipy import sparse
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import text, bindparam

from src.db.engine import get_engine

ART_DIR = Path("artifacts/tfidf")

def tfidf_search(query: str, k: int = 20):
    vectorizer = joblib.load(ART_DIR / "vectorizer.joblib")
    X = sparse.load_npz(ART_DIR / "matrix.npz")
    job_keys = np.load(ART_DIR / "job_keys.npy", allow_pickle=True).tolist()

    q = vectorizer.transform([query])
    sims = cosine_similarity(q, X).ravel()

    top_idx = sims.argsort()[::-1][:k]
    top_keys = [job_keys[i] for i in top_idx]
    top_scores = [float(sims[i]) for i in top_idx]

    engine = get_engine()
    stmt = text("""
        SELECT n.job_key, r.company_name, n.title, n.primary_location
        FROM jobs_normalized n
        JOIN jobs_raw r ON r.job_key = n.job_key
        WHERE n.job_key IN :keys
    """).bindparams(bindparam("keys", expanding=True))

    with engine.begin() as conn:
        rows = conn.execute(stmt, {"keys": top_keys}).fetchall()

    meta = {r[0]: (r[1], r[2], r[3]) for r in rows}
    results = []
    for jk, score in zip(top_keys, top_scores):
        company_name, title, loc = meta.get(jk, ("", "", ""))
        results.append((jk, company_name, title, loc, score))
    return results