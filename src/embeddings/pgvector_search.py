from sqlalchemy import text
from sentence_transformers import SentenceTransformer

from src.db.engine import get_engine


def get_latest_embedding_run(engine):
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT embedding_run_id, model_name, dim
            FROM embedding_runs
            ORDER BY created_at DESC
            LIMIT 1
        """)).mappings().first()
    return row


def _to_vector_literal(vec) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


def semantic_search(query: str, k: int = 20, us_only: bool = True, role_family: str = "(All)", company: str = "(All)"):
    engine = get_engine()
    run = get_latest_embedding_run(engine)
    if not run:
        raise RuntimeError("No embedding_runs found. Run: python -m src.embeddings.embed_jobs")

    model = SentenceTransformer(run["model_name"])
    q_emb = model.encode([query], normalize_embeddings=True)[0]
    q_emb_lit = _to_vector_literal(q_emb)

    where = ["e.embedding_run_id = CAST(:rid AS uuid)"]
    params = {"qvec": q_emb_lit, "rid": str(run["embedding_run_id"]), "k": k}

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

    sql = f"""
        SELECT
          n.job_key,
          r.company_name,
          n.title,
          n.primary_location,
          1 - (e.embedding <=> CAST(:qvec AS vector)) AS cosine_sim
        FROM job_embeddings e
        JOIN jobs_normalized n ON n.job_key = e.job_key
        JOIN jobs_raw r ON r.job_key = e.job_key
        WHERE {where_sql}
        ORDER BY e.embedding <=> CAST(:qvec AS vector)
        LIMIT :k
    """

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    return rows