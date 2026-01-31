import uuid
from datetime import datetime, timezone
from typing import List, Tuple

import numpy as np
from sqlalchemy import text
from src.db.engine import get_engine

from sentence_transformers import SentenceTransformer
from pgvector.sqlalchemy import Vector 

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  
BATCH_SIZE = 64

def now_utc():
    return datetime.now(timezone.utc)

def fetch_jobs(engine) -> List[Tuple[str, str]]:
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT job_key, job_text
            FROM jobs_normalized
            WHERE job_text IS NOT NULL AND job_text <> ''
        """)).fetchall()
    return [(r[0], r[1]) for r in rows]

def create_embedding_run(engine, model_name: str, dim: int) -> str:
    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO embedding_runs (embedding_run_id, model_name, dim, created_at)
            VALUES (CAST(:rid AS uuid), :model_name, :dim, :created_at)
        """), {"rid": run_id, "model_name": model_name, "dim": dim, "created_at": now_utc()})
    return run_id

def upsert_embeddings(engine, run_id: str, model_name: str, embeddings: List[Tuple[str, List[float]]]):
    with engine.begin() as conn:
        for job_key, emb in embeddings:
            conn.execute(text("""
                INSERT INTO job_embeddings (job_key, embedding_run_id, model_name, embedding, created_at)
                VALUES (:job_key, CAST(:rid AS uuid), :model_name, :embedding, :created_at)
                ON CONFLICT (job_key, embedding_run_id) DO UPDATE SET
                  embedding = EXCLUDED.embedding,
                  created_at = EXCLUDED.created_at
            """), {"job_key": job_key, "rid": run_id, "model_name": model_name, "embedding": emb, "created_at": now_utc()})

def main():
    engine = get_engine()
    jobs = fetch_jobs(engine)
    print(f"Found {len(jobs)} jobs to embed")

    model = SentenceTransformer(MODEL_NAME)
    dim = model.get_sentence_embedding_dimension()
    run_id = create_embedding_run(engine, MODEL_NAME, dim)
    print("Embedding run:", run_id, "dim:", dim)

    job_keys = [jk for jk, _ in jobs]
    texts = [t for _, t in jobs]

    all_embeddings = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch_texts = texts[i:i+BATCH_SIZE]
        batch_keys = job_keys[i:i+BATCH_SIZE]

        vecs = model.encode(batch_texts, normalize_embeddings=True, show_progress_bar=False)
        vecs = vecs.astype(np.float32)

        batch_pairs = [(k, v.tolist()) for k, v in zip(batch_keys, vecs)]
        upsert_embeddings(engine, run_id, MODEL_NAME, batch_pairs)

        print(f"Embedded {min(i+BATCH_SIZE, len(texts))}/{len(texts)}")

    print("✅ Embedding complete. Run id:", run_id)

if __name__ == "__main__":
    main()