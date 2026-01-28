import os, sys
from sqlalchemy import text

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.db.engine import get_engine

job_key = "databricks:8358626002"  # <-- change this

engine = get_engine()
with engine.begin() as conn:
    conn.execute(
        text("UPDATE jobs_raw SET content_hash = 'FORCE_CHANGE' WHERE job_key = :jk"),
        {"jk": job_key},
    )

print("forced content_hash change for", job_key)