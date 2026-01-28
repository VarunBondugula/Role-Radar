import uuid
import yaml
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

from sqlalchemy import text
from src.db.engine import get_engine
from src.ingestion.greenhouse_client import GreenhouseClient
from src.ingestion.normalize import normalize_job
from src.ingestion.ingest_report import write_ingest_report

BOARDS_PATH = Path("data/boards.yml")

UPSERT_RAW = """
INSERT INTO jobs_raw (
  job_key, company_slug, company_name, greenhouse_id, url,
  payload_json, first_seen_at, last_seen_at, content_hash
)
VALUES (
  :job_key, :company_slug, :company_name, :greenhouse_id, :url,
  CAST(:payload_json AS jsonb), :first_seen_at, :last_seen_at, :content_hash
)
ON CONFLICT (job_key) DO UPDATE SET
  company_name = EXCLUDED.company_name,
  url = EXCLUDED.url,
  payload_json = EXCLUDED.payload_json,
  last_seen_at = EXCLUDED.last_seen_at,
  content_hash = EXCLUDED.content_hash
RETURNING (xmax = 0) AS inserted;
"""

UPSERT_NORM = """
INSERT INTO jobs_normalized (
  job_key, title, location_raw, primary_location, is_us,
  description_text, job_text, posted_at, updated_at
)
VALUES (
  :job_key, :title, CAST(:location_raw AS jsonb), :primary_location, :is_us,
  :description_text, :job_text, :posted_at, :updated_at
)
ON CONFLICT (job_key) DO UPDATE SET
  title = EXCLUDED.title,
  location_raw = EXCLUDED.location_raw,
  primary_location = EXCLUDED.primary_location,
  is_us = EXCLUDED.is_us,
  description_text = EXCLUDED.description_text,
  job_text = EXCLUDED.job_text,
  posted_at = EXCLUDED.posted_at,
  updated_at = EXCLUDED.updated_at;
"""

INSERT_INGEST_RUN = """
INSERT INTO ingest_runs (
  run_id, started_at, finished_at, jobs_fetched, jobs_new, jobs_updated, errors, report_path
)
VALUES (
  CAST(:run_id AS uuid), :started_at, :finished_at, :jobs_fetched, :jobs_new, :jobs_updated, :errors, :report_path
);
"""


def now_utc():
    return datetime.now(timezone.utc)


def load_boards() -> List[Dict[str, str]]:
    data = yaml.safe_load(BOARDS_PATH.read_text(encoding="utf-8"))
    return data["boards"]


def main():
    run_id = str(uuid.uuid4())
    started_at = now_utc()

    client = GreenhouseClient()
    engine = get_engine()

    jobs_fetched = 0
    jobs_new = 0
    jobs_updated = 0
    errors = 0

    per_company: Dict[str, Any] = {}
    boards = load_boards()

    with engine.begin() as conn:
        for b in boards:
            slug = b["slug"]
            name = b.get("name", slug)

            try:
                jobs = client.fetch_embed_jobs(slug)
                jobs_fetched += len(jobs)
                per_company[slug] = {"company_name": name, "fetched": len(jobs), "new": 0, "updated": 0}

                for job in jobs:
                    raw_row, norm_row = normalize_job(slug, name, job)

    
                    old_hash = conn.execute(
                        text("SELECT content_hash FROM jobs_raw WHERE job_key = :job_key"),
                        {"job_key": raw_row["job_key"]},
                    ).scalar()

    
                    raw_row["payload_json"] = json.dumps(raw_row["payload_json"], ensure_ascii=False)
                    if norm_row["location_raw"] is not None:
                        norm_row["location_raw"] = json.dumps(norm_row["location_raw"], ensure_ascii=False)

                  
                    res = conn.execute(text(UPSERT_RAW), raw_row).mappings().first()
                    inserted = bool(res["inserted"]) if res else False

                    if inserted:
                        jobs_new += 1
                        per_company[slug]["new"] += 1
                    else:
                        if old_hash is not None and old_hash != raw_row["content_hash"]:
                            jobs_updated += 1
                            per_company[slug]["updated"] += 1

                    
                    conn.execute(text(UPSERT_NORM), norm_row)

            except Exception as e:
                errors += 1
                per_company[slug] = {"company_name": name, "error": str(e)}

    finished_at = now_utc()

    report = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "jobs_fetched": jobs_fetched,
        "jobs_new": jobs_new,
        "jobs_updated": jobs_updated,
        "errors": errors,
        "per_company": per_company,
    }
    report_path = write_ingest_report(run_id, report)

    with engine.begin() as conn:
        conn.execute(
            text(INSERT_INGEST_RUN),
            {
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "jobs_fetched": jobs_fetched,
                "jobs_new": jobs_new,
                "jobs_updated": jobs_updated,
                "errors": errors,
                "report_path": report_path,
            },
        )

    print(f"✅ Ingest complete. fetched={jobs_fetched} new={jobs_new} updated={jobs_updated} errors={errors}")
    print(f"📄 Report: {report_path}")


if __name__ == "__main__":
    main()