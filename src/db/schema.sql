CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS jobs_raw (
  job_key TEXT PRIMARY KEY,
  company_slug TEXT NOT NULL,
  company_name TEXT NOT NULL,
  greenhouse_id TEXT NOT NULL,
  url TEXT,
  payload_json JSONB NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  content_hash TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_jobs_raw_company_slug ON jobs_raw(company_slug);
CREATE INDEX IF NOT EXISTS idx_jobs_raw_last_seen ON jobs_raw(last_seen_at);

CREATE TABLE IF NOT EXISTS jobs_normalized (
  job_key TEXT PRIMARY KEY REFERENCES jobs_raw(job_key) ON DELETE CASCADE,
  title TEXT NOT NULL,
  location_raw JSONB,
  primary_location TEXT,
  is_us BOOLEAN NOT NULL DEFAULT FALSE,
  description_text TEXT,
  job_text TEXT NOT NULL,
  posted_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_norm_is_us ON jobs_normalized(is_us);
CREATE INDEX IF NOT EXISTS idx_jobs_norm_posted ON jobs_normalized(posted_at);

CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id UUID PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NOT NULL,
  jobs_fetched INT NOT NULL,
  jobs_new INT NOT NULL,
  jobs_updated INT NOT NULL,
  errors INT NOT NULL,
  report_path TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_started ON ingest_runs(started_at);

CREATE TABLE IF NOT EXISTS embedding_runs (
  embedding_run_id UUID PRIMARY KEY,
  model_name TEXT NOT NULL,
  dim INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS job_embeddings (
  job_key TEXT NOT NULL REFERENCES jobs_raw(job_key) ON DELETE CASCADE,
  embedding_run_id UUID NOT NULL REFERENCES embedding_runs(embedding_run_id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  embedding vector NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (job_key, embedding_run_id)
);

