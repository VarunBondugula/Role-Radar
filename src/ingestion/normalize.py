import re
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from dateutil import parser as dateparser

TAG_RE = re.compile(r"<[^>]+>")

US_HINTS = [
    "United States", "USA", "US", "U.S.", "Remote - US", "Remote (US)", "United States of America"
]

def clean_html(html: Optional[str]) -> str:
    if not html:
        return ""
    txt = TAG_RE.sub(" ", html)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def primary_location_from_payload(job: Dict[str, Any]) -> str:
    # embed payload often has: location: { name: "City, State" } or a string
    loc = job.get("location")
    if isinstance(loc, dict):
        return (loc.get("name") or "").strip()
    if isinstance(loc, str):
        return loc.strip()
    return ""

def is_us_location(primary_location: str) -> bool:
    if not primary_location:
        return False
    if any(h.lower() in primary_location.lower() for h in US_HINTS):
        return True
    # crude heuristic: contains ", <2-letter state>" in many postings
    return bool(re.search(r",\s*[A-Z]{2}\b", primary_location))

def build_job_text(title: str, primary_location: str, description_text: str) -> str:
    parts = [title.strip()]
    if primary_location.strip():
        parts.append(primary_location.strip())
    if description_text.strip():
        parts.append(description_text.strip())
    return "\n\n".join(parts)

def compute_hash(title: str, primary_location: str, description_text: str) -> str:
    payload = f"{title}|{primary_location}|{description_text}".encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).hexdigest()

def now_utc():
    return datetime.now(timezone.utc)

def normalize_job(company_slug: str, company_name: str, job: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    job_id = str(job.get("id"))
    title = (job.get("title") or "").strip()
    url = job.get("absolute_url") or job.get("url")

    # embed payload sometimes contains "content" (html). If not, we store empty for Phase 1.
    description_html = job.get("content") or ""
    description_text = clean_html(description_html)

    posted_at = None
    fp = job.get("first_published")
    if fp:
        try:
            posted_at = dateparser.parse(fp)
        except Exception:
            posted_at = None

    primary_location = primary_location_from_payload(job)
    is_us = is_us_location(primary_location)

    job_text = build_job_text(title, primary_location, description_text)
    content_hash = compute_hash(title, primary_location, description_text)

    job_key = f"{company_slug}:{job_id}"

    t = now_utc()

    raw_row = {
        "job_key": job_key,
        "company_slug": company_slug,
        "company_name": company_name,
        "greenhouse_id": job_id,
        "url": url,
        "payload_json": job,
        "first_seen_at": t,
        "last_seen_at": t,
        "content_hash": content_hash,
    }

    norm_row = {
        "job_key": job_key,
        "title": title,
        "location_raw": job.get("location"),
        "primary_location": primary_location,
        "is_us": is_us,
        "description_text": description_text,
        "job_text": job_text,
        "posted_at": posted_at,         # embed payload may not include posted date; fill later if available
        "updated_at": t,
    }

    return raw_row, norm_row