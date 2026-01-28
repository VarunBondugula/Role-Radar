import requests
from typing import Any, Dict, List, Optional

BASE = "https://api.greenhouse.io/v1/boards"

class GreenhouseClient:
    def __init__(self, timeout_s: int = 30):
        self.timeout_s = timeout_s
        self.session = requests.Session()

    def fetch_embed_jobs(self, company_slug: str) -> List[Dict[str, Any]]:
        url = f"{BASE}/{company_slug}/embed/jobs"
        r = self.session.get(url, timeout=self.timeout_s)
        r.raise_for_status()
        data = r.json()
        return data.get("jobs", [])

    def fetch_job_detail_optional(self, company_slug: str, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Optional detail endpoint
        """
        url = f"{BASE}/{company_slug}/jobs/{job_id}"
        r = self.session.get(url, timeout=self.timeout_s)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()