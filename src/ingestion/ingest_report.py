import json
from pathlib import Path
from typing import Any, Dict

def write_ingest_report(run_id: str, report: Dict[str, Any]) -> str:
    out_dir = Path("artifacts/ingest")
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{run_id}.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return str(path)