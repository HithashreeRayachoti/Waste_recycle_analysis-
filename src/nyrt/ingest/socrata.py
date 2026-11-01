"""Pull DSNY Monthly Tonnage from the Socrata API and land it in bronze, unchanged."""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests

from nyrt import dsny
from nyrt.config import DSNY_DATASET_ID, SOCRATA_DOMAIN, Settings

log = logging.getLogger(__name__)
PAGE_SIZE = 50_000


class BronzeCheckError(RuntimeError):
    pass


@dataclass
class Landing:
    path: Path
    extracted_at: datetime
    rows: list[dict]


def fetch_all(settings: Settings, session: requests.Session | None = None) -> tuple[list[dict], dict]:
    """Page through the whole dataset. It is small (one row per district-month), so a full pull
    each run is cheap and lets us detect revisions to any month."""
    session = session or requests.Session()
    url = f"https://{SOCRATA_DOMAIN}/resource/{DSNY_DATASET_ID}.json"
    headers = {"X-App-Token": settings.socrata_app_token} if settings.socrata_app_token else {}
    rows: list[dict] = []
    offset = 0
    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}
        resp = session.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        page = resp.json()
        rows.extend(page)
        log.info("fetched %s rows at offset %s", len(page), offset)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    query = {
        "url": url,
        "params": {"$order": ":id", "$limit": PAGE_SIZE},
        "pages": offset // PAGE_SIZE + 1,
    }
    return rows, query


def read_csv_export(path: Path) -> tuple[list[dict], dict]:
    """Read a CSV exported from the NYC Open Data portal (for when the API is unreachable)."""
    with path.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return rows, {"file": str(path), "note": "manual CSV export from NYC Open Data"}


def normalize_keys(rows: list[dict]) -> list[dict]:
    return [{dsny.normalize_column(k): v for k, v in row.items()} for row in rows]


def check_bronze(rows: list[dict]) -> dict:
    """Bronze checks from the design: row count > 0 and the expected column set is present."""
    if not rows:
        raise BronzeCheckError("bronze check failed: zero rows returned")
    columns: set[str] = set()
    for row in rows:
        columns.update(row.keys())
    missing, unknown = dsny.check_schema(columns)
    if missing:
        raise BronzeCheckError(f"bronze check failed: missing columns {missing}")
    if unknown:
        log.warning("columns not mapped to a material (ignored downstream): %s", unknown)
    return {"row_count": len(rows), "columns": sorted(columns), "unmapped_columns": unknown}


def land(settings: Settings, rows: list[dict], query: dict) -> Landing:
    """Write the response exactly as received, plus a manifest with extraction time and query."""
    extracted_at = datetime.now(UTC).replace(microsecond=0)
    stamp = extracted_at.strftime("%Y%m%dT%H%M%SZ")
    out_dir = settings.bronze_dir / "raw_api" / f"extracted_at={stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "response.json").write_text(json.dumps(rows))
    normalized = normalize_keys(rows)
    summary = check_bronze(normalized)
    manifest = {
        "dataset": DSNY_DATASET_ID,
        "extracted_at": extracted_at.isoformat(),
        "query": query,
        **summary,
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))
    log.info("landed %s rows in %s", len(rows), out_dir)
    return Landing(out_dir, extracted_at, normalized)
