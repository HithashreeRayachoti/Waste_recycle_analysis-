"""The pipeline steps, as plain functions that the CLI, Airflow, and tests all call."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import psycopg
import requests

from nyrt import dsny
from nyrt.analytics import export
from nyrt.config import Settings
from nyrt.ingest import socrata
from nyrt.source import extract, repository
from nyrt.transform import warehouse

log = logging.getLogger(__name__)
INCREMENTAL_MONTHS = 3  # re-pull window, in case DSNY revises recent figures


def latest_months(records: list[dict], n: int) -> set[date]:
    months = sorted({dsny.parse_month(r["month"]) for r in records if r.get("month")})
    return set(months[-n:])


def init_db(settings: Settings) -> None:
    with psycopg.connect(settings.pg_dsn) as conn:
        repository.init_schema(conn)


def ingest(settings: Settings, from_file: Path | None = None, backfill: bool = True) -> dict:
    """Bronze landing of the API response, then the operational write into Postgres."""
    rows, query = socrata.read_csv_export(from_file) if from_file else socrata.fetch_all(settings)
    landing = socrata.land(settings, rows, query)
    months = None if backfill else latest_months(landing.rows, INCREMENTAL_MONTHS)
    source = f"socrata:{landing.extracted_at.isoformat()}" if not from_file else f"file:{from_file}"
    if settings.ingest_url:
        resp = requests.post(
            f"{settings.ingest_url.rstrip('/')}/v1/records",
            json={
                "source": source,
                "records": landing.rows,
                "months": sorted(m.isoformat() for m in months) if months else None,
            },
            timeout=300,
        )
        resp.raise_for_status()
        result = resp.json()
    else:
        with psycopg.connect(settings.pg_dsn) as conn:
            result = repository.upsert_records(conn, landing.rows, source, months)
    result["months"] = sorted(m.isoformat() for m in months) if months else "all"
    result["bronze"] = str(landing.path)
    return result


def bronze_extract(settings: Settings, months: list[date] | None) -> list[date]:
    return extract.extract_months(settings, months)


def transform(settings: Settings, months: list[date] | None) -> dict:
    con = warehouse.connect(settings)
    try:
        warehouse.load_seeds(con, settings)
        months = months or warehouse.available_bronze_months(settings)
        silver = warehouse.run_silver(con, settings, months)
        gold = warehouse.run_gold(con, settings)
        return {"silver": silver, "gold": gold}
    finally:
        con.close()


def export_dashboard(settings: Settings, out_dir: Path | None = None) -> list[Path]:
    con = warehouse.connect(settings)
    try:
        return export.write(con, out_dir or settings.export_dir, settings.bronze_dir)
    finally:
        con.close()


def run_all(settings: Settings, backfill: bool, from_file: Path | None = None) -> dict:
    init_db(settings)
    ingested = ingest(settings, from_file=from_file, backfill=backfill)
    months = None if backfill else [date.fromisoformat(m) for m in ingested["months"]]
    extracted = bronze_extract(settings, months)
    transformed = transform(settings, extracted)
    exported = export_dashboard(settings)
    return {
        "ingest": ingested,
        "bronze_months": len(extracted),
        "transform": transformed,
        "exported": [str(p) for p in exported],
    }
