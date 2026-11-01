"""Ingestion service: the operational write path into Postgres.

POST /v1/records takes DSNY records in their published (wide) shape and upserts them.
Run with: uvicorn nyrt.source.service:app  (or `nyrt serve`).
"""

from __future__ import annotations

from datetime import date

import psycopg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from nyrt.config import get_settings
from nyrt.source import repository

app = FastAPI(title="NY Recycling Tracker ingestion service", version="0.1.0")


class RecordBatch(BaseModel):
    source: str = Field(description="Where the batch came from, for the load log")
    records: list[dict] = Field(min_length=1)
    months: list[date] | None = Field(default=None, description="Only write these months")


@app.get("/healthz")
def healthz() -> dict:
    with psycopg.connect(get_settings().pg_dsn) as conn:
        conn.execute("select 1")
    return {"ok": True}


@app.post("/v1/records")
def post_records(batch: RecordBatch) -> dict:
    with psycopg.connect(get_settings().pg_dsn) as conn:
        try:
            return repository.upsert_records(
                conn, batch.records, batch.source, set(batch.months) if batch.months else None
            )
        except repository.RecordError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
