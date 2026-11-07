"""Bronze extract of the Postgres source, one Parquet file per month (overwritten on rerun)."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg

from nyrt.config import Settings

QUERY = """
select borough, community_district, month, source_column, raw_month,
       tons::double precision as tons, revision, last_loaded_at as source_loaded_at
from ops.dsny_collection
where (%(months)s::date[] is null or month = any(%(months)s::date[]))
order by month, borough, community_district, source_column
"""


def extract_dir(settings: Settings) -> Path:
    return settings.bronze_dir / "source_extract"


def extract_months(settings: Settings, months: list[date] | None) -> list[date]:
    with psycopg.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(QUERY, {"months": months})
        cols = [d.name for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
    if df.empty:
        raise RuntimeError("bronze extract: Postgres returned no rows for the requested months")
    df["month"] = pd.to_datetime(df["month"]).dt.date
    written = []
    for month, part in df.groupby("month"):
        out = extract_dir(settings) / f"month={month:%Y-%m}"
        if out.exists():
            shutil.rmtree(out)
        out.mkdir(parents=True)
        part.to_parquet(out / "part-0.parquet", index=False)
        written.append(month)
    return sorted(written)
