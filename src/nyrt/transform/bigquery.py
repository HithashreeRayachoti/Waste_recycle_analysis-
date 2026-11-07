"""Mirror the gold star schema into BigQuery (the free sandbox works).

    pip install -e ".[bigquery]"
    gcloud auth application-default login
    nyrt-bq --project my-project --dataset nyrt_gold

Each table is exported from DuckDB to Parquet and loaded with WRITE_TRUNCATE, so reruns are
idempotent. Views are recreated in BigQuery from sql/gold/model.sql by hand if wanted; the
dashboard only needs the tables and the JSON export.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from nyrt.config import get_settings
from nyrt.transform import warehouse

TABLES = ["dim_district", "dim_material", "dim_month", "fact_tonnage", "ref_recovery_rates_used"]


def load(project: str, dataset: str, location: str = "US") -> list[str]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    client.create_dataset(bigquery.Dataset(f"{project}.{dataset}"), exists_ok=True)
    con = warehouse.connect(get_settings())
    loaded = []
    with tempfile.TemporaryDirectory() as tmp:
        for table in TABLES:
            path = Path(tmp) / f"{table}.parquet"
            con.execute(f"copy gold.{table} to '{path}' (format parquet)")
            job = client.load_table_from_file(
                path.open("rb"),
                f"{project}.{dataset}.{table}",
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ),
                location=location,
            )
            job.result()
            loaded.append(f"{dataset}.{table}")
    con.close()
    return loaded


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", default="nyrt_gold")
    ap.add_argument("--location", default="US")
    a = ap.parse_args()
    print("\n".join(load(a.project, a.dataset, a.location)))


if __name__ == "__main__":
    main()
