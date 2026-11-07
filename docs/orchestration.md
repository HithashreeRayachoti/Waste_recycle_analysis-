# Orchestration: Airflow, and the Azure Data Factory equivalent

The pipeline is five idempotent steps. Airflow runs them locally (`airflow/dags/nyrt_monthly.py`);
the same steps map one to one onto Azure Data Factory (or Fabric Data Factory) activities.

| Step | Airflow task | What it does | Azure Data Factory equivalent |
|---|---|---|---|
| 1 | `init_db` | Create the Postgres source schema if missing | Script activity against Azure Database for PostgreSQL |
| 2 | `ingest` | Pull the Socrata API, land the raw response in bronze, upsert into Postgres through the ingestion service | Copy activity (REST source, ADLS Gen2 sink) for bronze, then a Web activity calling the ingestion service |
| 3 | `bronze_extract` | Postgres to month-partitioned Parquet | Copy activity (PostgreSQL source, Parquet sink partitioned by month) |
| 4 | `silver_and_gold` | DuckDB SQL with checks between layers | Databricks Notebook activity running `spark/silver_spark.py`, then a Script activity for gold (Synapse serverless SQL) |
| 5 | `export` | JSON extracts for the dashboard | Copy activity to the `$web` container of a static-website storage account |

Schedule: 06:00 on the 5th of each month (schedule trigger in ADF). Scheduled runs re-pull the
latest three months; a manual run with `backfill = true` (an ADF pipeline parameter) loads
the full history. Retries: 2, ten minutes apart. A failed data-quality check fails the
activity, so later activities do not run and gold keeps the previous good months.
