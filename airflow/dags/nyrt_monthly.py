"""Monthly DAG: ingest -> bronze extract -> silver -> gold -> export.

Each task calls the same functions as the CLI. The first run (or a manual trigger with
{"backfill": true}) loads the full history; scheduled runs re-pull the latest three months,
in case DSNY revises recent figures. Every layer runs its checks and fails the task on a
violation, so a bad month never reaches gold.

Azure Data Factory mapping: docs/orchestration.md.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    dag_id="nyrt_monthly",
    schedule="0 6 5 * *",  # 06:00 on the 5th; DSNY posts the previous month early in the month
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    params={"backfill": False},
    tags=["nyrt", "dsny"],
)
def nyrt_monthly():
    @task
    def init_db() -> None:
        from nyrt import pipeline
        from nyrt.config import get_settings

        pipeline.init_db(get_settings())

    @task
    def ingest() -> list[str] | None:
        from nyrt import pipeline
        from nyrt.config import get_settings

        backfill = bool(get_current_context()["params"].get("backfill"))
        result = pipeline.ingest(get_settings(), backfill=backfill)
        return None if result["months"] == "all" else result["months"]

    @task
    def bronze_extract(months: list[str] | None) -> list[str]:
        from nyrt import pipeline
        from nyrt.config import get_settings

        ms = [date.fromisoformat(m) for m in months] if months else None
        return [m.isoformat() for m in pipeline.bronze_extract(get_settings(), ms)]

    @task
    def silver_and_gold(months: list[str]) -> dict:
        from nyrt import pipeline
        from nyrt.config import get_settings

        return pipeline.transform(get_settings(), [date.fromisoformat(m) for m in months])

    @task
    def export() -> list[str]:
        from nyrt import pipeline
        from nyrt.config import get_settings

        return [str(p) for p in pipeline.export_dashboard(get_settings())]

    months = ingest()
    init_db() >> months
    silver_and_gold(bronze_extract(months)) >> export()


nyrt_monthly()
