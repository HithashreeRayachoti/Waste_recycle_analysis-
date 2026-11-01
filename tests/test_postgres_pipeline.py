"""End to end through a real Postgres: ingest -> bronze extract -> silver -> gold -> export."""

from datetime import date

import psycopg
import pytest
import synthetic_dsny
from fastapi.testclient import TestClient

from nyrt import pipeline
from nyrt.source import repository


@pytest.fixture
def csv_file(tmp_path):
    return synthetic_dsny.write_csv(tmp_path / "dsny.csv", synthetic_dsny.records())


def test_full_run_from_csv(pg_settings, csv_file, tmp_path, monkeypatch):
    monkeypatch.setattr(type(pg_settings), "export_dir", property(lambda s: tmp_path / "export"))
    result = pipeline.run_all(pg_settings, backfill=True, from_file=csv_file)
    assert result["ingest"]["rows_upserted"] == 59 * 30 * 5
    assert result["bronze_months"] == 30
    assert (tmp_path / "export" / "ranking.json").exists()


def test_rerun_is_idempotent_and_revisions_are_tracked(pg_settings, csv_file):
    pipeline.init_db(pg_settings)
    pipeline.ingest(pg_settings, from_file=csv_file)
    recs = synthetic_dsny.records()
    recs[-1]["papertonscollected"] = 1.0  # DSNY revises the latest month
    with psycopg.connect(pg_settings.pg_dsn) as conn:
        res = repository.upsert_records(conn, recs, "test", months={date(2023, 6, 1)})
        assert res["rows_upserted"] == 59 * 5
        assert res["rows_revised"] == 1
        n, max_rev = conn.execute("select count(*), max(revision) from ops.dsny_collection").fetchone()
    assert n == 59 * 30 * 5
    assert max_rev == 2


def test_incremental_ingest_writes_latest_three_months(pg_settings, csv_file):
    pipeline.init_db(pg_settings)
    res = pipeline.ingest(pg_settings, from_file=csv_file, backfill=False)
    assert res["months"] == ["2023-04-01", "2023-05-01", "2023-06-01"]
    assert res["rows_upserted"] == 59 * 3 * 5


def test_ingestion_service(pg_settings, monkeypatch):
    from nyrt.source import service

    monkeypatch.setattr(service, "get_settings", lambda: pg_settings)
    pipeline.init_db(pg_settings)
    client = TestClient(service.app)
    assert client.get("/healthz").json() == {"ok": True}
    recs = synthetic_dsny.records(months=1)
    r = client.post("/v1/records", json={"source": "test", "records": recs})
    assert r.status_code == 200 and r.json()["rows_upserted"] == 59 * 5
    bad = client.post("/v1/records", json={"source": "t", "records": [{"month": "x"}]})
    assert bad.status_code == 422
