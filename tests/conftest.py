import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "fixtures"))


@pytest.fixture
def settings(tmp_path, monkeypatch):
    from nyrt.config import Settings

    return Settings(data_dir=tmp_path / "data", pg_dsn=os.getenv("NYRT_PG_DSN", ""))


@pytest.fixture
def pg_settings(tmp_path):
    """A fresh Postgres schema per test. Skips unless NYRT_PG_DSN points at a server."""
    dsn = os.getenv("NYRT_PG_DSN")
    if not dsn:
        pytest.skip("set NYRT_PG_DSN to run Postgres tests")
    import psycopg

    from nyrt.config import Settings

    db = f"nyrt_t_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"create database {db}")
    test_dsn = dsn.rsplit("/", 1)[0] + "/" + db
    yield Settings(data_dir=tmp_path / "data", pg_dsn=test_dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"drop database {db} with (force)")
