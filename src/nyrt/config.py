"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# DSNY Monthly Tonnage Data on NYC Open Data (Socrata).
DSNY_DATASET_ID = "ebb7-mvp5"
SOCRATA_DOMAIN = "data.cityofnewyork.us"


@dataclass(frozen=True)
class Settings:
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("NYRT_DATA_DIR", "./data")))
    pg_dsn: str = field(
        default_factory=lambda: os.getenv("NYRT_PG_DSN", "postgresql://nyrt:nyrt@localhost:5432/nyrt")
    )
    socrata_app_token: str | None = field(default_factory=lambda: os.getenv("NYRT_SOCRATA_APP_TOKEN") or None)
    ingest_url: str | None = field(default_factory=lambda: os.getenv("NYRT_INGEST_URL") or None)
    allow_unverified_rates: bool = field(
        default_factory=lambda: os.getenv("NYRT_ALLOW_UNVERIFIED_RATES", "0") == "1"
    )
    seeds_dir: Path = REPO_ROOT / "seeds"
    sql_dir: Path = REPO_ROOT / "sql"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def export_dir(self) -> Path:
        return REPO_ROOT / "dashboard" / "data"


def get_settings() -> Settings:
    return Settings()
