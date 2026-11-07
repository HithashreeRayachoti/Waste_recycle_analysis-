"""Run the silver and gold SQL in DuckDB, with data-quality checks between layers."""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import duckdb

from nyrt.config import Settings

log = logging.getLogger(__name__)

SEEDS = {
    "dim_district": "dim_district.csv",
    "dim_material": "dim_material.csv",
    "source_column_map": "source_column_map.csv",
    "recovery_rates": "ref_recovery_rates.csv",
    "district_population": "district_population.csv",
}

SEED_TYPES = {
    "dim_district": {"district_number": "INTEGER", "borough_code": "INTEGER", "boro_cd": "VARCHAR"},
    "dim_material": {"is_diverted": "BOOLEAN", "is_recyclable": "BOOLEAN"},
    "recovery_rates": {
        "recovery_low": "DOUBLE",
        "recovery_base": "DOUBLE",
        "recovery_high": "DOUBLE",
        "verified": "BOOLEAN",
        "source_year": "INTEGER",
    },
    "district_population": {
        "district_id": "VARCHAR",
        "population": "BIGINT",
        "population_year": "INTEGER",
        "source_url": "VARCHAR",
    },
}


class CheckFailed(RuntimeError):
    pass


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(settings.warehouse_path))


def load_seeds(con: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    con.execute("create schema if not exists ref")
    for table, filename in SEEDS.items():
        path = settings.seeds_dir / filename
        types = SEED_TYPES.get(table)
        type_arg = f", types={types!r}" if types else ""
        con.execute(
            f"create or replace table ref.{table} as select * from read_csv(?, header=true{type_arg})",
            [str(path)],
        )
    validate_recovery_rates(con)


def validate_recovery_rates(con: duckdb.DuckDBPyConnection) -> None:
    bad = con.execute(
        """select material from ref.recovery_rates
           where recovery_base is not null and not (
               recovery_low <= recovery_base and recovery_base <= recovery_high
               and recovery_low >= 0 and recovery_high <= 1
               and source_url is not null and rate_version is not null)"""
    ).fetchall()
    if bad:
        raise CheckFailed(f"ref_recovery_rates: invalid row(s) for {[b[0] for b in bad]}")


def _statements(sql: str) -> list[tuple[str | None, str]]:
    """Split a check file into (name, query) pairs using '-- name:' markers."""
    out, name, buf = [], None, []
    for line in sql.splitlines():
        m = re.match(r"--\s*name:\s*(\S+)", line)
        if m:
            if name and "".join(buf).strip():
                out.append((name, "\n".join(buf)))
            name, buf = m.group(1), []
        else:
            buf.append(line)
    if name and "".join(buf).strip():
        out.append((name, "\n".join(buf)))
    return out


def run_checks(con: duckdb.DuckDBPyConnection, path: Path) -> dict[str, int]:
    results = {}
    for name, query in _statements(path.read_text()):
        q = query.strip().rstrip(";")
        if not q:
            continue
        n = con.execute(f"select count(*) from (\n{q}\n)").fetchone()[0]
        results[name] = n
        log.info("check %s: %s violating rows", name, n)
    failed = {k: v for k, v in results.items() if v}
    if failed:
        raise CheckFailed(f"{path.name}: checks failed {failed}")
    return results


def _set_run_months(con: duckdb.DuckDBPyConnection, months: list[date]) -> None:
    con.execute("create or replace temp table run_months (month date)")
    con.executemany("insert into run_months values (?)", [(m,) for m in months])


def available_bronze_months(settings: Settings) -> list[date]:
    root = settings.bronze_dir / "source_extract"
    months = []
    for p in sorted(root.glob("month=*")):
        y, m = p.name.split("=")[1].split("-")
        months.append(date(int(y), int(m), 1))
    return months


def run_silver(con: duckdb.DuckDBPyConnection, settings: Settings, months: list[date]) -> dict:
    if not months:
        raise ValueError("silver: no months to process")
    files = [
        str(settings.bronze_dir / "source_extract" / f"month={m:%Y-%m}" / "part-0.parquet") for m in months
    ]
    missing = [f for f in files if not Path(f).exists()]
    if missing:
        raise FileNotFoundError(f"silver: bronze partitions missing: {missing}")
    con.execute(
        "create or replace temp view bronze_extract as "
        f"select * from read_parquet({files!r}, hive_partitioning = false)"
    )
    _set_run_months(con, months)
    con.execute((settings.sql_dir / "silver" / "tonnage.sql").read_text())
    results = run_checks(con, settings.sql_dir / "checks" / "silver.sql")
    flags = dict(
        con.execute(
            "select quality_flag, count(*) from silver.tonnage "
            "where month in (select month from run_months) group by 1"
        ).fetchall()
    )
    return {"months": len(months), "checks": results, "flags": flags}


def run_gold(con: duckdb.DuckDBPyConnection, settings: Settings) -> dict:
    sql = (settings.sql_dir / "gold" / "model.sql").read_text()
    sql = sql.replace(":allow_unverified", "true" if settings.allow_unverified_rates else "false")
    con.execute(sql)
    results = run_checks(con, settings.sql_dir / "checks" / "gold.sql")
    rates = con.execute("select material, verified from gold.ref_recovery_rates_used").fetchall()
    return {"checks": results, "recovery_rates_used": rates}
