"""Export gold extracts as JSON for the static dashboard."""

from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from nyrt.analytics import seasonality


def _clean(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, (date, datetime)):
        return v.isoformat()[:10] if isinstance(v, date) and not isinstance(v, datetime) else v.isoformat()
    if hasattr(v, "item"):
        return _clean(v.item())
    return v


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [{c: _clean(v) for c, v in zip(cols, row, strict=True)} for row in cur.fetchall()]


def build(con: duckdb.DuckDBPyConnection, extract: dict | None = None) -> dict[str, object]:
    window = _rows(
        con,
        "select min(month) as first_month, max(month) as last_month, "
        "count(distinct month) as months from gold.fact_tonnage",
    )[0]
    ranking_month = _rows(con, "select month from gold.ranking_month")[0]["month"]
    rates = _rows(
        con,
        "select material, rate_version, recovery_low, recovery_base, "
        "recovery_high, range_method, source_title, source_url, source_year, "
        "verified, notes from ref.recovery_rates order by material",
    )
    used = {r["material"] for r in _rows(con, "select material from gold.ref_recovery_rates_used")}
    for r in rates:
        r["used_in_estimates"] = r["material"] in used
    flags = _rows(
        con,
        "select quality_flag, material, count(*) as n from gold.fact_tonnage group by all order by 1, 2",
    )
    return {
        "summary": {
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "data_window": window,
            "ranking_month": ranking_month,
            "headline": (_rows(con, "select * from gold.diversion_headline") or [None])[0],
            "recovery_rates": rates,
            "quality_flags": flags,
            "source": "DSNY Monthly Tonnage Data, NYC Open Data (ebb7-mvp5)",
            # Where this run's bronze came from: the live API (query.url) or a file.
            "extract": extract,
        },
        "ranking": _rows(con, "select * from gold.district_rank order by city_rank"),
        "ranking_exclusions": _rows(con, "select * from gold.ranking_exclusions order by 1"),
        "borough_trend": _rows(
            con,
            "select borough, month, diversion_rate, recycling_rate_core, "
            "total_tons, districts_complete from gold.borough_month "
            "order by borough, month",
        ),
        "district_trend": _rows(
            con,
            "select district_id, month, rolling_diversion_rate, "
            "rolling_recycling_rate_core, yoy_change_diversion, "
            "yoy_change_core from gold.district_rate_trend "
            "where rolling_diversion_rate is not null "
            "order by district_id, month",
        ),
        "district_month": _rows(
            con,
            "select district_id, borough, district_name, month, "
            "refuse_tons, paper_tons, mgp_tons, organics_tons, "
            "diversion_rate, recycling_rate_core, is_complete, issues "
            "from gold.district_month order by district_id, month",
        ),
        "contamination": _rows(con, "select * from gold.contamination_by_material order by year, material"),
        "seasonal_index": _rows(con, "select * from gold.seasonal_index"),
        "seasonality_test": seasonality.run(con),
        "diversion_gap": _rows(con, "select * from gold.diversion_gap order by gap_tons desc"),
    }


def latest_manifest(bronze_dir: Path) -> dict | None:
    manifests = sorted((bronze_dir / "raw_api").glob("extracted_at=*/_manifest.json"))
    if not manifests:
        return None
    m = json.loads(manifests[-1].read_text())
    return {"extracted_at": m.get("extracted_at"), "query": m.get("query"), "row_count": m.get("row_count")}


def write(con: duckdb.DuckDBPyConnection, out_dir: Path, bronze_dir: Path | None = None) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    extract = latest_manifest(bronze_dir) if bronze_dir else None
    for name, payload in build(con, extract).items():
        p = out_dir / f"{name}.json"
        p.write_text(json.dumps(payload, indent=1, default=str))
        paths.append(p)
    return paths
