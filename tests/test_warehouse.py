"""Silver and gold on synthetic bronze partitions (no Postgres needed)."""

from dataclasses import replace
from datetime import UTC, datetime

import pandas as pd
import pytest
import synthetic_dsny

from nyrt.analytics import export
from nyrt.source import repository
from nyrt.transform import warehouse


def write_bronze(settings, records):
    rows = [r.__dict__ for rec in records for r in repository.unpivot(rec)]
    df = pd.DataFrame(rows)
    df["revision"] = 1
    df["source_loaded_at"] = datetime(2026, 1, 1, tzinfo=UTC)
    for month, part in df.groupby("month"):
        out = settings.bronze_dir / "source_extract" / f"month={month:%Y-%m}"
        out.mkdir(parents=True, exist_ok=True)
        part.to_parquet(out / "part-0.parquet", index=False)


@pytest.fixture
def built(settings):
    write_bronze(settings, synthetic_dsny.records())
    con = warehouse.connect(settings)
    warehouse.load_seeds(con, settings)
    months = warehouse.available_bronze_months(settings)
    silver = warehouse.run_silver(con, settings, months)
    gold = warehouse.run_gold(con, settings)
    yield con, silver, gold
    con.close()


def test_silver_flags_injected_quirks(built):
    con, silver, _ = built
    assert silver["flags"]["missing"] == 1
    assert silver["flags"]["zero"] == 1
    assert silver["flags"]["negative"] == 1
    # organics not reported for the first 12 months: 59 districts x 12
    assert silver["flags"]["not_reported"] == 59 * 12
    assert all(v == 0 for v in silver["checks"].values())


def test_silver_is_idempotent(built, settings):
    con, _, _ = built
    before = con.execute("select count(*), round(sum(tons_collected), 3) from silver.tonnage").fetchone()
    months = warehouse.available_bronze_months(settings)
    warehouse.run_silver(con, settings, months[-3:])
    after = con.execute("select count(*), round(sum(tons_collected), 3) from silver.tonnage").fetchone()
    assert before == after


def test_unknown_district_fails_the_silver_check(settings):
    recs = synthetic_dsny.records(months=1)
    recs[0]["communitydistrict"] = "99"
    write_bronze(settings, recs)
    con = warehouse.connect(settings)
    warehouse.load_seeds(con, settings)
    with pytest.raises(warehouse.CheckFailed, match="silver_every_district_maps"):
        warehouse.run_silver(con, settings, warehouse.available_bronze_months(settings))


def test_borough_totals_are_a_rollup_of_districts(built):
    con, _, _ = built
    diff = con.execute("""
        select max(abs(b.total_tons - d.total_tons)) from gold.borough_month b
        join (select borough, month, sum(total_tons) as total_tons from gold.district_month
              where is_complete group by all) d using (borough, month)""").fetchone()[0]
    assert diff < 1e-6


def test_incomplete_districts_are_excluded_from_ranking(built):
    con, _, _ = built
    ranked = {r[0] for r in con.execute("select district_id from gold.district_rank").fetchall()}
    excluded = {r[0] for r in con.execute("select district_id from gold.ranking_exclusions").fetchall()}
    assert ranked.isdisjoint(excluded)
    assert len(ranked) + len(excluded) == 59


def test_unverified_rates_produce_no_recovery_estimates(built):
    con, _, gold = built
    assert gold["recovery_rates_used"] == []
    assert con.execute("select count(tons_recovered_est) from gold.fact_tonnage").fetchone()[0] == 0
    head = con.execute(
        "select gap_tons_collected, gap_tons_recovered_base from gold.diversion_headline"
    ).fetchone()
    assert head[0] > 0 and head[1] is None


def test_allowing_unverified_rates_gives_a_range(built, settings):
    con, _, _ = built
    warehouse.run_gold(con, replace(settings, allow_unverified_rates=True))
    lo, base, hi = con.execute(
        "select contamination_low, contamination_base, contamination_high "
        "from gold.contamination_by_material where material = 'mgp' and year = 2022"
    ).fetchone()
    assert lo < base < hi
    # paper has no figure yet, so the blended (paper + MGP) range stays empty
    assert con.execute("select recovery_base from gold.blended_recovery").fetchone()[0] is None


def test_gap_is_non_negative_and_benchmark_is_top_quartile(built):
    con, _, _ = built
    rows = con.execute("select borough, rate, benchmark_rate, gap_tons from gold.diversion_gap").df()
    assert (rows.gap_tons >= 0).all()
    for _, g in rows.groupby("borough"):
        assert g.benchmark_rate.iloc[0] == pytest.approx(g.rate.quantile(0.75))


def test_export_builds_every_panel(built):
    con, _, _ = built
    out = export.build(con)
    assert out["ranking"] and out["borough_trend"] and out["seasonal_index"] is not None
    assert out["seasonality_test"]["recycling_rate_core"]["status"] in {"ok", "insufficient data"}
    assert out["summary"]["headline"]["gap_tons_collected"] > 0
