import csv
from collections import Counter

from nyrt import dsny
from nyrt.config import REPO_ROOT

SEEDS = REPO_ROOT / "seeds"


def read(name):
    with (SEEDS / name).open() as f:
        return list(csv.DictReader(f))


def test_dim_district_has_the_59_community_districts():
    rows = read("dim_district.csv")
    assert len(rows) == 59
    assert len({r["district_id"] for r in rows}) == 59
    assert Counter(r["borough"] for r in rows) == {
        "Manhattan": 12,
        "Bronx": 12,
        "Brooklyn": 18,
        "Queens": 14,
        "Staten Island": 3,
    }


def test_source_column_map_matches_code():
    rows = read("source_column_map.csv")
    assert {r["source_column"]: r["material"] for r in rows} == dsny.MATERIAL_COLUMNS


def test_recovery_rates_are_cited_and_ordered():
    for r in read("ref_recovery_rates.csv"):
        assert r["source_url"].startswith("https://"), r
        assert r["rate_version"]
        if r["recovery_base"]:
            lo, base, hi = (
                float(r["recovery_low"]),
                float(r["recovery_base"]),
                float(r["recovery_high"]),
            )
            assert 0 <= lo <= base <= hi <= 1
            assert r["range_method"]
