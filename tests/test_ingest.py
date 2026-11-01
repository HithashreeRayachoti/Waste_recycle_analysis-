import pytest
import synthetic_dsny

from nyrt.ingest import socrata
from nyrt.source import repository


def test_unpivot_one_row_per_material_column():
    rec = synthetic_dsny.records(months=1)[0]
    rows = repository.unpivot(rec)
    assert {r.source_column for r in rows} == {
        "refusetonscollected",
        "papertonscollected",
        "mgptonscollected",
        "resorganicstons",
        "leavesorganictons",
    }
    assert all(r.month.isoformat() == "2021-01-01" for r in rows)


def test_unpivot_rejects_bad_records():
    with pytest.raises(repository.RecordError):
        repository.unpivot({"borough": "Bronx", "communitydistrict": "01"})
    with pytest.raises(repository.RecordError):
        repository.unpivot(
            {
                "month": "2021 / 01",
                "borough": "Bronx",
                "communitydistrict": "01",
                "papertonscollected": "abc",
            }
        )


def test_bronze_checks():
    with pytest.raises(socrata.BronzeCheckError, match="zero rows"):
        socrata.check_bronze([])
    with pytest.raises(socrata.BronzeCheckError, match="missing columns"):
        socrata.check_bronze([{"month": "2021 / 01"}])


def test_land_writes_raw_response_and_manifest(settings):
    rows = synthetic_dsny.records(months=1)
    landing = socrata.land(settings, rows, {"test": True})
    assert (landing.path / "response.json").exists()
    manifest = (landing.path / "_manifest.json").read_text()
    assert '"row_count": 59' in manifest


def test_profile_from_csv(tmp_path, settings):
    from nyrt.ingest import profile

    csv_path = synthetic_dsny.write_csv(tmp_path / "d.csv", synthetic_dsny.records(months=13))
    out = profile.run(settings, csv_path, tmp_path / "profile.md")
    assert out["missing_columns"] == [] and out["district_pairs"] == 59
    text = (tmp_path / "profile.md").read_text()
    assert "| resorganicstons |" in text and "2022-01-01" in text
