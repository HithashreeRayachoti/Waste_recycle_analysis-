from datetime import date

import pytest

from nyrt import dsny


@pytest.mark.parametrize(
    "label", ["2019 / 01", "2019/1", "2019-01", "2019-01-01T00:00:00.000", "01/2019", "1/1/2019"]
)
def test_parse_month(label):
    assert dsny.parse_month(label) == date(2019, 1, 1)


def test_parse_month_rejects_garbage():
    with pytest.raises(ValueError):
        dsny.parse_month("Jan")


def test_normalize_column_handles_display_names_and_aliases():
    assert dsny.normalize_column("Refuse Tons Collected") == "refusetonscollected"
    assert dsny.normalize_column("MGPTonsCollected") == "mgptonscollected"
    assert dsny.normalize_column("mpg_tons_collected") == "mgptonscollected"


def test_borough_and_district():
    assert dsny.normalize_borough(" staten  island ") == "Staten Island"
    assert dsny.normalize_district_number("1") == "01"
    assert dsny.normalize_district_number("01 ") == "01"
    with pytest.raises(ValueError):
        dsny.normalize_borough("Long Island")


def test_parse_tons():
    assert dsny.parse_tons("1,234.5") == 1234.5
    assert dsny.parse_tons("") is None
    assert dsny.parse_tons(None) is None


def test_check_schema():
    missing, unknown = dsny.check_schema({"month", "borough", "newcolumn"})
    assert "communitydistrict" in missing
    assert unknown == ["newcolumn"]
