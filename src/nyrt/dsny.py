"""What we expect from the DSNY Monthly Tonnage dataset, and helpers to read it.

The column names below are the Socrata field names as best known when this was written
(they are not confirmed against the live API yet; `nyrt profile` checks them). They are
kept in one place so step 1 of the roadmap only has to change this file.
"""

from __future__ import annotations

import re
from datetime import date

# Field names that must be present for the pipeline to run.
REQUIRED_COLUMNS = frozenset(
    {
        "month",
        "borough",
        "communitydistrict",
        "refusetonscollected",
        "papertonscollected",
        "mgptonscollected",
    }
)

# Every tonnage column we know how to map, and the material it rolls up to.
# Organics arrive as several columns that start at different points in the history.
MATERIAL_COLUMNS: dict[str, str] = {
    "refusetonscollected": "refuse",
    "papertonscollected": "paper",
    "mgptonscollected": "mgp",
    "resorganicstons": "organics",
    "schoolorganictons": "organics",
    "leavesorganictons": "organics",
    "xmastreetons": "organics",
    "otherorganicstons": "organics",
}

# Alternative spellings seen in older copies of the dataset (e.g. CSV exports, R packages).
COLUMN_ALIASES: dict[str, str] = {
    "refuse_tons_collected": "refusetonscollected",
    "paper_tons_collected": "papertonscollected",
    "mgp_tons_collected": "mgptonscollected",
    "mpg_tons_collected": "mgptonscollected",
    "community_district": "communitydistrict",
}

ID_COLUMNS = frozenset({"month", "borough", "communitydistrict", "borough_id"})

BOROUGH_ALIASES = {
    "manhattan": "Manhattan",
    "bronx": "Bronx",
    "the bronx": "Bronx",
    "brooklyn": "Brooklyn",
    "queens": "Queens",
    "staten island": "Staten Island",
    "statenisland": "Staten Island",
}

_MONTH_PATTERNS = [
    re.compile(r"^\s*(\d{4})\s*/\s*(\d{1,2})\s*$"),  # "2019 / 01"
    re.compile(r"^\s*(\d{4})-(\d{1,2})(?:-\d{1,2})?(?:T.*)?\s*$"),  # "2019-01", ISO timestamps
    re.compile(r"^\s*(\d{1,2})/(?:\d{1,2}/)?(\d{4})\s*$"),  # "01/2019", "01/01/2019"
]


def normalize_column(name: str) -> str:
    """Lowercase a header and strip spaces so CSV display names match API field names."""
    key = re.sub(r"\s+", "", name.strip().lower())
    return COLUMN_ALIASES.get(key, key)


def parse_month(value: str) -> date:
    """Parse DSNY's month label into the first day of that month."""
    if value is None:
        raise ValueError("month is missing")
    text = str(value)
    for i, pattern in enumerate(_MONTH_PATTERNS):
        m = pattern.match(text)
        if m:
            if i == 2:
                month, year = int(m.group(1)), int(m.group(2))
            else:
                year, month = int(m.group(1)), int(m.group(2))
            return date(year, month, 1)
    raise ValueError(f"unrecognised month label: {value!r}")


def normalize_borough(value: str) -> str:
    key = re.sub(r"\s+", " ", str(value).strip().lower())
    if key not in BOROUGH_ALIASES:
        raise ValueError(f"unrecognised borough: {value!r}")
    return BOROUGH_ALIASES[key]


def normalize_district_number(value: str) -> str:
    """'1', '01', '01 ' -> '01'. Anything non-numeric is returned stripped, for silver to flag."""
    text = str(value).strip()
    return f"{int(text):02d}" if text.isdigit() else text


def parse_tons(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text == "" or text.lower() in {"nan", "null", "none"}:
        return None
    return float(text)


def check_schema(columns: set[str]) -> tuple[list[str], list[str]]:
    """Return (missing required columns, unknown columns we would ignore)."""
    missing = sorted(REQUIRED_COLUMNS - columns)
    unknown = sorted(c for c in columns if c not in MATERIAL_COLUMNS and c not in ID_COLUMNS)
    return missing, unknown
