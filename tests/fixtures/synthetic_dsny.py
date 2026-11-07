"""SYNTHETIC test fixture shaped like DSNY Monthly Tonnage rows.

These numbers are generated, not real. They exist only to exercise the pipeline and its
checks in tests and CI. Nothing derived from them is a finding.
"""

from __future__ import annotations

import csv
import math
import random
from pathlib import Path

BOROUGHS = [
    ("Manhattan", 12),
    ("Bronx", 12),
    ("Brooklyn", 18),
    ("Queens", 14),
    ("Staten Island", 3),
]


def records(start_year: int = 2021, months: int = 30, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for i in range(months):
        year, month = start_year + i // 12, i % 12 + 1
        season = 1 + 0.08 * math.cos(2 * math.pi * (month - 1) / 12)
        for borough, n in BOROUGHS:
            for cd in range(1, n + 1):
                base = rng.uniform(1500, 6000)
                share = 0.12 + 0.12 * (cd / n)
                row = {
                    "month": f"{year} / {month:02d}",
                    "borough": borough,
                    "communitydistrict": f"{cd:02d}",
                    "refusetonscollected": round(base, 1),
                    "papertonscollected": round(base * share * 0.45 * season, 1),
                    "mgptonscollected": round(base * share * 0.55 * season, 1),
                    # organics only reported from month 13 onward, like a programme rollout
                    "resorganicstons": round(base * 0.02, 1) if i >= 12 else "",
                    "leavesorganictons": "",
                }
                out.append(row)
    # Injected quirks the checks must catch and flag:
    for idx, col, val in [
        (5, "papertonscollected", ""),  # missing value
        (70, "mgptonscollected", 0),  # zero
        (130, "refusetonscollected", -4.0),
    ]:  # negative
        if idx < len(out):
            out[idx][col] = val
    return out


def write_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    # CSV exports use display-style headers; exercise the header normalization too.
    headers = {
        "month": "Month",
        "borough": "Borough",
        "communitydistrict": "CommunityDistrict",
        "refusetonscollected": "RefuseTonsCollected",
        "papertonscollected": "PaperTonsCollected",
        "mgptonscollected": "MGPTonsCollected",
        "resorganicstons": "ResOrganicsTons",
        "leavesorganictons": "LeavesOrganicTons",
    }
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(headers.values()))
        w.writeheader()
        for r in rows:
            w.writerow({headers[k]: v for k, v in r.items()})
    return path
