"""Roadmap step 1: profile the real DSNY data and confirm (or refute) the known quirks."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

from nyrt import dsny
from nyrt.config import Settings
from nyrt.ingest import socrata


def run(settings: Settings, from_file: Path | None, out: Path) -> dict:
    rows, query = socrata.read_csv_export(from_file) if from_file else socrata.fetch_all(settings)
    rows = socrata.normalize_keys(rows)
    columns = sorted({c for r in rows for c in r})
    missing, unknown = dsny.check_schema(set(columns))
    month_labels = Counter(r.get("month") for r in rows)
    bad_months = [m for m in month_labels if m is not None and _try(dsny.parse_month, m) is None]
    months = sorted(m for m in (_try(dsny.parse_month, x) for x in month_labels) if m)
    boroughs = Counter(r.get("borough") for r in rows)
    districts = Counter((r.get("borough"), r.get("communitydistrict")) for r in rows)
    per_col = {}
    first_nonzero = {}
    for col in dsny.MATERIAL_COLUMNS:
        if col not in columns:
            continue
        vals = [(r.get("month"), r.get(col)) for r in rows]
        parsed = [(m, _try(dsny.parse_tons, v)) for m, v in vals]
        per_col[col] = {
            "null": sum(1 for _, v in parsed if v is None),
            "zero": sum(1 for _, v in parsed if v == 0),
            "negative": sum(1 for _, v in parsed if v is not None and v < 0),
        }
        nz = [_try(dsny.parse_month, m) for m, v in parsed if v]
        nz = [m for m in nz if m]
        first_nonzero[col] = min(nz).isoformat() if nz else None
    dup = Counter((r.get("borough"), r.get("communitydistrict"), r.get("month")) for r in rows)
    dups = sum(1 for v in dup.values() if v > 1)
    per_month = defaultdict(int)
    for r in rows:
        m = _try(dsny.parse_month, r.get("month"))
        if m:
            per_month[m] += 1
    short_months = {m.isoformat(): n for m, n in per_month.items() if n != 59}

    lines = [
        "# DSNY Monthly Tonnage: data profile",
        "",
        f"Source: {query}",
        f"Rows: {len(rows)}; months: {months[0] if months else None} to "
        f"{months[-1] if months else None} ({len(months)})",
        "",
        f"Columns: {', '.join(columns)}",
        f"Missing required columns: {missing or 'none'}",
        f"Unmapped columns: {unknown or 'none'}",
        f"Example month labels: {list(month_labels)[:5]}; unparseable: {bad_months[:10] or 'none'}",
        f"Boroughs: {dict(boroughs)}",
        f"Distinct borough x district pairs: {len(districts)} (expect 59)",
        f"Duplicate borough x district x month rows: {dups}",
        f"Months without exactly 59 rows: {len(short_months)} {dict(list(short_months.items())[:12])}",
        "",
        "| column | null | zero | negative | first non-zero month |",
        "|---|---|---|---|---|",
    ]
    for col, s in per_col.items():
        lines.append(f"| {col} | {s['null']} | {s['zero']} | {s['negative']} | {first_nonzero[col]} |")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n")
    return {
        "written": str(out),
        "rows": len(rows),
        "missing_columns": missing,
        "unmapped_columns": unknown,
        "district_pairs": len(districts),
    }


def _try(fn, v):
    try:
        return fn(v)
    except (ValueError, TypeError):
        return None
