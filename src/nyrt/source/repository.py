"""Write path into the Postgres operational store: validate, unpivot, upsert."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import psycopg

from nyrt import dsny
from nyrt.config import REPO_ROOT


@dataclass(frozen=True)
class CollectionRow:
    borough: str
    community_district: str
    month: date
    source_column: str
    raw_month: str
    tons: float | None


class RecordError(ValueError):
    pass


def unpivot(record: dict) -> list[CollectionRow]:
    """One published DSNY row (district x month, a column per material) -> one row per column."""
    rec = {dsny.normalize_column(k): v for k, v in record.items()}
    try:
        raw_month = str(rec["month"])
        month = dsny.parse_month(raw_month)
        borough = dsny.normalize_borough(rec["borough"])
        district = dsny.normalize_district_number(rec["communitydistrict"])
    except KeyError as e:
        raise RecordError(f"record is missing {e}") from e
    except ValueError as e:
        raise RecordError(str(e)) from e
    rows = []
    for column in dsny.MATERIAL_COLUMNS:
        if column in rec:
            try:
                tons = dsny.parse_tons(rec[column])
            except ValueError as e:
                raise RecordError(f"{column}: not a number: {rec[column]!r}") from e
            rows.append(CollectionRow(borough, district, month, column, raw_month, tons))
    return rows


def init_schema(conn: psycopg.Connection) -> None:
    conn.execute((REPO_ROOT / "sql" / "source" / "schema.sql").read_text())
    conn.commit()


UPSERT = """
insert into ops.dsny_collection
    (borough, community_district, month, source_column, raw_month, tons, load_id)
values (%s, %s, %s, %s, %s, %s, %s)
on conflict (borough, community_district, month, source_column) do update set
    raw_month       = excluded.raw_month,
    tons            = excluded.tons,
    load_id         = excluded.load_id,
    last_loaded_at  = now(),
    revision        = ops.dsny_collection.revision
                      + case when ops.dsny_collection.tons is distinct from excluded.tons
                             then 1 else 0 end,
    last_changed_at = case when ops.dsny_collection.tons is distinct from excluded.tons
                           then now() else ops.dsny_collection.last_changed_at end
returning (xmax = 0) as inserted, revision, last_changed_at = now() as changed
"""


def upsert_records(
    conn: psycopg.Connection, records: list[dict], source: str, months: set[date] | None = None
) -> dict:
    """Upsert published records. If `months` is given, only those months are written
    (the incremental run re-pulls the latest three)."""
    rows: list[CollectionRow] = []
    for record in records:
        for row in unpivot(record):
            if months is None or row.month in months:
                rows.append(row)
    with conn.transaction():
        load_id = conn.execute(
            "insert into ops.load_log (source, records_in, rows_upserted, rows_changed)"
            " values (%s, %s, 0, 0) returning load_id",
            (source, len(records)),
        ).fetchone()[0]
        changed = 0
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    UPSERT,
                    (
                        r.borough,
                        r.community_district,
                        r.month,
                        r.source_column,
                        r.raw_month,
                        r.tons,
                        load_id,
                    ),
                )
                inserted, _revision, was_changed = cur.fetchone()
                if was_changed and not inserted:
                    changed += 1
        conn.execute(
            "update ops.load_log set rows_upserted = %s, rows_changed = %s where load_id = %s",
            (len(rows), changed, load_id),
        )
    return {
        "load_id": load_id,
        "records_in": len(records),
        "rows_upserted": len(rows),
        "rows_revised": changed,
    }
