"""Command line entry point: `nyrt <step>`."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path

from nyrt import pipeline
from nyrt.config import get_settings


def _months(values: list[str] | None) -> list[date] | None:
    return [date.fromisoformat(v + "-01" if len(v) == 7 else v) for v in values] if values else None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="nyrt", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="create the Postgres source schema")
    p = sub.add_parser("ingest", help="pull DSNY data, land bronze, upsert into Postgres")
    p.add_argument("--from-file", type=Path, help="use a CSV exported from NYC Open Data")
    p.add_argument("--incremental", action="store_true", help="only write the latest 3 months")
    p = sub.add_parser("extract", help="bronze extract of Postgres, partitioned by month")
    p.add_argument("--month", action="append", help="YYYY-MM (repeatable); default all")
    p = sub.add_parser("transform", help="silver + gold in DuckDB, with checks")
    p.add_argument("--month", action="append", help="YYYY-MM (repeatable); default all in bronze")
    sub.add_parser("export", help="write dashboard JSON from gold")
    p = sub.add_parser("run", help="every step end to end")
    p.add_argument("--from-file", type=Path)
    p.add_argument("--incremental", action="store_true")
    p = sub.add_parser("profile", help="profile the DSNY data (roadmap step 1)")
    p.add_argument("--from-file", type=Path)
    p.add_argument("--out", type=Path, default=Path("docs/data_profile.md"))
    p = sub.add_parser("serve", help="run the ingestion service")
    p.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    if args.cmd == "init-db":
        pipeline.init_db(s)
        result = {"ok": True}
    elif args.cmd == "ingest":
        result = pipeline.ingest(s, args.from_file, backfill=not args.incremental)
    elif args.cmd == "extract":
        result = [m.isoformat() for m in pipeline.bronze_extract(s, _months(args.month))]
    elif args.cmd == "transform":
        result = pipeline.transform(s, _months(args.month))
    elif args.cmd == "export":
        result = [str(p) for p in pipeline.export_dashboard(s)]
    elif args.cmd == "run":
        result = pipeline.run_all(s, backfill=not args.incremental, from_file=args.from_file)
    elif args.cmd == "profile":
        from nyrt.ingest import profile

        result = profile.run(s, args.from_file, args.out)
    elif args.cmd == "serve":
        import uvicorn

        uvicorn.run("nyrt.source.service:app", host="0.0.0.0", port=args.port)
        return
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
