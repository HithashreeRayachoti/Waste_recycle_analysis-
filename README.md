# NY Recycling Tracker

A batch data pipeline and dashboard built on New York City's
[DSNY Monthly Tonnage Data](https://data.cityofnewyork.us/City-Government/DSNY-Monthly-Tonnage-Data/ebb7-mvp5).
It answers four questions for city sustainability teams:

1. **Ranking:** which community districts and boroughs divert the largest share of their waste, and who is improving or slipping?
2. **Contamination:** which materials are collected as recycling but lost to contamination? *(modeled, see below)*
3. **Seasonality:** do recycling rates dip at certain times of year?
4. **Diversion opportunity:** if every district matched a realistic benchmark, how many tons a year would leave the refuse stream?

> **Status.** The pipeline, checks, tests and dashboard are built and pass on a synthetic test
> fixture. They have **not yet run on the real DSNY data**. The build environment could not
> reach `data.cityofnewyork.us`. Nothing in this repo is a finding until `nyrt profile` and
> `nyrt run` have been run against the live API (see *Getting the real data*).

## Read this first: what the data is and isn't

- **The Postgres "transactional" source is a stand-in.** DSNY does not publish individual
  pickups. It publishes one figure per community district, month and material. The Postgres
  table `ops.dsny_collection` holds those monthly figures, written through an ingestion
  service the way an operational system would write them. It tracks revisions (a `revision`
  counter goes up when DSNY changes a published number). It is **not** pickup events.
- **Collection is measured; recovery is modeled.** DSNY measures tons collected. How much of
  the collected recycling is actually recovered (vs. lost as contamination or residue) is not
  published per district. It is estimated from citywide rates by material in
  [`seeds/ref_recovery_rates.csv`](seeds/ref_recovery_rates.csv), each with a source URL, a
  version and a low / base / high range. The estimates sit in separate `*_est` columns.
- **Recovery rates are only used once verified.** A rate row with `verified=false` is ignored
  (set `NYRT_ALLOW_UNVERIFIED_RATES=1` to override for exploration). Current state:

  | material | base | range | status |
  |---|---|---|---|
  | MGP | 85% recovered (≈15% MRF residue) | 80–90% (±5 pts, a project assumption) | **unverified**: seen only as a search excerpt of an undated Sims Sunset Park MRF fact sheet |
  | paper | not set | not set | needs the composition tables of the [2023 NYC Waste Characterization Study](https://www.nyc.gov/assets/dsny/downloads/resources/reports/waste-characterization-studies/2023/wcs-2023.pdf) |

  Until both are verified, the contamination panel stays off and the headline diversion number
  is reported in collected tons only, with no recovered-tons range. This follows the design
  doc's mitigation for "no citable recovery rates found".

## Architecture

```mermaid
flowchart TD
  A[NYC Open Data<br/>Socrata API ebb7-mvp5] -->|raw JSON + manifest| B0[(bronze/raw_api)]
  A --> S[Ingestion service<br/>FastAPI, upsert]
  S --> P[(Postgres ops.dsny_collection<br/>district x month x DSNY column)]
  P -->|month-partitioned Parquet| B1[(bronze/source_extract)]
  B1 --> SV[Silver: DuckDB SQL<br/>or PySpark]
  SV -->|checks| G[Gold star schema + metric views<br/>DuckDB, mirrored to BigQuery]
  R[seeds/ref_recovery_rates.csv<br/>cited, versioned] --> G
  D[seeds/dim_district.csv<br/>59 community districts] --> SV
  G -->|checks| E[JSON extracts]
  E --> W[Static dashboard<br/>GitHub Pages]
  AF{{Airflow monthly DAG<br/>ADF mapping in docs}} -.-> S & B1 & SV & G & E
```

Each layer is idempotent and partitioned by month (a rerun replaces a month, it does not
duplicate it) and promotes data only after its checks pass.

| Layer | Checks (fail the run on any violation) |
|---|---|
| Bronze | row count > 0; required columns present (`nyrt.dsny.REQUIRED_COLUMNS`) |
| Silver | keys unique; every district maps to `dim_district`; no unflagged negative tons; complete district × month × material grid; no unflagged nulls |
| Gold | citywide gold totals equal silver totals per month; recovered ≤ collected; recovery only on recyclable streams |

### Gold schema

- `gold.fact_tonnage`: one row per **district × month × material** (`refuse`, `paper`, `mgp`,
  `organics`) with `tons_collected` (measured), `tons_recovered_est` / `_low` / `_high`
  (modeled), `recovery_rate_version`, `quality_flag`, `source_loaded_at`.
- `gold.dim_district`: the 59 community districts with borough, number, BoroCD and name.
  **Borough figures are always a rollup of districts**, never loaded separately.
- `gold.dim_material`, `gold.dim_month` (season, holiday-month flag).

### Metrics (all in `sql/gold/model.sql`)

| Metric | Definition |
|---|---|
| Diversion rate | (paper + MGP + organics) ÷ all tons collected, district × month |
| Recycling rate (core) | (paper + MGP) ÷ (refuse + paper + MGP); reported alongside, because organics started partway through the history and unevenly across districts |
| Rate trend | 12-month rolling, tonnage-weighted rate, and its year-over-year change; only when all 12 months are complete |
| Rank | rank of the rolling rate in the city and within the borough |
| Estimated contamination | recyclable tons collected − recovered (modeled; low / base / high) |
| Seasonal index | a month's citywide rate ÷ that year's average, averaged across complete years; tested with OLS, district + year fixed effects, SEs clustered by district, joint F-test on month terms |
| Diversion gap | (top-quartile core rate among districts in the same borough − district rate) × district tons, floored at 0, over the latest complete 12 months |

Quality flags in silver: `ok`, `missing` (blank), `absent_row` (no row for that district-month),
`zero` (0 tons for refuse, paper or MGP, almost certainly a gap), `negative`, and
`not_reported` (organics before a district had the programme). Any flag other than `ok` /
`not_reported` makes the district-month incomplete: it is left out of rates and rankings, and
the dashboard lists the excluded districts.

## Running it

```bash
pip install -e ".[dev]"
docker compose up -d postgres ingest           # Postgres + ingestion service
export NYRT_PG_DSN=postgresql://nyrt:nyrt@localhost:5432/nyrt

nyrt profile                                   # roadmap step 1: writes docs/data_profile.md
nyrt run                                       # full backfill: ingest -> bronze -> silver -> gold -> export
nyrt run --incremental                         # re-pull and reprocess the latest 3 months
python -m http.server -d dashboard 8000        # open http://localhost:8000
```

Individual steps: `nyrt init-db | ingest | extract | transform | export | serve`.
No network to the API? Export the dataset as CSV from the portal and use
`nyrt run --from-file DSNY_Monthly_Tonnage_Data.csv`.

- **Spark:** `spark/silver_spark.py` is the PySpark version of silver (runs on Databricks
  Community Edition); `tests/test_spark_parity.py` checks it row for row against the SQL.
- **BigQuery:** `pip install -e ".[bigquery]" && nyrt-bq --project <id>` loads the gold tables.
- **Airflow:** `airflow/dags/nyrt_monthly.py`; the ADF mapping is in `docs/orchestration.md`.
- **Kubernetes / AKS:** `Dockerfile` and `deploy/k8s/nyrt.yaml` (ingestion Deployment +
  monthly CronJob).
- **CI:** `.github/workflows/ci.yml` runs lint, unit tests, Postgres tests and Spark parity,
  a pipeline run on the synthetic sample, and a Docker build.

## Getting the real data (roadmap step 1)

1. `nyrt profile` and read `docs/data_profile.md`. Confirm the column names in
   `src/nyrt/dsny.py`, the month label format, whether all 59 districts appear, when each
   organics column starts, and how many zeros and blanks there are.
2. Confirm whether DSNY revises recent months (`ops.dsny_collection.revision > 1` after a
   few monthly runs). The three-month re-pull assumes it does.
3. Verify the recovery rates: open each source, check the figure, set `verified=true`.
4. Fill `seeds/district_population.csv` for per-capita figures (optional).

## Key decisions

- **Unpivot on the write path.** The ingestion service turns each published row into one row
  per DSNY tonnage column, so Postgres holds district × month × material rows as the design
  says. Silver then maps columns to materials (the organics columns roll up into `organics`).
- **Full pull each run, partial write.** The dataset is small (about 59 rows a month), so
  every run lands the whole API response in bronze for audit, but incremental runs only write
  and reprocess the latest three months.
- **Gold is rebuilt from silver each run.** It is small, and a rebuild is simpler and
  just as idempotent as month-level merges.
- **Peer group is the borough.** The benchmark is the 75th percentile of the borough's
  district rates, not the single best district. Staten Island has only three districts, so
  its benchmark is less stable.
- **Headline uses the core rate (no organics),** because organics coverage differs between
  districts and would inflate the gap where the programme has not arrived.

## Out of scope

Real-time streaming (the data is monthly), route or cost optimization, production cloud
spend, NY State data (planned v2), and A/B testing (covered by a separate project).

## Layout

```
src/nyrt/      ingest (Socrata, profile), source (Postgres, service, extract), transform (DuckDB, BigQuery), analytics
sql/           source schema, silver, gold, checks
seeds/         dim_district, dim_material, source_column_map, ref_recovery_rates, district_population
spark/         PySpark silver
airflow/       DAG
deploy/k8s/    AKS manifest
dashboard/     static site (reads dashboard/data/*.json)
tests/         unit, Postgres, Spark parity; fixtures/synthetic_dsny.py is SYNTHETIC test data
```
