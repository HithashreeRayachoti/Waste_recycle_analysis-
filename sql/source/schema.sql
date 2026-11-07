-- Operational source: DSNY monthly records as an operational system would hold them.
-- DSNY does not publish individual pickups. Each row is one published monthly figure for one
-- district, month and tonnage column, written through the ingestion service. It is a
-- deliberate stand-in for a transactional system, not real pickup events.
create schema if not exists ops;

create table if not exists ops.dsny_collection (
    borough             text        not null,  -- as published, normalized spelling
    community_district  text        not null,  -- as published, zero-padded when numeric
    month               date        not null,  -- first day of the month
    source_column       text        not null,  -- DSNY tonnage column, e.g. papertonscollected
    raw_month           text        not null,  -- the month label exactly as published
    tons                numeric(12, 2),        -- null when DSNY left it blank
    revision            integer     not null default 1,  -- bumps when DSNY changes the figure
    first_loaded_at     timestamptz not null default now(),
    last_loaded_at      timestamptz not null default now(),
    last_changed_at     timestamptz not null default now(),
    load_id             bigint,
    primary key (borough, community_district, month, source_column)
);

create index if not exists dsny_collection_month_idx on ops.dsny_collection (month);

create table if not exists ops.load_log (
    load_id      bigserial primary key,
    loaded_at    timestamptz not null default now(),
    source       text        not null,
    records_in   integer     not null,
    rows_upserted integer    not null,
    rows_changed integer     not null
);
