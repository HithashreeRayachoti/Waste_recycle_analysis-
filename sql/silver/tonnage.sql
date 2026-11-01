-- Silver: reconcile the Postgres extract into district x month x material.
-- Inputs (set up by nyrt.transform.silver): bronze_extract (view over the month partitions),
-- run_months(month), ref.dim_district, ref.dim_material, ref.source_column_map.
create schema if not exists silver;

create table if not exists silver.tonnage (
    district_id      varchar not null,
    month            date    not null,
    material         varchar not null,
    tons_collected   double,             -- null when DSNY did not report a figure
    quality_flag     varchar not null,   -- ok | missing | absent_row | zero | negative | not_reported
    n_source_columns integer not null,   -- how many DSNY columns rolled up into this material
    max_revision     integer,            -- highest revision count seen in Postgres
    source_loaded_at timestamp,
    primary key (district_id, month, material)
);

create table if not exists silver.rejected (
    borough            varchar,
    community_district varchar,
    month              date,
    source_column      varchar,
    tons               double,
    reason             varchar
);

-- Deduplicate (latest load wins) and map districts and columns.
create or replace temp table _mapped as
select
    d.district_id,
    b.month::date            as month,
    m.material,
    b.borough,
    b.community_district,
    b.source_column,
    b.tons::double           as tons,
    b.revision::integer      as revision,
    b.source_loaded_at::timestamp as source_loaded_at
from bronze_extract b
left join ref.dim_district d
       on d.borough = b.borough
      and lpad(d.district_number::varchar, 2, '0') = b.community_district
left join ref.source_column_map m on m.source_column = b.source_column
where b.month::date in (select month from run_months)
qualify row_number() over (
    partition by b.borough, b.community_district, b.month, b.source_column
    order by b.source_loaded_at desc
) = 1;

delete from silver.rejected where month in (select month from run_months);
insert into silver.rejected
select borough, community_district, month, source_column, tons,
       case when district_id is null then 'district not in dim_district'
            else 'column not mapped to a material' end
from _mapped
where district_id is null or material is null;

create or replace temp table _agg as
select district_id, month, material,
       sum(tons)            as tons,          -- sum() skips nulls
       count(*)             as n_cols,
       count(tons)          as n_reported,
       min(tons)            as min_tons,
       max(revision)        as max_revision,
       max(source_loaded_at) as source_loaded_at
from _mapped
where district_id is not null and material is not null
group by all;

-- Every district x month x material must exist, so gaps are flagged instead of silently absent.
create or replace temp table _silver as
select
    g.district_id,
    g.month,
    g.material,
    case when a.n_reported > 0 then a.tons end as tons_collected,
    case
        when a.material is null and g.material = 'organics' then 'not_reported'
        when a.material is null                         then 'absent_row'
        when a.n_reported = 0 and g.material = 'organics' then 'not_reported'
        when a.n_reported = 0                            then 'missing'
        when a.min_tons < 0                              then 'negative'
        when a.tons = 0 and g.material <> 'organics'     then 'zero'
        else 'ok'
    end as quality_flag,
    coalesce(a.n_cols, 0) as n_source_columns,
    a.max_revision,
    a.source_loaded_at
from (
    select d.district_id, r.month, m.material
    from ref.dim_district d, run_months r, ref.dim_material m
) g
left join _agg a using (district_id, month, material);

-- Idempotent by month: a rerun replaces the month instead of duplicating it.
delete from silver.tonnage where month in (select month from run_months);
insert into silver.tonnage by name select * from _silver;
