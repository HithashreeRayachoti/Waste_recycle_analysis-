-- Gold: a small star schema plus the metric views the dashboard reads.
-- Rebuilt from silver on every run (it is small), so it is idempotent.
-- The runner replaces :allow_unverified with true/false.
create schema if not exists gold;

create or replace table gold.dim_district as
select d.district_id, d.borough, d.borough_code, d.district_number, d.boro_cd, d.district_name,
       p.population, p.population_year
from ref.dim_district d
left join ref.district_population p using (district_id);

create or replace table gold.dim_material as select * from ref.dim_material;

create or replace table gold.dim_month as
select month,
       year(month)  as year,
       month(month) as month_of_year,
       case when month(month) in (12, 1, 2) then 'winter'
            when month(month) in (3, 4, 5)  then 'spring'
            when month(month) in (6, 7, 8)  then 'summer'
            else 'fall' end as season,
       month(month) in (11, 12) as is_holiday_month
from (select distinct month from silver.tonnage);

-- Only rates with a figure, and (unless overridden) only rates marked verified.
create or replace table gold.ref_recovery_rates_used as
select material, rate_version, recovery_low, recovery_base, recovery_high, source_url, verified
from ref.recovery_rates
where recovery_base is not null and (verified or :allow_unverified);

-- Measured and modeled values sit in separate columns.
create or replace table gold.fact_tonnage as
select t.district_id,
       t.month,
       t.material,
       t.tons_collected,
       case when m.is_recyclable then t.tons_collected * r.recovery_base end as tons_recovered_est,
       case when m.is_recyclable then t.tons_collected * r.recovery_low  end as tons_recovered_est_low,
       case when m.is_recyclable then t.tons_collected * r.recovery_high end as tons_recovered_est_high,
       case when m.is_recyclable then r.rate_version end as recovery_rate_version,
       t.quality_flag,
       t.source_loaded_at
from silver.tonnage t
join gold.dim_material m using (material)
left join gold.ref_recovery_rates_used r using (material);

-- District x month, wide, with both rate definitions.
create or replace view gold.district_month as
with p as (
    select district_id, month,
           max(tons_collected) filter (where material = 'refuse') as refuse_tons,
           max(tons_collected) filter (where material = 'paper')  as paper_tons,
           max(tons_collected) filter (where material = 'mgp')    as mgp_tons,
           coalesce(max(tons_collected) filter (where material = 'organics'), 0) as organics_tons,
           bool_and(quality_flag in ('ok', 'not_reported')) as is_complete,
           string_agg(distinct quality_flag, ',') filter (where quality_flag not in ('ok', 'not_reported'))
               as issues
    from gold.fact_tonnage
    group by all
)
select p.*, d.borough, d.district_name,
       refuse_tons + paper_tons + mgp_tons + organics_tons as total_tons,
       case when is_complete then (paper_tons + mgp_tons + organics_tons)
            / nullif(refuse_tons + paper_tons + mgp_tons + organics_tons, 0) end as diversion_rate,
       case when is_complete then (paper_tons + mgp_tons)
            / nullif(refuse_tons + paper_tons + mgp_tons, 0) end as recycling_rate_core
from p join gold.dim_district d using (district_id);

-- 12-month rolling rates (tonnage-weighted) and their year-over-year change.
-- A window only counts when all 12 of its months are complete.
create or replace view gold.district_rate_trend as
with r as (
    select district_id, borough, district_name, month,
           sum(case when is_complete then 1 else 0 end) over w as complete_months,
           sum(paper_tons + mgp_tons + organics_tons) over w as diverted_12m,
           sum(total_tons) over w as total_12m,
           sum(paper_tons + mgp_tons) over w as recycled_core_12m,
           sum(refuse_tons + paper_tons + mgp_tons) over w as total_core_12m
    from gold.district_month
    window w as (partition by district_id order by month
                 range between interval 11 month preceding and current row)
), rr as (
    select *,
           case when complete_months = 12 then diverted_12m / nullif(total_12m, 0) end
               as rolling_diversion_rate,
           case when complete_months = 12 then recycled_core_12m / nullif(total_core_12m, 0) end
               as rolling_recycling_rate_core
    from r
)
select a.district_id, a.borough, a.district_name, a.month, a.complete_months,
       a.total_12m, a.total_core_12m,
       a.rolling_diversion_rate, a.rolling_recycling_rate_core,
       a.rolling_diversion_rate - b.rolling_diversion_rate as yoy_change_diversion,
       a.rolling_recycling_rate_core - b.rolling_recycling_rate_core as yoy_change_core
from rr a
left join rr b on b.district_id = a.district_id and b.month = a.month - interval 12 month;

-- The latest month with any complete 12-month window is the ranking month.
create or replace view gold.ranking_month as
select max(month) as month from gold.district_rate_trend where rolling_diversion_rate is not null;

create or replace view gold.district_rank as
select t.*,
       rank() over (order by rolling_diversion_rate desc) as city_rank,
       rank() over (partition by borough order by rolling_diversion_rate desc) as borough_rank,
       rank() over (order by rolling_recycling_rate_core desc) as city_rank_core
from gold.district_rate_trend t
join gold.ranking_month rm using (month)
where t.rolling_diversion_rate is not null;

-- Districts left out of the ranking, and why.
create or replace view gold.ranking_exclusions as
select d.district_id, d.borough, d.district_name,
       coalesce(t.complete_months, 0) as complete_months_in_window,
       (select string_agg(distinct dm.issues, ',') from gold.district_month dm, gold.ranking_month rm2
         where dm.district_id = d.district_id and dm.issues is not null
           and dm.month > rm2.month - interval 12 month and dm.month <= rm2.month) as issues
from gold.dim_district d
cross join gold.ranking_month rm
left join gold.district_rate_trend t on t.district_id = d.district_id and t.month = rm.month
where t.rolling_diversion_rate is null;

create or replace view gold.borough_month as
select borough, month,
       sum(refuse_tons) as refuse_tons, sum(paper_tons) as paper_tons, sum(mgp_tons) as mgp_tons,
       sum(organics_tons) as organics_tons, sum(total_tons) as total_tons,
       sum(paper_tons + mgp_tons + organics_tons) / nullif(sum(total_tons), 0) as diversion_rate,
       sum(paper_tons + mgp_tons) / nullif(sum(refuse_tons + paper_tons + mgp_tons), 0)
           as recycling_rate_core,
       count(*) as districts_complete
from gold.district_month
where is_complete
group by all;

-- Modeled contamination by material and year (recyclable streams only).
-- High recovery means low contamination, so the bounds swap.
create or replace view gold.contamination_by_material as
select year(f.month) as year, f.material,
       count(distinct f.month) as months,
       sum(f.tons_collected) as tons_collected,
       sum(f.tons_recovered_est_low)  as recovered_low,
       sum(f.tons_recovered_est)      as recovered_base,
       sum(f.tons_recovered_est_high) as recovered_high,
       sum(f.tons_collected) - sum(f.tons_recovered_est_high) as contamination_low,
       sum(f.tons_collected) - sum(f.tons_recovered_est)      as contamination_base,
       sum(f.tons_collected) - sum(f.tons_recovered_est_low)  as contamination_high,
       any_value(f.recovery_rate_version) as recovery_rate_version
from gold.fact_tonnage f
join gold.dim_material m using (material)
where m.is_recyclable and f.quality_flag = 'ok'
group by all;

-- Seasonal index: a month's citywide rate / that year's average monthly rate, averaged
-- over complete years. Districts with incomplete months are left out of that month.
create or replace view gold.seasonal_index as
with city as (
    select month,
           sum(paper_tons + mgp_tons + organics_tons) / sum(total_tons) as diversion_rate,
           sum(paper_tons + mgp_tons) / sum(refuse_tons + paper_tons + mgp_tons) as core_rate
    from gold.district_month where is_complete group by month
), y as (
    select *, month(month) as month_of_year,
           avg(diversion_rate) over (partition by year(month)) as yr_div,
           avg(core_rate) over (partition by year(month)) as yr_core,
           count(*) over (partition by year(month)) as months_in_year
    from city
)
select month_of_year,
       avg(diversion_rate / yr_div) as seasonal_index_diversion,
       avg(core_rate / yr_core)     as seasonal_index_core,
       count(*) as years
from y where months_in_year = 12
group by month_of_year order by month_of_year;

-- Diversion gap over the latest complete 12 months, on the recycling rate without organics
-- (curbside organics rolled out unevenly, so it is not comparable between districts).
-- Benchmark: the top-quartile rate among districts in the same borough.
create or replace view gold.diversion_gap as
with d as (
    select t.district_id, t.borough, t.district_name, t.month as window_end,
           t.rolling_recycling_rate_core as rate, t.total_core_12m as tons_12m
    from gold.district_rate_trend t join gold.ranking_month rm using (month)
    where t.rolling_recycling_rate_core is not null
), b as (
    select *, quantile_cont(rate, 0.75) over (partition by borough) as benchmark_rate from d
)
select *, greatest(benchmark_rate - rate, 0) * tons_12m as gap_tons from b;

-- Blended recovery for paper + MGP over the same window, weighted by tonnage.
-- Null unless every recyclable material has a usable rate.
create or replace view gold.blended_recovery as
select case when count(f.tons_recovered_est) = count(*)
            then sum(f.tons_recovered_est_low) / sum(f.tons_collected) end as recovery_low,
       case when count(f.tons_recovered_est) = count(*)
            then sum(f.tons_recovered_est) / sum(f.tons_collected) end as recovery_base,
       case when count(f.tons_recovered_est) = count(*)
            then sum(f.tons_recovered_est_high) / sum(f.tons_collected) end as recovery_high
from gold.fact_tonnage f
join gold.dim_material m using (material)
cross join gold.ranking_month rm
where m.is_recyclable and f.quality_flag = 'ok'
  and f.month > rm.month - interval 12 month and f.month <= rm.month;

create or replace view gold.diversion_headline as
select any_value(g.window_end) as window_end,
       count(*) as districts,
       sum(g.gap_tons) as gap_tons_collected,
       sum(g.gap_tons) * any_value(br.recovery_low)  as gap_tons_recovered_low,
       sum(g.gap_tons) * any_value(br.recovery_base) as gap_tons_recovered_base,
       sum(g.gap_tons) * any_value(br.recovery_high) as gap_tons_recovered_high
from gold.diversion_gap g cross join gold.blended_recovery br;
