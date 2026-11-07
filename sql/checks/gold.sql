-- name: gold_totals_match_silver
select s.month, s.tons as silver_tons, g.tons as gold_tons
from (select month, coalesce(sum(tons_collected), 0) as tons from silver.tonnage group by month) s
full join (select month, coalesce(sum(tons_collected), 0) as tons from gold.fact_tonnage group by month) g
  using (month)
where g.tons is null or s.tons is null or abs(s.tons - g.tons) > 0.01;

-- name: gold_recovery_within_collected
select * from gold.fact_tonnage
where tons_recovered_est_high > tons_collected + 0.001 or tons_recovered_est_low < 0;

-- name: gold_recovery_only_for_recyclables
select f.* from gold.fact_tonnage f join gold.dim_material m using (material)
where not m.is_recyclable and f.tons_recovered_est is not null;
