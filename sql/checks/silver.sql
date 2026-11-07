-- Each query returns violating rows; the runner fails the layer if any query returns rows.
-- name: silver_unique_keys
select district_id, month, material, count(*) as n
from silver.tonnage where month in (select month from run_months)
group by all having count(*) > 1;

-- name: silver_every_district_maps
select * from silver.rejected
where month in (select month from run_months) and reason = 'district not in dim_district';

-- name: silver_no_unflagged_negative
select * from silver.tonnage
where month in (select month from run_months) and tons_collected < 0 and quality_flag <> 'negative';

-- name: silver_complete_grid
select r.month, count(t.district_id) as rows_present,
       (select count(*) from ref.dim_district) * (select count(*) from ref.dim_material) as expected
from run_months r left join silver.tonnage t using (month)
group by r.month
having count(t.district_id) <> (select count(*) from ref.dim_district) * (select count(*) from ref.dim_material);

-- name: silver_no_unflagged_missing
select * from silver.tonnage
where month in (select month from run_months) and tons_collected is null
  and quality_flag not in ('missing', 'absent_row', 'not_reported');
