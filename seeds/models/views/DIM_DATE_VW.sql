select *,month*-1 as month_order from {{ ref("DIM_DATE") }}
