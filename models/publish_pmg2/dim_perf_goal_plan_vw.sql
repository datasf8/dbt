select
    dpgp_goal_plan_id_dpgp as goal_plan_id,
    dpgp_goal_plan_name_dpgp as goal_plan_name,
    dpgp_year_dpgp as year,
    dpgp_displayorder_dpgp as display_order
from {{ ref("dim_perf_goal_plan") }}
