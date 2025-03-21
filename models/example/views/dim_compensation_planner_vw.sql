select
    dcpv_comp_plan_sk_ddep as comp_plan_sk,
    comp_planner_user_id,
    comp_planner_full_name
from {{ ref("dim_compensation_planner") }}
