select distinct
    null as year,
    fpgu_goal_plan_id_dpgp as goal_plan_id,
    fpgu_perf_goal_key_dppg as perf_goal_key,
    fppg_employee_key_ddep as employee_key,
    fppg_category_key_dpgc as goal_category_key,
    fppg_status_dpgs as goal_status_key,
    fppg_perf_goals_cascaded_key_dpgs as goal_cascaded_key,
    fppg_perf_goals_type_key_dpgt as goal_type_key,
    fpgu_is_current_situation_fpgu as is_current_situation,
    fpgu_previous_goal_name_fpgu as previous_goal_name,
    fpgu_updated_goal_name_fpgu as updated_goal_name,
    fpgu_previous_kpi_name_fpgu as previous_kpi_name,
    fpgu_updated_kpi_name_fpgu as updated_kpi_name,
    fpgu_update_date_fpgu as update_date,
    fppg_fg_deleted_fppg as is_goal_active,
    update_date_month_display
from {{ ref("fact_perf_goals_update_final") }}
join
    {{ ref("dim_employee_profile_final") }}
    on fppg_employee_key_ddep = ddep_employee_profile_sk_ddep
