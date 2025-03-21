select
    null as fppg_year_fppg,
    fppg_month_fppg as month,
    fppg_employee_key_ddep as employee_key,
    fppg_perf_goal_key_dppg as perf_goal_key,
    nb_goals_per_employee as nb_goals_per_employee,
    fppg_status_dpgs as goal_status_key,
    fppg_category_key_dpgc as goal_category_key,
    fppg_creation_date_fppg as creation_date,
    fppg_last_modification_date_fppg as last_modification_date,
    fppg_due_date_fppg as due_date,
    fppg_perf_goals_cascaded_key_dpgs as goal_cascaded_key,
    fppg_fg_deleted_fppg as is_goal_deleted,
    fppg_perf_goals_type_key_dpgt as goal_type_key,
    number_of_linked_activites,
    creation_date_month_display,
    due_date_month_display,
    fppg_goal_plan_id_fppg as goal_plan_id
from {{ ref("fact_perf_goals_final") }}
join
    {{ env_var("DBT_CORE_DB") }}.cmn_core_sch.dim_employee_profile_final
    on fppg_employee_key_ddep = ddep_employee_profile_sk_ddep
