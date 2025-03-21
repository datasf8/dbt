select distinct
    fdgu_employee_profile_key_ddep as employee_profile_key,
    fdgu_dev_goal_key_ddpg as dev_goal_key,
    fdgu_dev_goals_status_key_ddgs as goal_status_key,
    fdgu_purpose_key_ddpp as purpose_key,
    fdgu_dev_goal_category_key_ddgc as goal_category_key,
    fdgu_is_current_situation_fdgu as is_current_situation,
    fdgu_previous_goal_name_fdgu as previous_goal_name,
    fdgu_updated_goal_name_fdgu as updated_goal_name,
    fdgu_update_date_fdgu as update_date,
    update_date_month_display,
    fdgu_dev_goal_template_id_ddgp as dev_goal_template_id,
    fdgu_year_ddgp as year
from {{ ref("fact_dev_goals_update_final") }}
join
    {{ ref("dim_employee_profile_final") }}
    on fdgu_employee_profile_key_ddep = ddep_employee_profile_sk_ddep
