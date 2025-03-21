select distinct
    ddgp_year_ddgp as year,
    month,
    fdpg_employee_profile_key_ddep as employee_profile_key,
    fdpg_dev_goal_key_ddpg as dev_goal_key,
    nb_employee_goals,
    fdpg_dev_goals_status_key_ddgs as goal_status_key,
    fdpg_purpose_key_ddpp as purpose_key,
    fdpg_due_date_fdpg as due_date,
    fdpg_goal_creation_date_fdpg as creation_date,
    fdpg_goal_active_deleted_fdpg as active_deleted,
    fdpg_last_modified_date_fdpg as last_modified_date,
    fdpg_nb_linked_activities_fdpg as nb_linked_activities,
    fdpg_nb_linked_learnings_fdpg as nb_linked_learnings,
    fdpg_dev_goal_category_key_ddgc as goal_category_key,
    creation_date_month_display,
    due_date_month_display,
    dev_goal_plan_template_id,
    dev_goal_plan_template_name,
    dev_goal_plan_template_display_order,
    dev_goal_plan_template_due_date
from {{ ref("fact_dev_goals_final") }}
join
    {{ ref("dim_employee_profile_final") }}
    on fdpg_employee_profile_key_ddep = ddep_employee_profile_sk_ddep
