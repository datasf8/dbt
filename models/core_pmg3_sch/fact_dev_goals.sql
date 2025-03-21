{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
        on_schema_change="sync_all_columns",
    )
}}
with
    fact_dev_goals as (
        select distinct
            udgdi.user_id as user_id,
            udgdi.last_modified_date,
            dep.ddep_employee_profile_sk_ddep,
            ddg.ddpg_dev_goal_sk_ddpg,
            ddgs.ddgs_dev_goals_status_sk_ddgs,
            ddgc.ddgc_dev_goal_category_sk_ddgc,
            dgp.ddpp_purpose_sk_ddpp,
            udgdi.nb_employee_goals,
            udgdi.due_date,
            ddg.ddpg_dev_goal_id_ddpg,
            sdgr.created_date,
            sdgr.goal_active_deleted,
            sdgr.nb_activities_per_goal,
            sdgr.nb_linked_learning_goals,
            udgdi.dev_goal_template_id,
            ddgp_dev_goal_plan_name_ddgp,
            ddgp_display_order_ddgp,
            ddgp_due_date_ddgp,
            ddgp_year_ddgp
        from {{ ref("stg_user_dev_goal_details_int") }} udgdi
        left outer join
            {{ ref("dim_employee_profile") }} dep
            on udgdi.user_id = dep.ddep_employee_id_ddep
        left outer join
            {{ ref("dim_dev_goals") }} ddg on udgdi.goal_id = ddg.ddpg_dev_goal_id_ddpg
        left outer join
            {{ ref("dim_dev_goals_status") }} ddgs
            on udgdi.goal_status = ddgs.ddgs_dev_goals_status_label_ddgs
        left outer join
            {{ ref("dim_dev_goals_category") }} ddgc
            on udgdi.category = ddgc.ddgc_dev_goal_category_label_ddgc
        left outer join
            {{ ref("dim_goal_purpose") }} dgp on udgdi.purpose = dgp.ddpp_purpose_ddpp
        left outer join
            {{ ref("stg_dev_goal_report_int") }} sdgr
            on udgdi.goal_id = sdgr.development_goal_id
        left outer join
            {{ ref("dim_dev_goals_plan") }} ddgp
            on udgdi.dev_goal_template_id = ddgp.ddgp_dev_goal_template_id_ddgp
    )
select
    user_id,
    ddep_employee_profile_sk_ddep as fdpg_employee_profile_key_ddep,
    ddpg_dev_goal_sk_ddpg as fdpg_dev_goal_key_ddpg,
    ddgs_dev_goals_status_sk_ddgs as fdpg_dev_goals_status_key_ddgs,
    ddgc_dev_goal_category_sk_ddgc as fdpg_dev_goal_category_key_ddgc,
    ddpp_purpose_sk_ddpp as fdpg_purpose_key_ddpp,
    nb_employee_goals as fdpg_nb_employee_goals_fdpg,
    created_date as fdpg_goal_creation_date_fdpg,
    due_date as fdpg_due_date_fdpg,
    last_modified_date as fdpg_last_modified_date_fdpg,
    goal_active_deleted as fdpg_goal_active_deleted_fdpg,
    nb_activities_per_goal as fdpg_nb_linked_activities_fdpg,
    nb_linked_learning_goals as fdpg_nb_linked_learnings_fdpg,
    dev_goal_template_id as dev_goal_plan_template_id,
    ddgp_dev_goal_plan_name_ddgp as dev_goal_plan_template_name,
    ddgp_display_order_ddgp as dev_goal_plan_template_display_order,
    ddgp_due_date_ddgp as dev_goal_plan_template_due_date,
    ddgp_year_ddgp as fdpf_year_ddgp
from fact_dev_goals
where fdpf_year_ddgp is not null
