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
    fact_dev_goals_update as (
        select distinct
            prev.user_id,
            dep.ddep_employee_profile_sk_ddep,
            ddg.ddpg_dev_goal_sk_ddpg,
            ddgs.ddgs_dev_goals_status_sk_ddgs,
            ddgc.ddgc_dev_goal_category_sk_ddgc,
            dgp.ddpp_purpose_sk_ddpp,
            ddg.ddpg_dev_goal_id_ddpg,
            dgr.goal_active_deleted as fdgu_is_current_situation_fdgu,
            prev.goal_name as fdgu_previous_goal_name_fdgu,
            act.goal_name as fdgu_updated_goal_name_fdgu,
            act.dbt_updated_at as fdgu_update_date_fdgu,
            ddgp_dev_goal_template_id_ddgp,
            ddgp_year_ddgp
        from (select * from {{ ref("stg_user_dev_goal_details_flatten") }}) prev
        left outer join
            {{ ref("stg_dev_goal_report") }} dgr
            on prev.goal_id = dgr.development_goal_id
        inner join
            {{ ref("stg_user_dev_goal_details_flatten") }} act
            on prev.user_id = act.user_id
            and prev.goal_id = act.goal_id
            and prev.goal_name <> act.goal_name
        left outer join
            {{ ref("dim_employee_profile") }} dep
            on prev.user_id = dep.ddep_employee_id_ddep
        left outer join
            {{ ref("dim_dev_goals") }} ddg on prev.goal_id = ddg.ddpg_dev_goal_id_ddpg
        left outer join
            {{ ref("dim_dev_goals_status") }} ddgs
            on prev.goal_status = ddgs.ddgs_dev_goals_status_label_ddgs
        left outer join
            {{ ref("dim_dev_goals_category") }} ddgc
            on prev.category = ddgc.ddgc_dev_goal_category_label_ddgc
        left outer join
            {{ ref("dim_goal_purpose") }} dgp on prev.purpose = dgp.ddpp_purpose_ddpp
        inner join
            {{ ref("dim_dev_goals_plan") }} ddgp
            on to_char(prev.dev_goal_template_id)
            = to_char(ddgp_dev_goal_template_id_ddgp)
    )
select
    ddep_employee_profile_sk_ddep as fdgu_employee_profile_key_ddep,
    ddpg_dev_goal_sk_ddpg as fdgu_dev_goal_key_ddpg,
    ddgs_dev_goals_status_sk_ddgs as fdgu_dev_goals_status_key_ddgs,
    ddgc_dev_goal_category_sk_ddgc as fdgu_dev_goal_category_key_ddgc,
    ddpp_purpose_sk_ddpp as fdgu_purpose_key_ddpp,
    fdgu_is_current_situation_fdgu,
    fdgu_previous_goal_name_fdgu,
    fdgu_updated_goal_name_fdgu,
    fdgu_update_date_fdgu,
    ddgp_dev_goal_template_id_ddgp as fdgu_dev_goal_template_id_ddgp,
    ddgp_year_ddgp as fdgu_year_ddgp
from fact_dev_goals_update
where fdgu_year_ddgp is not null
