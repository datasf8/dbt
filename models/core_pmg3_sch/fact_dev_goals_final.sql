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
    fact_dev as (
        select distinct
            ddgp_year_ddgp,
            collate(monthname(fdpg_goal_creation_date_fdpg), 'en-ci') as month,
            fdpg_employee_profile_key_ddep,
            fdpg_dev_goal_key_ddpg,
            count(distinct fdpg_dev_goal_key_ddpg) over (
                partition by fdpg_employee_profile_key_ddep, ddgp_year_ddgp
            ) as nb_employee_goals,
            fdpg_dev_goals_status_key_ddgs,
            fdpg_purpose_key_ddpp,
            fdpg_due_date_fdpg,
            fdpg_goal_creation_date_fdpg,
            fdpg_goal_active_deleted_fdpg,
            fdpg_last_modified_date_fdpg,
            fdpg_nb_linked_activities_fdpg,
            fdpg_nb_linked_learnings_fdpg,
            fdpg_dev_goal_category_key_ddgc,
            collate(
                case
                    when ddgp_year_ddgp = year(fdpg_goal_creation_date_fdpg)
                    then
                        right(
                            '0'
                            || cast(month(fdpg_goal_creation_date_fdpg) as varchar(4)),
                            2
                        )
                    when ddgp_year_ddgp > year(fdpg_goal_creation_date_fdpg)
                    then '.Prev Years'
                    when ddgp_year_ddgp < year(fdpg_goal_creation_date_fdpg)
                    then 'Next Years'
                end,
                'en-ci'
            ) as creation_date_month_display,
            collate(
                case
                    when ddgp_year_ddgp = year(fdpg_due_date_fdpg)
                    then right('0' || cast(month(fdpg_due_date_fdpg) as varchar(4)), 2)
                    when ddgp_year_ddgp > year(fdpg_due_date_fdpg)
                    then '.Prev Years'
                    when ddgp_year_ddgp < year(fdpg_due_date_fdpg)
                    then 'Next Years'
                end,
                'en-ci'
            ) as due_date_month_display,
            dev_goal_plan_template_id,
            dev_goal_plan_template_name,
            dev_goal_plan_template_display_order,
            dev_goal_plan_template_due_date
        from {{ ref("fact_dev_goals") }}
        inner join
            {{ ref("dim_dev_goals_plan") }} ddgp
            on dev_goal_plan_template_id = ddgp_dev_goal_template_id_ddgp
        inner join
            {{ ref("dim_dev_goals_category") }} ddgc
            on ddgc_dev_goal_category_sk_ddgc = fdpg_dev_goal_category_key_ddgc
        where
            (
                fdpg_goal_active_deleted_fdpg = 'Active'
                or fdpg_goal_active_deleted_fdpg is null
            )
            and ddgc_dev_goal_category_label_ddgc <> 'Archive'
    )

select
    ddgp_year_ddgp,
    month,
    fdpg_employee_profile_key_ddep,
    fdpg_dev_goal_key_ddpg,
    nb_employee_goals,
    fdpg_dev_goals_status_key_ddgs,
    fdpg_purpose_key_ddpp,
    fdpg_due_date_fdpg,
    fdpg_goal_creation_date_fdpg,
    fdpg_goal_active_deleted_fdpg,
    fdpg_last_modified_date_fdpg,
    fdpg_nb_linked_activities_fdpg,
    fdpg_nb_linked_learnings_fdpg,
    fdpg_dev_goal_category_key_ddgc,
    creation_date_month_display,
    due_date_month_display,
    dev_goal_plan_template_id,
    dev_goal_plan_template_name,
    dev_goal_plan_template_display_order,
    dev_goal_plan_template_due_date
from fact_dev
