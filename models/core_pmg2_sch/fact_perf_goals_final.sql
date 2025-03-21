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
    fact_years as (
        select distinct
            fppg_goal_plan_id_fppg,
            collate(monthname(fppg_creation_date_fppg), 'en-ci') as month,
            fppg_employee_key_ddep,
            fppg_perf_goal_key_dppg,
            nb_goals_per_employee,
            fppg_status_dpgs,
            fppg_category_key_dpgc,
            fppg_creation_date_fppg,
            fppg_last_modification_date_fppg,
            fppg_due_date_fppg,
            fppg_perf_goals_cascaded_key_dpgs,
            fppg_fg_deleted_fppg,
            fppg_perf_goals_type_key_dpgt,
            number_of_linked_activites,
            collate(
                case
                    when dpgp_year_dpgp = year(fppg_creation_date_fppg)
                    then
                        right(
                            '0' || cast(month(fppg_creation_date_fppg) as varchar(4)), 2
                        )
                    when dpgp_year_dpgp > year(fppg_creation_date_fppg)
                    then '.Prev Years'
                    when dpgp_year_dpgp < year(fppg_creation_date_fppg)
                    then 'Next Years'
                end,
                'en-ci'
            ) as creation_date_month_display,
            collate(
                case
                    when dpgp_year_dpgp = year(fppg_due_date_fppg)
                    then right('0' || cast(month(fppg_due_date_fppg) as varchar(4)), 2)
                    when dpgp_year_dpgp > year(fppg_due_date_fppg)
                    then '.Prev Years'
                    when dpgp_year_dpgp < year(fppg_due_date_fppg)
                    then 'Next Years'
                end,
                'en-ci'
            ) as due_date_month_display
        from {{ ref("fact_perf_goals") }}
        inner join
            {{ ref("dim_perf_goal_plan") }}
            on dpgp_goal_plan_id_dpgp = fppg_goal_plan_id_fppg
        join
            {{ ref("dim_perf_goals_category_vw") }}
            on goal_category_key = fppg_category_key_dpgc
        where
            (fppg_fg_deleted_fppg = 'Active' or fppg_fg_deleted_fppg is null)
            and goal_category_label <> 'Archive'
    )

select
    fppg_goal_plan_id_fppg,
    month as fppg_month_fppg,
    fppg_employee_key_ddep,
    fppg_perf_goal_key_dppg,
    nb_goals_per_employee,
    fppg_status_dpgs,
    fppg_category_key_dpgc,
    fppg_creation_date_fppg,
    fppg_last_modification_date_fppg,
    fppg_due_date_fppg,
    fppg_perf_goals_cascaded_key_dpgs,
    fppg_fg_deleted_fppg,
    fppg_perf_goals_type_key_dpgt,
    number_of_linked_activites,
    creation_date_month_display,
    due_date_month_display
from fact_years
