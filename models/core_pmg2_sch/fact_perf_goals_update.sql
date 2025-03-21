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
    fact_perf_goals_update as (
        select distinct
            dpgp_goal_plan_id_dpgp as fpgu_goal_plan_id_dpgp,
            perf.dppg_perf_goal_sk_dppg as fpgu_perf_goal_key_dppg,
            rep.active_deleted as fpgu_is_current_situation_fpgu,
            prev.goal_name as fpgu_previous_goal_name_fpgu,
            act.goal_name as fpgu_updated_goal_name_fpgu,
            prev.kpi_expected_results as fpgu_previous_kpi_name_fpgu,
            act.kpi_expected_results as fpgu_updated_kpi_name_fpgu,
            act.dbt_updated_at as fpgu_update_date_fpgu

        from {{ ref("stg_user_goal_details_flatten") }} prev
        left outer join
            {{ ref("stg_perf_goal_report") }} rep on rep.goal_id = prev.goal_id
        inner join
            {{ ref("stg_user_goal_details_flatten") }} act
            on prev.user_id = act.user_id
            and prev.goal_id = act.goal_id
            and (
                prev.goal_name <> act.goal_name
                or prev.kpi_expected_results <> act.kpi_expected_results
            )
        left outer join
            {{ ref("dim_perf_goals") }} perf
            on perf.dppg_perf_goal_id_dppg = prev.goal_id
        inner join
            {{ ref("dim_perf_goal_plan") }} dpgp
            on to_char(dpgp_goal_plan_id_dpgp) = to_char(prev.goal_template_id)
        where prev.dbt_valid_to is null
    )

select
    fpgu_goal_plan_id_dpgp,
    fpgu_perf_goal_key_dppg,
    fpgu_is_current_situation_fpgu,
    fpgu_previous_goal_name_fpgu,
    fpgu_updated_goal_name_fpgu,
    fpgu_previous_kpi_name_fpgu,
    fpgu_updated_kpi_name_fpgu,
    fpgu_update_date_fpgu
from fact_perf_goals_update
