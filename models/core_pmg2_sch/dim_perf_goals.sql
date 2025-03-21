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
    perf_goals as (
        select distinct
            user_id as dppg_employee_id_dppg,
            goal_id as dppg_perf_goal_id_dppg,
            goal_name as dppg_perf_goal_name_dppg,
            kpi_expected_results as dppg_perf_goal_kpi_dppg,
            '' as dppg_perf_goal_name_data_quality_dppg,
            '' as dppg_perf_goal_kpi_data_quality_dppg

        from
            (
                select *
                from {{ ref("stg_user_goal_details_flatten") }}
                where dbt_valid_to is null
            )
    ),
    surrogate_key as (
        select
            p.*,
            {{
                dbt_utils.surrogate_key(
                    "DPPG_PERF_GOAL_ID_DPPG", "DPPG_EMPLOYEE_ID_DPPG"
                )
            }} as sk_goal_id
        from perf_goals p

    )
select
    hash(dppg_perf_goal_id_dppg, dppg_employee_id_dppg) as dppg_perf_goal_sk_dppg,
    sk_goal_id as dppg_perf_goal_key_dppg,
    dppg_employee_id_dppg,
    dppg_perf_goal_id_dppg,
    dppg_perf_goal_name_dppg,
    dppg_perf_goal_kpi_dppg,
    dppg_perf_goal_name_data_quality_dppg,
    dppg_perf_goal_kpi_data_quality_dppg
from surrogate_key
