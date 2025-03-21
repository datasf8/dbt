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
    connect_activities as (
        select distinct
            a.dpcac_activity_sk_dpcac as reag_activity_key_dcac,
            c.dppg_perf_goal_sk_dppg as reag_goal_key_reag,
            'PerformanceGoal' as reag_goal_type_reag
        from {{ ref("dim_connect_activities") }} a
        inner join
            {{ ref("stg_perf_goal_report") }} b
            on a.dcac_activity_id_dcac = b.activity_id
        inner join {{ ref("dim_perf_goals") }} c on c.dppg_perf_goal_id_dppg = b.goal_id

    )
select reag_activity_key_dcac, reag_goal_key_reag, reag_goal_type_reag
from connect_activities
