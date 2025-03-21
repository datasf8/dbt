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
            a.ddcac_activity_sk_ddcac as reag_activity_key_dcac,
            c.ddpg_dev_goal_sk_ddpg as reag_goal_key_dppg,
            'DevelopmentGoal' as reag_goal_type_reag
        from {{ ref("dim_dev_connect_activities") }} a
        inner join
            {{ ref("stg_dev_goal_report") }} b
            on a.dcac_activity_id_dcac = b.activity_id
        inner join
            {{ ref("dim_dev_goals") }} c
            on c.ddpg_dev_goal_id_ddpg = b.development_goal_id

    )
select reag_activity_key_dcac, reag_goal_key_dppg, reag_goal_type_reag
from connect_activities
