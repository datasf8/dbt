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
    perf_goals_status as (
        select distinct status as dpgs_perf_goals_status_label_dpgs
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
            {{ dbt_utils.surrogate_key("DPGS_PERF_GOALS_STATUS_LABEL_DPGS") }}
            as sk_goal_status_id
        from perf_goals_status p

    )
select
    hash(dpgs_perf_goals_status_label_dpgs) as dpgs_perf_goals_status_sk_dpgs,
    sk_goal_status_id as dpgs_perf_goals_status_key_dpgs,
    dpgs_perf_goals_status_label_dpgs
from surrogate_key
