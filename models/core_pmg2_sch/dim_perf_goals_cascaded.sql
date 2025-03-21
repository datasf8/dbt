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
    perf_goals_cascaded as (
        select distinct goal_aligned_up as dpgs_perf_goals_cascaded_label_dpgs
        from {{ ref("stg_perf_goal_report") }}

    ),
    surrogate_key as (
        select
            p.*,
            {{ dbt_utils.surrogate_key("DPGS_PERF_GOALS_CASCADED_LABEL_DPGS") }}
            as sk_cascaded_label
        from perf_goals_cascaded p

    )
select
    hash(dpgs_perf_goals_cascaded_label_dpgs) as dpgs_perf_goals_cascaded_sk_dpgs,
    sk_cascaded_label as dpgs_perf_goals_cascaded_key_dpgs,
    dpgs_perf_goals_cascaded_label_dpgs
from surrogate_key
