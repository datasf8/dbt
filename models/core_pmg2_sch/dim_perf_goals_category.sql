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
    perf_goals_category as (
        select distinct category as dpgc_perf_goal_category_label_dpgc
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
            {{ dbt_utils.surrogate_key("DPGC_PERF_GOAL_CATEGORY_LABEL_DPGC") }}
            as sk_goal_catg_id
        from perf_goals_category p

    )
select
    hash(dpgc_perf_goal_category_label_dpgc) as dpgc_perf_goal_category_sk_dpgc,
    sk_goal_catg_id as dpgc_perf_goal_category_key_dpgc,
    dpgc_perf_goal_category_label_dpgc
from surrogate_key
