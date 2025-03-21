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
    perf_goals_type as (
        select distinct
            goal_characteristic as dpgt_perf_goals_type_label_dpgt,
            case
                when goal_characteristic = '0'
                then 'Public'
                when goal_characteristic = '1'
                then 'Private'
                else null
            end as perf_goals_characteristic
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
            {{ dbt_utils.surrogate_key("DPGT_PERF_GOALS_TYPE_LABEL_DPGT") }}
            as sk_goal_type_id
        from perf_goals_type p

    )
select
    hash(dpgt_perf_goals_type_label_dpgt) as dpgt_perf_goals_type_sk_dpgt,
    sk_goal_type_id as dpgt_perf_goals_type_key_dpgt,
    dpgt_perf_goals_type_label_dpgt,
    perf_goals_characteristic
from surrogate_key
