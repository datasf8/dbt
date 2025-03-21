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
    goal as (
        select distinct goal_status
        from {{ ref("stg_user_dev_goal_details_flatten") }}
        where dbt_valid_to is null
    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("GOAL_STATUS") }} as sk_goal_status
        from goal p
    )

select
    hash(goal_status) as ddgs_dev_goals_status_sk_ddgs,
    sk_goal_status as ddgs_dev_goals_status_key_ddgs,
    goal_status as ddgs_dev_goals_status_label_ddgs
from surrogate_key
