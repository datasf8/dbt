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
    dev_goals as (
        select distinct user_id, goal_id, goal_name, goal_owner, goal_modifier
        from
            (
                select *
                from {{ ref("stg_user_dev_goal_details_flatten") }}
                where dbt_valid_to is null
            )
    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("GOAL_ID") }} as sk_goal_id
        from dev_goals p
    )

select
    hash(goal_id) as ddpg_dev_goal_sk_ddpg,
    sk_goal_id as ddpg_dev_goal_key_ddpg,
    user_id as ddpg_user_id_ddpg,
    goal_id as ddpg_dev_goal_id_ddpg,
    goal_name as ddpg_dev_goal_name_ddpg,
    goal_owner as ddpg_goal_owner_ddpg,
    goal_modifier as ddpg_goal_modifier_ddpg
from surrogate_key
