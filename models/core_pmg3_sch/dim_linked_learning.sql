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
    linked as (
        select distinct learning_activity_id from {{ ref("stg_dev_goal_report") }}
    ),
    surrogate_key as (
        select
            p.*,{{ dbt_utils.surrogate_key("LEARNING_ACTIVITY_ID") }} as sk_learning_id
        from linked p
    )

select
    hash(learning_activity_id) as ddll_linked_learning_sk_ddll,
    sk_learning_id as ddll_linked_learning_key_ddll,
    learning_activity_id as ddll_linked_id_ddll
from surrogate_key
where learning_activity_id <> ''
