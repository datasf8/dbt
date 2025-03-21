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
    category as (
        select distinct category
        from
            (
                select *
                from {{ ref("stg_user_dev_goal_details_flatten") }}
                where dbt_valid_to is null
            )
    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("CATEGORY") }} as sk_category
        from category p
    )

select
    hash(CATEGORY) as ddgc_dev_goal_category_sk_ddgc,
    sk_category as ddgc_dev_goal_category_key_ddgc,
    category as ddgc_dev_goal_category_label_ddgc
from surrogate_key
