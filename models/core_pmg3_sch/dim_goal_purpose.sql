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
    purp as (
        select distinct purpose, purpose_label
        from
            (
                select *
                from {{ ref("stg_user_dev_goal_details_flatten") }}
                where dbt_valid_to is null
            )
    ),
    surrogate_key as (
        select p.*,{{ dbt_utils.surrogate_key("PURPOSE") }} as sk_purpose from purp p
    )

select
    hash(purpose) as ddpp_purpose_sk_ddpp,
    sk_purpose as ddpp_purpose_key_ddpp,
    purpose as ddpp_purpose_ddpp,
    purpose_label as ddpp_purpose_label_ddpp
from surrogate_key
