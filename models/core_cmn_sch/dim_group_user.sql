{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

with
    group_user as (
        select distinct grp_id, employee_id, groupname
        from {{ ref("stg_group_user_flatten") }}
    )
select grp_id, employee_id, groupname
from group_user
