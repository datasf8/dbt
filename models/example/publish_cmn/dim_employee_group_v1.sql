{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select * rename(employee_group_id as employee_group_sk)
from {{ ref("employee_group") }}
union all
select -1, null, null, null, null, null, null
