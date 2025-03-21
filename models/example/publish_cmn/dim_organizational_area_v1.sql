{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select * rename(organizational_area_id as organizational_area_sk)
from {{ ref("organizational_area") }}
union all
select -1, null, null, null, null, null, null
