{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select * rename(functional_area_id as functional_area_sk)
from {{ ref("functional_area") }}
union all
select -1, null, null, null, null, null, null
