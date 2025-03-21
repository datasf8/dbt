{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select * rename(brand_id as brand_sk)
from {{ ref("brand") }}
union all
select -1, null, null, null, null, null, null
