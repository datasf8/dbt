{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select range_id as range_sk, * exclude (range_id)
from {{ ref("ranges_v1") }}
union all
select -1, null, null, null, null, null, null
