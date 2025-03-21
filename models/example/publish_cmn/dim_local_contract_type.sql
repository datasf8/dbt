{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    * replace(
        local_contract_type_id::int as local_contract_type_id
    ) rename(local_contract_type_id as local_contract_type_sk)
from {{ ref("local_contract_type") }}
union all
select -1, null, null, null, null, null, null
