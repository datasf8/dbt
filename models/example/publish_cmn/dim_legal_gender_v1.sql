{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select * rename(legal_gender_id as legal_gender_sk)
from {{ ref("legal_gender") }}
union all
select -1, null, null, null, null, null, null
