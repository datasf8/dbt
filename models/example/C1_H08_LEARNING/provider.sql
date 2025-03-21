{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(cpnt_src_id, dbt_valid_from) as provider_id,
    cpnt_src_id as provider_code,
    dbt_valid_from as provider_start_date,
    nvl(dbt_valid_to,'9999-12-31') as provider_end_date,
    cpnt_src_desc as provider_description
from {{ ref("stg_pa_cpnt_src") }}
where dbt_valid_to is null
