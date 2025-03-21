{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(dmn_id, dbt_valid_from) as security_domain_id,
    dmn_id as security_domain_code,
    dbt_valid_from as security_domain_start_date,
    nvl(dbt_valid_to,'9999-12-31') as security_domain_end_date,
    dmn_desc as security_domain_description
from {{ ref("stg_pa_domain") }}
where dbt_valid_to is null
