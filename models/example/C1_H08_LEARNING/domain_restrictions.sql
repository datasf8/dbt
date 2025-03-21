{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select dmn_restriction_id as domain_restriction_code, dmn_id as security_domain_code
from {{ ref("stg_pa_domain_restriction_domain") }}
where dbt_valid_to is null
