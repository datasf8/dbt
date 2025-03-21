{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    catalog_id as library_code,
    dmn_id as domain_id,
    catalog_desc as library_description,
    active as is_active_flag,
    discount_applied as is_discount_applied_flag
from {{ ref("stg_pa_catalog") }}
where dbt_valid_to is null
