{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, country) as time_type_id,
    externalcode as time_type_code,
    externalname_defaultvalue as time_type_name,
    country as country_code,
    unit as unit,
    calculationmethod as calculation_method,
    allowedfractionsunitday as allowed_franctions_unit_day,
    '' as custom_group_type,
    '' as custom_global_type,
    loastarteventreason as loa_start_event_reason
from {{ ref("stg_time_type_flatten") }}
where dbt_valid_to is null
