{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}


with
    dim_range as (
        select distinct range_code, range_type, range, range_inf, range_sup
        from {{ source("landing_tables_CMN", "RANGE") }}
    )
select range_code, range_type, range, range_inf, range_sup
from dim_range
