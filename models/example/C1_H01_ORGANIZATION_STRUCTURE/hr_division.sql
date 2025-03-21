{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(externalcode, startdate) hr_division_id,
    externalcode as hr_division_code,
    startdate as hr_division_start_date,
    iff(
        enddate in ('9999-12-31'),
        lead(startdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by startdate
        ),
        enddate
    ) as hr_division_end_date,
    cust_short_code_defaultvalue as hr_division_short_code,
    name as hr_division_name_en,
    nvl(name_fr_fr, 'Not Translated FR') as hr_division_name_fr,
    status as hr_division_status
from {{ ref("stg_fo_business_unit_flatten") }}
where dbt_valid_to is null
