{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(code, effectivestartdate) as currency_id,
    code as currency_code,

    iff(
        effectiveenddate in ('9999-12-31'),
        lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
            partition by code order by effectivestartdate
        ),
        effectiveenddate
    ) as currency_end_date,
    effectivestartdate as currency_start_date,
    externalname_defaultvalue as currency_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as currency_name_fr,
    status as currency_status
from {{ ref("stg_currency_flatten") }}
where dbt_valid_to is null
