{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}

select
    hash(externalcode, effectivestartdate) as brand_id,
    externalcode as brand_code,
    effectivestartdate as brand_start_date,
    iff(
        mdfsystemeffectiveenddate in ('9999-12-31'),
        lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by effectivestartdate
        ),
        mdfsystemeffectiveenddate
    ) as brand_end_date,
    externalname_defaultvalue brand_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as brand_name_fr,
    mdfsystemstatus as brand_status
from {{ ref("stg_cust_brand_flatten") }}
where dbt_valid_to is null
