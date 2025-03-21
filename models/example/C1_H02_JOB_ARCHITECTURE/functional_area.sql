{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, effectivestartdate) as functional_area_id,
    externalcode as functional_area_code,
    effectivestartdate as functional_area_start_date,
    iff(
        mdfsystemeffectiveenddate in ('9999-12-31'),
        lead(effectivestartdate - 1, 1, {d '9999-12-31'}) over (
            partition by externalcode order by effectivestartdate
        ),
        mdfsystemeffectiveenddate
    ) as functional_area_end_date,
    externalname_defaultvalue as functional_area_name_en,
    nvl(externalname_fr_fr, 'Not Translated FR') as functional_area_name_fr,
    mdfsystemstatus as functional_area_status
from {{ ref("stg_cust_operational_area_flatten") }}
where dbt_valid_to is null
