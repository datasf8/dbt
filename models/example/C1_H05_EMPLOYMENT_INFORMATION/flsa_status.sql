{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    picklistdata as (
        select
            v.optionid,
            v.externalcode,
            i.effectivestartdate,
            iff(
                i.effectiveenddate in ('9999-12-31'),
                lead(i.effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by v.externalcode order by i.effectivestartdate
                ),
                i.effectiveenddate
            ) as effectiveenddate,
            v.label_defaultvalue,
            nvl(v.label_fr_fr, 'Not Translated FR') as label_fr_fr,
            v.status
        from {{ ref("stg_picklist_v2_flatten") }} i
        inner join
            {{ ref("stg_picklist_value_v2_flatten") }} v
            on v.picklistv2_id = i.id
            and v.picklistv2_effectivestartdate = i.effectivestartdate
            and v.dbt_valid_to is null
            and i.dbt_valid_to is null
            and i.id = 'FLSASTATUS_USA'
    )
select
    optionid as flsa_status_id,
    externalcode as flsa_status_code,
    effectivestartdate as flsa_status_start_date,
    effectiveenddate as flsa_status_end_date,
    label_defaultvalue as flsa_status_name_en,
    label_fr_fr as flsa_status_name_fr,
    status as flsa_status_status
from picklistdata
