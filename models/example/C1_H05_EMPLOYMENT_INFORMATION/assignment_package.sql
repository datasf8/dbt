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
            and i.id = 'OA_Package'
    )
select
    optionid as assignment_package_id,
    externalcode as assignment_package_code,
    effectivestartdate as assignment_package_start_date,
    effectiveenddate as assignment_package_end_date,
    label_defaultvalue as assignment_package_name_en,
    label_fr_fr as assignment_package_name_fr,
    status as assignment_package_status
from picklistdata
