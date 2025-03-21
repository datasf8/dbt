{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
with
    ethnicity as (
        select
            v.optionId,
            v.externalcode,
            i.effectivestartdate,
            iff(
                i.effectiveenddate in ('9999-12-31'),
                lead(i.effectivestartdate - 1, 1, {d '9999-12-31'}) over (
                    partition by v.externalcode order by i.effectivestartdate
                ),
                i.effectiveenddate
            ) as effectiveenddate,
            'USA' as country_code,
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
            and i.id = 'OA_USA_Ethnicity'
    ),SURROGATE_KEY as
(
  select p.*,{{ dbt_utils.generate_surrogate_key(
      ["OPTIONID", "COUNTRY_CODE"]
  ) }} as SK_ETHNICITY_ID from ethnicity p
)
select
    SK_ETHNICITY_ID,
    optionId as ethnicity_id,
    externalcode as ethnicity_code,
    effectivestartdate as ethnicity_start_date,
    effectiveenddate as ethnicity_end_date,
    country_code as country_code,
    label_defaultvalue as ethnicity_name_en,
    label_fr_fr as ethnicity_name_fr,
    status as ethnicity_status
from SURROGATE_KEY