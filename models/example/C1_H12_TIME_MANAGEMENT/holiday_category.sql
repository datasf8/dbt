{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode) as holiday_category_id,
    externalCode as holiday_category_code,
    externalName_defaultValue as holiday_category_name_en,
    externalName_fr_FR as holiday_category_name_fr,
    mdfSystemRecordStatus as holiday_category_status
from {{ ref("stg_holidaycategory_flatten") }}
where dbt_valid_to is null
