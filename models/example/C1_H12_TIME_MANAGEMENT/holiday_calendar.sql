{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, country) as holiday_calendar_id,
    externalcode as holiday_calendar_code,
    name_defaultvalue as holiday_calendar_name_en,
    name_fr_fr as holiday_calendar_name_fr,
    country as country_code,
    mdfsystemstatus as holiday_calendar_status
from {{ ref("stg_holidaycalendar_flatten") }}
where dbt_valid_to is null
