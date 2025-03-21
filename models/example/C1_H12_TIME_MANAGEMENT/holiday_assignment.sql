{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    HolidayCalendar_externalCode as holiday_calendar_code,
    holiday as holiday_name,
    date as date_of_holiday,
    holidayCategory as holiday_category_code,
    mdfSystemRecordStatus as holiday_assignment_status
from {{ ref("stg_holidayassignment_flatten") }}
where dbt_valid_to is null
