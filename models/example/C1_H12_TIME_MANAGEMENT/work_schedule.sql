{{
    config(
        materialized="incremental",
        unique_key="1",
        transient=false,
        incremental_strategy="delete+insert",
    )
}}
select
    hash(externalcode, startingdate) as work_schedule_id,
    externalcode as work_schedule_code,
    startingdate as work_schedule_start_date,
    externalname_defaultvalue as work_schedule_name_en,
    externalname_fr_fr as work_schedule_name_fr,
    modelcategory as period_category_code,
    averagehoursperday as average_hours_per_day,
    averagehoursperweek as average_hours_per_week,
    country as country_code,
    mdfsystemrecordstatus as work_schedule_status
from {{ ref("stg_workschedule_flatten") }}
where dbt_valid_to is null
